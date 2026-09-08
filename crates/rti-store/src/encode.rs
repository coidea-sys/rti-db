//! Columnar compression encoding:
//! - timestamp column: delta-of-delta + zigzag varint (simplified Gorilla);
//! - value column: XOR float compression (simplified Gorilla: 0 bits / 1+6bit length+significant bits).
//!
//! Encoding writes into caller-provided buffers; decoding is a streaming iterator — point by point, zero allocation.

use rti_core::{Error, Result, Sample};

// ---------------------------------------------------------------- varint

/// LEB128 unsigned varint encode, appended to `out`.
pub fn write_uvarint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        }
        out.push(b | 0x80);
    }
}

/// Read one varint from `buf[*pos..]`; returns `None` on failure (truncation/overflow).
pub fn read_uvarint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        let b = *buf.get(*pos)?;
        *pos += 1;
        if shift >= 64 {
            return None;
        }
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
    }
}

/// zigzag encode: signed → unsigned (small absolute values → small integers).
pub fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// zigzag decode.
pub fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

// ---------------------------------------------------------------- bit io

/// Bit writer (LSB-first byte stream).
pub struct BitWriter {
    out: Vec<u8>,
    acc: u128,
    nbits: u32,
}

impl BitWriter {
    /// Create a new empty writer.
    pub fn new() -> Self {
        Self { out: Vec::new(), acc: 0, nbits: 0 }
    }

    /// Write the low `n` bits of `v` (`n <= 64`).
    pub fn write_bits(&mut self, v: u64, n: u32) {
        debug_assert!(n <= 64);
        if n == 0 {
            return;
        }
        let mask = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
        self.acc |= ((v & mask) as u128) << self.nbits;
        self.nbits += n;
        while self.nbits >= 8 {
            self.out.push(self.acc as u8);
            self.acc >>= 8;
            self.nbits -= 8;
        }
    }

    /// Flush (last byte zero-padded) and return the byte stream.
    pub fn finish(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            self.out.push(self.acc as u8);
        }
        self.out
    }
}

/// Bit reader, the counterpart of [`BitWriter`].
pub struct BitReader<'a> {
    buf: &'a [u8],
    pos: usize,
    acc: u128,
    nbits: u32,
}

impl<'a> BitReader<'a> {
    /// Create a reader from a byte slice.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0, acc: 0, nbits: 0 }
    }

    /// Read `n` bits; returns `None` when the stream is exhausted.
    pub fn read_bits(&mut self, n: u32) -> Option<u64> {
        debug_assert!(n <= 64);
        while self.nbits < n {
            let b = *self.buf.get(self.pos)?;
            self.pos += 1;
            self.acc |= (b as u128) << self.nbits;
            self.nbits += 8;
        }
        let mask = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
        let v = (self.acc as u64) & mask;
        self.acc >>= n;
        self.nbits -= n;
        Some(v)
    }
}

// ------------------------------------------------- timestamps (delta-of-delta)

/// Encode a timestamp column (`samples` must be sorted by ts).
pub fn encode_ts(samples: &[Sample], out: &mut Vec<u8>) {
    if samples.is_empty() {
        return;
    }
    write_uvarint(zigzag_encode(samples[0].ts), out);
    if samples.len() == 1 {
        return;
    }
    let mut prev_delta = samples[1].ts - samples[0].ts;
    write_uvarint(zigzag_encode(prev_delta), out);
    let mut prev_ts = samples[1].ts;
    for s in &samples[2..] {
        let delta = s.ts - prev_ts;
        let dod = delta - prev_delta;
        write_uvarint(zigzag_encode(dod), out);
        prev_delta = delta;
        prev_ts = s.ts;
    }
}

/// Streaming timestamp-column decoder.
pub struct TsDecoder<'a> {
    buf: &'a [u8],
    pos: usize,
    emitted: usize,
    prev_ts: i64,
    prev_delta: i64,
}

impl<'a> TsDecoder<'a> {
    /// Create a decoder and read out the first timestamp; an empty stream returns `Ok(None)`.
    pub fn new(buf: &'a [u8]) -> Result<Option<Self>> {
        let mut pos = 0;
        match read_uvarint(buf, &mut pos) {
            Some(v) => Ok(Some(Self {
                buf,
                pos,
                emitted: 0,
                prev_ts: zigzag_decode(v),
                prev_delta: 0,
            })),
            None if buf.is_empty() => Ok(None),
            None => Err(Error::Corrupt("ts stream truncated at header".into())),
        }
    }

    /// Next timestamp (the `n`-th call returns the `n`-th point).
    pub fn next_ts(&mut self) -> Option<i64> {
        let ts = match self.emitted {
            0 => self.prev_ts,
            1 => {
                let d = zigzag_decode(read_uvarint(self.buf, &mut self.pos)?);
                self.prev_delta = d;
                self.prev_ts = self.prev_ts.wrapping_add(d);
                self.prev_ts
            }
            _ => {
                let dod = zigzag_decode(read_uvarint(self.buf, &mut self.pos)?);
                self.prev_delta = self.prev_delta.wrapping_add(dod);
                self.prev_ts = self.prev_ts.wrapping_add(self.prev_delta);
                self.prev_ts
            }
        };
        self.emitted += 1;
        Some(ts)
    }
}

// -------------------------------------------- block decode (v0.2, 8-way unrolled)

/// delta-of-delta single-step decode (inner step of block decode).
#[inline(always)]
fn dod_step(buf: &[u8], pos: &mut usize, prev_ts: &mut i64, prev_delta: &mut i64) -> Option<i64> {
    let dod = zigzag_decode(read_uvarint(buf, pos)?);
    *prev_delta = prev_delta.wrapping_add(dod);
    *prev_ts = prev_ts.wrapping_add(*prev_delta);
    Some(*prev_ts)
}

/// Block-decode a timestamp column: one call decodes up to N timestamps from `buf` (column start) into `out`.
///
/// Returns the number actually decoded (`<= N`; less than N when the stream runs out early). Semantics are
/// **exactly identical** to point-by-point [`TsDecoder`] decoding (including wrapping overflow behavior),
/// so scan/bench hot paths can batch-decode and then apply predicate filtering themselves.
///
/// Implementation note (honest): delta-of-delta has a serial data dependency and cannot be truly SIMD;
/// this uses safe-Rust 8-way unrolling (fixed-count inner loop + `#[inline(always)]` single step),
/// amortizing the branch/bounds-check overhead of per-point calls and helping the compiler's instruction scheduling.
/// Does not use nightly `std::simd`.
pub fn decode_ts_block<const N: usize>(buf: &[u8], out: &mut [i64; N]) -> Result<usize> {
    if N == 0 {
        return Ok(0);
    }
    let mut pos = 0usize;
    let mut prev_ts = match read_uvarint(buf, &mut pos) {
        Some(v) => zigzag_decode(v),
        None if buf.is_empty() => return Ok(0),
        None => return Err(Error::Corrupt("ts stream truncated at header".into())),
    };
    out[0] = prev_ts;
    if N == 1 {
        return Ok(1);
    }
    let mut prev_delta = match read_uvarint(buf, &mut pos) {
        Some(v) => zigzag_decode(v),
        None => return Ok(1),
    };
    prev_ts = prev_ts.wrapping_add(prev_delta);
    out[1] = prev_ts;
    let mut n = 2usize;
    // 8-way unrolled main loop: one inlined single step per lane.
    while n + 8 <= N {
        macro_rules! lane {
            ($l:expr) => {
                match dod_step(buf, &mut pos, &mut prev_ts, &mut prev_delta) {
                    Some(ts) => out[n + $l] = ts,
                    None => return Ok(n + $l),
                }
            };
        }
        lane!(0); lane!(1); lane!(2); lane!(3);
        lane!(4); lane!(5); lane!(6); lane!(7);
        n += 8;
    }
    // tail (when N is not a multiple of 8).
    while n < N {
        match dod_step(buf, &mut pos, &mut prev_ts, &mut prev_delta) {
            Some(ts) => {
                out[n] = ts;
                n += 1;
            }
            None => return Ok(n),
        }
    }
    Ok(n)
}

// ----------------------------------------------------------- values (xor)

/// Encode a value column (simplified Gorilla XOR).
pub fn encode_vals(samples: &[Sample], out: &mut Vec<u8>) {
    if samples.is_empty() {
        return;
    }
    let mut bw = BitWriter::new();
    let mut prev = samples[0].value.to_bits();
    bw.write_bits(prev, 64);
    for s in &samples[1..] {
        let bits = s.value.to_bits();
        let xor = bits ^ prev;
        if xor == 0 {
            bw.write_bits(0, 1);
        } else {
            let sig = 64 - xor.leading_zeros(); // significant-bits length 1..=64
            bw.write_bits(1, 1);
            bw.write_bits((sig - 1) as u64, 6);
            bw.write_bits(xor, sig);
        }
        prev = bits;
    }
    *out = bw.finish();
}

/// Streaming value-column decoder, yielding f64 point by point.
pub struct ValDecoder<'a> {
    br: BitReader<'a>,
    prev: u64,
    first: bool,
}

impl<'a> ValDecoder<'a> {
    /// Create a decoder; an empty stream returns `Ok(None)`.
    pub fn new(buf: &'a [u8]) -> Result<Option<Self>> {
        if buf.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self { br: BitReader::new(buf), prev: 0, first: true }))
    }

    /// Next value.
    pub fn next_val(&mut self) -> Option<f64> {
        if self.first {
            self.first = false;
            self.prev = self.br.read_bits(64)?;
            return Some(f64::from_bits(self.prev));
        }
        let control = self.br.read_bits(1)?;
        if control == 0 {
            return Some(f64::from_bits(self.prev));
        }
        let sig = self.br.read_bits(6)? as u32 + 1;
        let payload = self.br.read_bits(sig)?;
        self.prev ^= payload;
        Some(f64::from_bits(self.prev))
    }
}

// -------------------------------------------------- value block decode (v0.2)

/// XOR single-step decode (inner step of block decode).
#[inline(always)]
fn xor_step(br: &mut BitReader<'_>, prev: &mut u64) -> Option<f64> {
    let control = br.read_bits(1)?;
    if control == 0 {
        return Some(f64::from_bits(*prev));
    }
    let sig = br.read_bits(6)? as u32 + 1;
    let payload = br.read_bits(sig)?;
    *prev ^= payload;
    Some(f64::from_bits(*prev))
}

/// Block-decode a value column: one call decodes up to N f64 values from `buf` (column start) into `out`.
///
/// Returns the number actually decoded (`<= N`). Semantics are **bit-exact** with point-by-point
/// [`ValDecoder`] decoding. Same implementation constraints as [`decode_ts_block`]:
/// safe Rust, 8-way unrolled, no nightly `std::simd`.
pub fn decode_val_block<const N: usize>(buf: &[u8], out: &mut [f64; N]) -> Result<usize> {
    if N == 0 {
        return Ok(0);
    }
    if buf.is_empty() {
        return Ok(0);
    }
    let mut br = BitReader::new(buf);
    let mut prev = br
        .read_bits(64)
        .ok_or_else(|| Error::Corrupt("val stream truncated at header".into()))?;
    out[0] = f64::from_bits(prev);
    let mut n = 1usize;
    while n + 8 <= N {
        macro_rules! lane {
            ($l:expr) => {
                match xor_step(&mut br, &mut prev) {
                    Some(v) => out[n + $l] = v,
                    None => return Ok(n + $l),
                }
            };
        }
        lane!(0); lane!(1); lane!(2); lane!(3);
        lane!(4); lane!(5); lane!(6); lane!(7);
        n += 8;
    }
    while n < N {
        match xor_step(&mut br, &mut prev) {
            Some(v) => {
                out[n] = v;
                n += 1;
            }
            None => return Ok(n),
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(n: usize) -> Vec<Sample> {
        (0..n)
            .map(|i| Sample::new(1_700_000_000_000_000_000 + i as i64 * 1_000_000_000, 20.0 + i as f64 * 0.13))
            .collect()
    }

    #[test]
    fn varint_zigzag_roundtrip() {
        let mut out = Vec::new();
        let vals = [0i64, -1, 1, i64::MIN / 2, i64::MAX / 2, -123_456_789];
        for &v in &vals {
            write_uvarint(zigzag_encode(v), &mut out);
        }
        let mut pos = 0;
        for &v in &vals {
            assert_eq!(zigzag_decode(read_uvarint(&out, &mut pos).unwrap()), v);
        }
        assert_eq!(pos, out.len());
    }

    #[test]
    fn bit_io_roundtrip() {
        let mut bw = BitWriter::new();
        bw.write_bits(0b101, 3);
        bw.write_bits(u64::MAX, 64);
        bw.write_bits(0, 1);
        bw.write_bits(7, 3);
        let buf = bw.finish();
        let mut br = BitReader::new(&buf);
        assert_eq!(br.read_bits(3), Some(0b101));
        assert_eq!(br.read_bits(64), Some(u64::MAX));
        assert_eq!(br.read_bits(1), Some(0));
        assert_eq!(br.read_bits(3), Some(7));
    }

    #[test]
    fn ts_dod_roundtrip() {
        for n in [0usize, 1, 2, 3, 1000] {
            let s = samples(n);
            let mut buf = Vec::new();
            encode_ts(&s, &mut buf);
            let dec = TsDecoder::new(&buf).unwrap();
            if n == 0 {
                assert!(dec.is_none());
                continue;
            }
            let mut dec = dec.unwrap();
            for want in &s {
                assert_eq!(dec.next_ts(), Some(want.ts));
            }
        }
    }

    #[test]
    fn ts_dod_handles_jitter() {
        let s = vec![
            Sample::new(100, 0.0),
            Sample::new(110, 0.0),
            Sample::new(115, 0.0),  // dod = -5
            Sample::new(150, 0.0),  // dod = +30
            Sample::new(150, 0.0),  // duplicate timestamps
        ];
        let mut buf = Vec::new();
        encode_ts(&s, &mut buf);
        let mut dec = TsDecoder::new(&buf).unwrap().unwrap();
        for want in &s {
            assert_eq!(dec.next_ts(), Some(want.ts));
        }
    }

    #[test]
    fn val_xor_roundtrip() {
        for n in [0usize, 1, 2, 500] {
            let s = samples(n);
            let mut buf = Vec::new();
            encode_vals(&s, &mut buf);
            let dec = ValDecoder::new(&buf).unwrap();
            if n == 0 {
                assert!(dec.is_none());
                continue;
            }
            let mut dec = dec.unwrap();
            for want in &s {
                assert_eq!(dec.next_val().map(|v| v.to_bits()), Some(want.value.to_bits()));
            }
        }
    }

    /// Block decode and scalar streaming decode must agree point by point (including N not a multiple of 8, and jittered streams).
    #[test]
    fn ts_block_decode_matches_scalar() {
        let mut s = samples(1000);
        // add jitter and duplicate timestamps to cover negative dod
        s[100].ts += 7;
        s[200].ts = s[199].ts;
        let mut buf = Vec::new();
        encode_ts(&s, &mut buf);

        let mut scalar = Vec::with_capacity(s.len());
        let mut dec = TsDecoder::new(&buf).unwrap().unwrap();
        while let Some(ts) = dec.next_ts() {
            scalar.push(ts);
        }

        // decode all 1000 points as one block
        let mut tmp = [0i64; 1000];
        let got = decode_ts_block::<1000>(&buf, &mut tmp).unwrap();
        assert_eq!(got, s.len());
        assert_eq!(&tmp[..got], &scalar[..], "N=1000 whole block must match the scalar decoder");

        // prefixes decoded at various block sizes (including non-multiples of 8) must match the scalar decoder
        macro_rules! check_prefix {
            ($n:expr) => {{
                let mut small = [0i64; $n];
                let g = decode_ts_block::<$n>(&buf, &mut small).unwrap();
                assert_eq!(g, $n.min(s.len()));
                assert_eq!(&small[..g], &scalar[..g], "N={} prefix", $n);
            }};
        }
        check_prefix!(2);
        check_prefix!(8);
        check_prefix!(9);
        check_prefix!(64);
    }

    #[test]
    fn val_block_decode_matches_scalar_bit_exact() {
        let s = samples(777); // not a multiple of 8
        let mut buf = Vec::new();
        encode_vals(&s, &mut buf);

        let mut scalar = Vec::with_capacity(s.len());
        let mut dec = ValDecoder::new(&buf).unwrap().unwrap();
        while let Some(v) = dec.next_val() {
            scalar.push(v.to_bits());
        }

        let mut tmp = [0.0f64; 777];
        let got = decode_val_block::<777>(&buf, &mut tmp).unwrap();
        assert_eq!(got, s.len());
        for (b, w) in tmp.iter().zip(scalar.iter()) {
            assert_eq!(b.to_bits(), *w, "block decode must be bit-exact");
        }
    }

    #[test]
    fn block_decode_edge_cases() {
        // empty stream
        let mut out8 = [0i64; 8];
        assert_eq!(decode_ts_block::<8>(b"", &mut out8).unwrap(), 0);
        let mut vout = [0.0f64; 8];
        assert_eq!(decode_val_block::<8>(b"", &mut vout).unwrap(), 0);
        // N=0
        assert_eq!(decode_ts_block::<0>(b"", &mut []).unwrap(), 0);
        assert_eq!(decode_val_block::<0>(b"", &mut []).unwrap(), 0);
        // truncated head (a nonempty-but-incomplete varint is impossible — a single byte is already valid;
        // use all-0x80 bytes to construct an illegal infinitely-continuing varint)
        let bad = [0x80u8; 3];
        assert!(decode_ts_block::<8>(&bad, &mut out8).is_err());
        // single-point / two-point streams
        let one = samples(1);
        let mut b1 = Vec::new();
        encode_ts(&one, &mut b1);
        assert_eq!(decode_ts_block::<8>(&b1, &mut out8).unwrap(), 1);
        assert_eq!(out8[0], one[0].ts);
        let two = samples(2);
        let mut b2 = Vec::new();
        encode_ts(&two, &mut b2);
        let n = decode_ts_block::<8>(&b2, &mut out8).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&out8[..2], &[two[0].ts, two[1].ts]);
        // N smaller than the stream length: truncates exactly at the first N points
        let many = samples(100);
        let mut bm = Vec::new();
        encode_ts(&many, &mut bm);
        let mut o10 = [0i64; 10];
        assert_eq!(decode_ts_block::<10>(&bm, &mut o10).unwrap(), 10);
        for (i, ts) in o10.iter().enumerate() {
            assert_eq!(*ts, many[i].ts);
        }
    }

    #[test]
    fn compression_actually_compresses() {
        let s = samples(10_000);
        let raw = s.len() * 16;
        let mut ts = Vec::new();
        let mut vals = Vec::new();
        encode_ts(&s, &mut ts);
        encode_vals(&s, &mut vals);
        assert!(
            ts.len() + vals.len() < raw / 2,
            "compressed {} vs raw {}",
            ts.len() + vals.len(),
            raw
        );
    }
}
