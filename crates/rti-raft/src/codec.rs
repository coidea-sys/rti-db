//! 协议消息的二进制编解码（小端，无第三方依赖）。
//!
//! 帧格式：`len u32` + payload（不含 len 自身）。payload 首字节为
//! 消息 tag，随后定长字段按声明序、变长字段以 `count u32` 引导。
//! 解码遇到截断/未知 tag 返回 `None`（Raft 容忍丢包：坏帧即丢）。

use rti_wal::Record;

use crate::node::{Entry, Msg, Snapshot};

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_entry(out: &mut Vec<u8>, e: &Entry) {
    put_u64(out, e.term);
    put_u32(out, e.records.len() as u32);
    for r in &e.records {
        put_u32(out, r.series);
        put_u64(out, r.sample.ts as u64);
        put_u64(out, r.sample.value.to_bits());
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn u32(&mut self) -> Option<u32> {
        let b = self.buf.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_le_bytes(b.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        let b = self.buf.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(u64::from_le_bytes(b.try_into().ok()?))
    }

    fn entry(&mut self) -> Option<Entry> {
        let term = self.u64()?;
        let n = self.u32()? as usize;
        if n > 1 << 20 {
            return None; // 防御性上限，拒绝畸形帧
        }
        let mut records = Vec::with_capacity(n.min(1 << 10));
        for _ in 0..n {
            let series = self.u32()?;
            let ts = self.u64()? as i64;
            let bits = self.u64()?;
            records.push(Record::new(series, ts, f64::from_bits(bits)));
        }
        Some(Entry { term, records })
    }
}

/// 把 `msg` 编码为一帧（`len u32` + payload）追加到 `out`。
pub fn encode_msg(msg: &Msg, out: &mut Vec<u8>) {
    let mut payload = Vec::new();
    match msg {
        Msg::RequestVote { term, candidate, last_log_index, last_log_term } => {
            payload.push(0u8);
            put_u64(&mut payload, *term);
            put_u64(&mut payload, *candidate);
            put_u64(&mut payload, *last_log_index);
            put_u64(&mut payload, *last_log_term);
        }
        Msg::VoteResponse { term, granted } => {
            payload.push(1u8);
            put_u64(&mut payload, *term);
            payload.push(*granted as u8);
        }
        Msg::AppendEntries { term, leader, prev_log_index, prev_log_term, entries, leader_commit } => {
            payload.push(2u8);
            put_u64(&mut payload, *term);
            put_u64(&mut payload, *leader);
            put_u64(&mut payload, *prev_log_index);
            put_u64(&mut payload, *prev_log_term);
            put_u32(&mut payload, entries.len() as u32);
            for e in entries {
                put_entry(&mut payload, e);
            }
            put_u64(&mut payload, *leader_commit);
        }
        Msg::AppendResponse { term, success, match_index } => {
            payload.push(3u8);
            put_u64(&mut payload, *term);
            payload.push(*success as u8);
            put_u64(&mut payload, *match_index);
        }
        Msg::PreVote { term, candidate, last_log_index, last_log_term } => {
            payload.push(4u8);
            put_u64(&mut payload, *term);
            put_u64(&mut payload, *candidate);
            put_u64(&mut payload, *last_log_index);
            put_u64(&mut payload, *last_log_term);
        }
        Msg::PreVoteResponse { term, granted } => {
            payload.push(5u8);
            put_u64(&mut payload, *term);
            payload.push(*granted as u8);
        }
        Msg::InstallSnapshot { term, leader, snapshot } => {
            payload.push(6u8);
            put_u64(&mut payload, *term);
            put_u64(&mut payload, *leader);
            put_u64(&mut payload, snapshot.last_included_index);
            put_u64(&mut payload, snapshot.last_included_term);
            put_u32(&mut payload, snapshot.state.len() as u32);
            payload.extend_from_slice(&snapshot.state);
        }
    }
    put_u32(out, payload.len() as u32);
    out.extend_from_slice(&payload);
}

/// 从帧体（payload，不含 len 前缀）解码一条消息；坏帧返回 `None`。
pub fn decode_payload(buf: &[u8]) -> Option<Msg> {
    let mut r = Reader { buf, pos: 0 };
    let tag = *buf.first()?;
    r.pos = 1;
    let msg = match tag {
        0 => Msg::RequestVote {
            term: r.u64()?,
            candidate: r.u64()?,
            last_log_index: r.u64()?,
            last_log_term: r.u64()?,
        },
        1 => Msg::VoteResponse { term: r.u64()?, granted: r.u8()? != 0 },
        2 => {
            let term = r.u64()?;
            let leader = r.u64()?;
            let prev_log_index = r.u64()?;
            let prev_log_term = r.u64()?;
            let n = r.u32()? as usize;
            if n > 1 << 20 {
                return None;
            }
            let mut entries = Vec::with_capacity(n.min(64));
            for _ in 0..n {
                entries.push(r.entry()?);
            }
            let leader_commit = r.u64()?;
            Msg::AppendEntries { term, leader, prev_log_index, prev_log_term, entries, leader_commit }
        }
        3 => Msg::AppendResponse { term: r.u64()?, success: r.u8()? != 0, match_index: r.u64()? },
        4 => Msg::PreVote {
            term: r.u64()?,
            candidate: r.u64()?,
            last_log_index: r.u64()?,
            last_log_term: r.u64()?,
        },
        5 => Msg::PreVoteResponse { term: r.u64()?, granted: r.u8()? != 0 },
        6 => {
            let term = r.u64()?;
            let leader = r.u64()?;
            let last_included_index = r.u64()?;
            let last_included_term = r.u64()?;
            let n = r.u32()? as usize;
            if n > 16 << 20 {
                return None; // 与帧长上限一致的防御
            }
            let state = r.buf.get(r.pos..r.pos + n)?.to_vec();
            let snapshot = Snapshot { last_included_index, last_included_term, state };
            Msg::InstallSnapshot { term, leader, snapshot }
        }
        _ => return None,
    };
    Some(msg)
}

/// 尝试从字节流头部切出一帧：成功返回 `(消息, 消费字节数)`，
/// 数据不足返回 `Ok(None)`，坏帧返回 `Err`。
pub fn decode_frame(buf: &[u8]) -> std::result::Result<Option<(Msg, usize)>, ()> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().map_err(|_| ())?) as usize;
    if len > 16 << 20 {
        return Err(()); // 防御性上限
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let msg = decode_payload(&buf[4..4 + len]).ok_or(())?;
    Ok(Some((msg, 4 + len)))
}

/// 单帧便捷解码（含 len 前缀），供测试使用。
pub fn decode_msg(buf: &[u8]) -> Option<Msg> {
    match decode_frame(buf) {
        Ok(Some((m, _))) => Some(m),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_msgs() -> Vec<Msg> {
        vec![
            Msg::RequestVote { term: 7, candidate: 42, last_log_index: 99, last_log_term: 6 },
            Msg::VoteResponse { term: 7, granted: true },
            Msg::VoteResponse { term: 1, granted: false },
            Msg::AppendEntries {
                term: 3,
                leader: 1,
                prev_log_index: 2,
                prev_log_term: 3,
                entries: vec![
                    Entry { term: 3, records: vec![Record::new(1, 100, 1.5), Record::new(2, -200, -0.0)] },
                    Entry { term: 4, records: vec![] },
                ],
                leader_commit: 2,
            },
            Msg::AppendEntries {
                term: 9,
                leader: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            Msg::AppendResponse { term: 3, success: true, match_index: 17 },
            Msg::AppendResponse { term: 3, success: false, match_index: 0 },
            Msg::PreVote { term: 8, candidate: 7, last_log_index: 12, last_log_term: 6 },
            Msg::PreVoteResponse { term: 7, granted: true },
            Msg::PreVoteResponse { term: 2, granted: false },
            Msg::InstallSnapshot {
                term: 5,
                leader: 1,
                snapshot: Snapshot {
                    last_included_index: 40,
                    last_included_term: 4,
                    state: b"state-machine-bytes\x00\xFF".to_vec(),
                },
            },
            Msg::InstallSnapshot {
                term: 1,
                leader: 2,
                snapshot: Snapshot { last_included_index: 1, last_included_term: 1, state: Vec::new() },
            },
        ]
    }

    #[test]
    fn codec_roundtrip_all_variants() {
        for m in sample_msgs() {
            let mut buf = Vec::new();
            encode_msg(&m, &mut buf);
            let got = decode_msg(&buf).unwrap();
            assert_eq!(got, m);
        }
    }

    #[test]
    fn codec_handles_stream_split() {
        let mut buf = Vec::new();
        for m in sample_msgs() {
            encode_msg(&m, &mut buf);
        }
        // 逐字节喂给 decode_frame，模拟 TCP 流切分
        let mut pending: Vec<u8> = Vec::new();
        let mut decoded = Vec::new();
        for b in buf {
            pending.push(b);
            loop {
                match decode_frame(&pending) {
                    Ok(Some((m, n))) => {
                        decoded.push(m);
                        pending.drain(..n);
                    }
                    Ok(None) => break,
                    Err(()) => panic!("bad frame"),
                }
            }
        }
        assert_eq!(decoded, sample_msgs());
    }

    #[test]
    fn codec_rejects_garbage() {
        assert!(decode_msg(b"").is_none());
        assert!(decode_msg(&[9, 0, 0, 0, 0xFE]).is_none()); // 未知 tag
        assert!(decode_msg(&[100, 0, 0, 0, 0x00]).is_none()); // 长度超实际
    }
}
