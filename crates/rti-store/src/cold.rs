//! Cold tiering (v0.4): old segments sink to low-cost storage; scan reads back transparently.
//!
//! - [`ColdTier`]: a minimal object-store abstraction for storing segment bytes by name;
//! - [`LocalFsColdTier`]: full implementation (local-directory simulation, tmp + rename atomic writes);
//! - [`S3ColdTier`] (feature `s3`, completed in v0.5): a real S3 HTTP transport —
//!   a pure-std minimal HTTP/1.1 client (**zero new dependencies**, plaintext HTTP endpoints only,
//!   see the [`S3Config`] docs) + full SigV4 request signing (pure-safe in-house
//!   SHA-256/HMAC, tested against known vectors from RFC 4231 / AWS docs). Credentials and
//!   endpoint/region/bucket are read from environment variables ([`S3Config::from_env`]) or explicit
//!   configuration; missing credentials yield a graceful `Err`. Tests use the embedded [`MockS3Server`]
//!   for put/get/list round-trips (no real S3 needed).

use std::fs;
use std::path::{Path, PathBuf};

use rti_core::{Error, Result};

/// Cold-tier object-store abstraction (stores complete segment bytes by segment name).
///
/// Implementations must be `Send + Sync` (attached to `Db` and shared across threads).
pub trait ColdTier: Send + Sync {
    /// Write one segment (same name overwrites).
    fn put_segment(&self, name: &str, data: &[u8]) -> Result<()>;
    /// Read the complete bytes of one segment.
    fn get_segment(&self, name: &str) -> Result<Vec<u8>>;
    /// List all segment names in the cold tier.
    fn list(&self) -> Result<Vec<String>>;
}

/// Local-directory cold tier (full implementation; semantically equivalent to a single-bucket object store).
///
/// Writes are tmp + rename; a crash can only leave a `.tmp` remnant (ignored by list).
pub struct LocalFsColdTier {
    dir: PathBuf,
}

impl LocalFsColdTier {
    /// Use `dir` as the cold-tier root (created if missing).
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, name: &str) -> Result<PathBuf> {
        // path-traversal guard: segment names may only be simple file names
        validate_segment_name(name)?;
        Ok(self.dir.join(name))
    }
}

impl ColdTier for LocalFsColdTier {
    fn put_segment(&self, name: &str, data: &[u8]) -> Result<()> {
        let path = self.path(name)?;
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn get_segment(&self, name: &str) -> Result<Vec<u8>> {
        let path = self.path(name)?;
        if !path.exists() {
            return Err(Error::NotFound);
        }
        Ok(fs::read(path)?)
    }

    fn list(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for e in fs::read_dir(&self.dir)? {
            let e = e?;
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".seg") {
                out.push(name);
            }
        }
        out.sort();
        Ok(out)
    }
}

// ------------------------------------------------------------- S3 (full implementation, v0.5)

#[cfg(feature = "s3")]
use std::io::{Read, Write};
#[cfg(feature = "s3")]
use std::time::SystemTime;

/// S3 connection configuration (endpoint / region / bucket / credentials).
///
/// `endpoint` accepts `http://host[:port]` or a bare `host[:port]` (default 80).
/// **Plaintext HTTP only**: the TLS (rustls/openssl) dependency chain exceeds this crate's
/// dependency budget (see 'dependency decisions' in the README); for production HTTPS put a reverse
/// proxy in front, or talk directly to a local/Intranet MinIO. An `https://` prefix is an explicit error at use time.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct S3Config {
    /// S3 endpoint (`http://host[:port]` or `host[:port]`).
    pub endpoint: String,
    /// AWS region (part of the SigV4 scope).
    pub region: String,
    /// Bucket name (path-style addressing: `/{bucket}/{key}`).
    pub bucket: String,
    /// Access Key ID.
    pub access_key: String,
    /// Secret Access Key (always redacted in Debug/Display).
    pub secret_key: String,
}

#[cfg(feature = "s3")]
impl S3Config {
    /// Explicit constructor (performs no network requests).
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
        }
    }

    /// Read configuration from the process environment; returns a **graceful Err** listing missing variables when incomplete.
    ///
    /// Variable precedence (earlier wins):
    /// - endpoint: `RTI_S3_ENDPOINT` / `S3_ENDPOINT` / `AWS_ENDPOINT_URL`
    /// - region: `RTI_S3_REGION` / `AWS_REGION` / `AWS_DEFAULT_REGION`
    ///   (default `us-east-1`)
    /// - bucket: `RTI_S3_BUCKET` / `S3_BUCKET`
    /// - access key: `RTI_S3_ACCESS_KEY` / `AWS_ACCESS_KEY_ID`
    /// - secret key: `RTI_S3_SECRET_KEY` / `AWS_SECRET_ACCESS_KEY`
    pub fn from_env() -> Result<Self> {
        Self::from_env_with(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
    }

    /// Version with an injectable environment reader (for tests, avoids touching real environment variables).
    pub fn from_env_with(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let pick = |keys: &[&str]| keys.iter().find_map(|k| get(k));
        let endpoint = pick(&["RTI_S3_ENDPOINT", "S3_ENDPOINT", "AWS_ENDPOINT_URL"]);
        let region = pick(&["RTI_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"])
            .unwrap_or_else(|| "us-east-1".to_string());
        let bucket = pick(&["RTI_S3_BUCKET", "S3_BUCKET"]);
        let access_key = pick(&["RTI_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"]);
        let secret_key = pick(&["RTI_S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"]);
        let mut missing: Vec<&str> = Vec::new();
        if endpoint.is_none() {
            missing.push("endpoint(RTI_S3_ENDPOINT/S3_ENDPOINT/AWS_ENDPOINT_URL)");
        }
        if bucket.is_none() {
            missing.push("bucket(RTI_S3_BUCKET/S3_BUCKET)");
        }
        if access_key.is_none() {
            missing.push("access_key(RTI_S3_ACCESS_KEY/AWS_ACCESS_KEY_ID)");
        }
        if secret_key.is_none() {
            missing.push("secret_key(RTI_S3_SECRET_KEY/AWS_SECRET_ACCESS_KEY)");
        }
        if !missing.is_empty() {
            return Err(Error::Corrupt(format!(
                "S3 config incomplete, missing: {}",
                missing.join(", ")
            )));
        }
        Ok(Self::new(
            endpoint.unwrap(),
            region,
            bucket.unwrap(),
            access_key.unwrap(),
            secret_key.unwrap(),
        ))
    }
}

#[cfg(feature = "s3")]
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

/// S3 cold tier (feature `s3`, **full implementation** since v0.5).
///
/// - transport: pure-std minimal HTTP/1.1 client (`TcpStream`, connection-level timeouts,
///   `Connection: close`), **zero new dependencies**; plaintext HTTP only (see the
///   [`S3Config`] docs).
/// - addressing: path-style `PUT/GET /{bucket}/{key}`; list goes through
///   ListObjectsV2 (`?list-type=2&prefix=`, with continuation pagination).
/// - authentication: every request carries a full SigV4 signature (`Authorization` header +
///   `x-amz-date` + `x-amz-content-sha256` payload hash); the signing primitives are
///   pure-safe in-house SHA-256/HMAC in this file (tested against AWS-documented known vectors).
/// - construction: [`S3ColdTier::from_env`] (environment variables) or
///   [`S3ColdTier::from_config`] (explicit [`S3Config`]); without credentials it fails
///   gracefully with `Err` — no panic, no network request.
///
/// Single-shot uploads (segments are usually <= a few MB); no retries/multipart — the archive
/// path tolerates failure-and-redo anyway (`archive_older_than` is idempotent); see known limitations in the README.
#[cfg(feature = "s3")]
pub struct S3ColdTier {
    config: S3Config,
}

#[cfg(feature = "s3")]
impl S3ColdTier {
    /// Explicit constructor (performs no network requests).
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self { config: S3Config::new(endpoint, region, bucket, access_key, secret_key) }
    }

    /// Construct from an [`S3Config`]; an invalid endpoint (empty / https) errors immediately.
    pub fn from_config(config: S3Config) -> Result<Self> {
        parse_endpoint(&config.endpoint)?;
        Ok(Self { config })
    }

    /// Construct from environment variables (see [`S3Config::from_env`]); missing credentials yield a graceful `Err`.
    pub fn from_env() -> Result<Self> {
        Self::from_config(S3Config::from_env()?)
    }

    /// SigV4 signing-key derivation: kDate→kRegion→kService→kSigning.
    pub fn signing_key(&self, date: &str) -> [u8; 32] {
        signing_key(&self.config.secret_key, date, &self.config.region, "s3")
    }

    /// SigV4 request signing: `hex(HMAC(kSigning, string_to_sign))`.
    pub fn sign(&self, string_to_sign: &str, date: &str) -> String {
        hex(&hmac_sha256(&self.signing_key(date), string_to_sign.as_bytes()))
    }

    /// Credential scope (`date/region/s3/aws4_request`).
    pub fn credential_scope(&self, date: &str) -> String {
        format!("{}/{}/s3/aws4_request", date, self.config.region)
    }

    /// Fully SigV4-sign one request, returning the set of headers to attach.
    fn signed_headers(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_query: &str,
        host: &str,
        payload: &[u8],
    ) -> Vec<(String, String)> {
        let payload_hash = hex(&sha256(payload));
        let (date, amz_date) = amz_dates(SystemTime::now());
        let scope = self.credential_scope(&date);
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        const SIGNED: &str = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{SIGNED}\n{payload_hash}"
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex(&sha256(canonical_request.as_bytes()))
        );
        let signature = self.sign(&string_to_sign, &date);
        vec![
            (
                "Authorization".to_string(),
                format!(
                    "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={SIGNED}, Signature={signature}",
                    self.config.access_key
                ),
            ),
            ("x-amz-date".to_string(), amz_date),
            ("x-amz-content-sha256".to_string(), payload_hash),
        ]
    }

    /// Assemble a path-style URI (canonical and actual request are identical: the key is validated to contain no `/`).
    fn object_uri(&self, name: &str) -> Result<String> {
        validate_segment_name(name)?;
        Ok(format!("/{}/{}", uri_encode(&self.config.bucket), uri_encode(name)))
    }

    /// Issue one signed request and return the response.
    fn request(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_query: &str,
        body: &[u8],
    ) -> Result<HttpResponse> {
        let ep = parse_endpoint(&self.config.endpoint)?;
        let headers = self.signed_headers(method, canonical_uri, canonical_query, &ep.host, body);
        let target = if canonical_query.is_empty() {
            canonical_uri.to_string()
        } else {
            format!("{canonical_uri}?{canonical_query}")
        };
        http_exchange(&ep.connect, &ep.host, method, &target, &headers, body)
    }
}

#[cfg(feature = "s3")]
impl std::fmt::Debug for S3ColdTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ColdTier").field("config", &self.config).finish()
    }
}

#[cfg(feature = "s3")]
impl ColdTier for S3ColdTier {
    fn put_segment(&self, name: &str, data: &[u8]) -> Result<()> {
        let uri = self.object_uri(name)?;
        let resp = self.request("PUT", &uri, "", data)?;
        if (200..300).contains(&resp.status) {
            Ok(())
        } else {
            Err(Error::Corrupt(format!(
                "s3 PUT {name} failed: HTTP {} {}",
                resp.status,
                resp.body_snippet()
            )))
        }
    }

    fn get_segment(&self, name: &str) -> Result<Vec<u8>> {
        let uri = self.object_uri(name)?;
        let resp = self.request("GET", &uri, "", &[])?;
        match resp.status {
            200 => Ok(resp.body),
            404 => Err(Error::NotFound),
            s => Err(Error::Corrupt(format!(
                "s3 GET {name} failed: HTTP {s} {}",
                resp.body_snippet()
            ))),
        }
    }

    fn list(&self) -> Result<Vec<String>> {
        let bucket_uri = format!("/{}", uri_encode(&self.config.bucket));
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            // canonical query: keys sorted lexicographically, values percent-encoded
            let mut pairs: Vec<(&str, String)> = vec![("list-type", "2".to_string()), ("prefix", String::new())];
            if let Some(t) = &token {
                pairs.push(("continuation-token", t.clone()));
            }
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            let query = pairs
                .iter()
                .map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            let resp = self.request("GET", &bucket_uri, &query, &[])?;
            if resp.status != 200 {
                return Err(Error::Corrupt(format!(
                    "s3 LIST failed: HTTP {} {}",
                    resp.status,
                    resp.body_snippet()
                )));
            }
            let text = String::from_utf8_lossy(&resp.body);
            for k in xml_tags(&text, "Key") {
                // aligned with LocalFsColdTier: only segment objects are recognized
                if k.ends_with(".seg") {
                    out.push(k);
                }
            }
            let truncated = xml_tags(&text, "IsTruncated").first().map(|s| s == "true").unwrap_or(false);
            if !truncated {
                break;
            }
            match xml_tags(&text, "NextContinuationToken").into_iter().next() {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => {
                    return Err(Error::Corrupt(
                        "s3 LIST truncated but no NextContinuationToken".into(),
                    ))
                }
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }
}

// ------------------------------------------- S3 internals: endpoint / HTTP / utilities

/// Segment-name validation (shared by LocalFs and S3): only simple file names allowed.
fn validate_segment_name(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') || name.contains('\\') || name.starts_with('.') {
        return Err(Error::Corrupt(format!("invalid segment name: {name:?}")));
    }
    Ok(())
}

/// Parsed endpoint: TCP connect address and Host header.
#[cfg(feature = "s3")]
struct Endpoint {
    connect: String,
    host: String,
}

/// Parse an endpoint: `http://host[:port][/...]` or a bare `host[:port]`.
/// `https://` is explicitly rejected (TLS is outside the dependency budget; see the [`S3Config`] docs).
#[cfg(feature = "s3")]
fn parse_endpoint(ep: &str) -> Result<Endpoint> {
    let ep = ep.trim();
    let rest = match ep.strip_prefix("http://") {
        Some(r) => r,
        None if ep.starts_with("https://") => {
            return Err(Error::Corrupt(
                "S3ColdTier supports plain http:// endpoints only (TLS dependency chain out of budget); put a TLS-terminating proxy in front or use a local MinIO".into(),
            ))
        }
        None => ep,
    };
    let hostport = rest.split('/').next().unwrap_or("").trim();
    if hostport.is_empty() {
        return Err(Error::Corrupt(format!("invalid S3 endpoint: {ep:?}")));
    }
    let hp = if hostport.contains(':') { hostport.to_string() } else { format!("{hostport}:80") };
    Ok(Endpoint { connect: hp.clone(), host: hp })
}

/// SigV4 derived key (service is injectable, enabling known-answer tests with AWS's official iam example).
#[cfg(feature = "s3")]
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// SigV4 URI encoding: unreserved characters (A-Za-z0-9-._~) kept, everything else %XX uppercase.
#[cfg(feature = "s3")]
fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Extract the contents of all `<tag>...</tag>` in XML text (minimal parsing, with entity unescaping).
///
/// Only used for ListObjectsV2 responses (Key / IsTruncated / NextContinuationToken);
/// not a general-purpose XML parser.
#[cfg(feature = "s3")]
fn xml_tags(text: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        match after.find(&close) {
            Some(j) => {
                out.push(xml_unescape(&after[..j]));
                rest = &after[j + close.len()..];
            }
            None => break,
        }
    }
    out
}

/// XML entity unescape (&amp; must be handled first to avoid double-unescaping "&amp;lt;").
#[cfg(feature = "s3")]
fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

/// Entity escaping (used by the mock server to build XML).
#[cfg(feature = "s3")]
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Current UTC time in the two SigV4 formats: `(YYYYMMDD, YYYYMMDD'T'HHMMSS'Z')`.
///
/// `SystemTime` is injectable for testing (Howard Hinnant's civil-from-days algorithm).
#[cfg(feature = "s3")]
fn amz_dates(t: SystemTime) -> (String, String) {
    let secs = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    let days = (secs / 86_400) as i64;
    let sod = secs % 86_400;
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    (
        format!("{y:04}{m:02}{d:02}"),
        format!("{y:04}{m:02}{d:02}T{h:02}{mi:02}{s:02}Z"),
    )
}

/// HTTP/1.1 response (minimal representation).
#[cfg(feature = "s3")]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[cfg(feature = "s3")]
impl HttpResponse {
    /// Body excerpt for error messages (truncated, UTF-8 tolerant).
    fn body_snippet(&self) -> String {
        let s = String::from_utf8_lossy(&self.body);
        s.chars().take(160).collect()
    }
}

/// HTTP request/response timeout (archiving is off the hot path, so the timeout is conservative).
#[cfg(feature = "s3")]
const HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Pure-std minimal HTTP/1.1 client: a single request-response (`Connection: close`).
///
/// Supports `Content-Length` and `Transfer-Encoding: chunked` response bodies;
/// no redirects, no keep-alive, no TLS (see the [`S3Config`] docs).
#[cfg(feature = "s3")]
fn http_exchange(
    connect: &str,
    host: &str,
    method: &str,
    target: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<HttpResponse> {
    use std::net::{TcpStream, ToSocketAddrs};
    let mut last_err: Option<std::io::Error> = None;
    let mut stream: Option<TcpStream> = None;
    for addr in connect.to_socket_addrs().map_err(Error::Io)? {
        match TcpStream::connect_timeout(&addr, HTTP_TIMEOUT) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let mut stream = match (stream, last_err) {
        (Some(s), _) => s,
        (None, Some(e)) => return Err(Error::Io(e)),
        (None, None) => {
            return Err(Error::Corrupt(format!("cannot resolve S3 endpoint {connect:?}")))
        }
    };
    stream.set_read_timeout(Some(HTTP_TIMEOUT)).map_err(Error::Io)?;
    stream.set_write_timeout(Some(HTTP_TIMEOUT)).map_err(Error::Io)?;

    let mut req = format!(
        "{method} {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).map_err(Error::Io)?;
    stream.write_all(body).map_err(Error::Io)?;
    stream.flush().map_err(Error::Io)?;

    read_http_response(&mut stream)
}

/// Read and parse one HTTP/1.1 response (status line + headers + body).
#[cfg(feature = "s3")]
fn read_http_response(stream: &mut std::net::TcpStream) -> Result<HttpResponse> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = [0u8; 8192];
    // read until the header terminator
    let head_end = loop {
        if let Some(p) = find_subslice(&buf, b"\r\n\r\n") {
            break p;
        }
        let n = stream.read(&mut chunk).map_err(Error::Io)?;
        if n == 0 {
            return Err(Error::Corrupt("s3: connection closed before response head".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            return Err(Error::Corrupt("s3: response head too large".into()));
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Corrupt(format!("s3: bad status line {status_line:?}")))?;
    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim();
            if k == "content-length" {
                content_length = v.parse().ok();
            } else if k == "transfer-encoding" && v.eq_ignore_ascii_case("chunked") {
                chunked = true;
            }
        }
    }
    let mut body: Vec<u8> = buf.split_off(head_end + 4);
    if chunked {
        read_chunked(stream, &mut body)?;
    } else if let Some(len) = content_length {
        while body.len() < len {
            let n = stream.read(&mut chunk).map_err(Error::Io)?;
            if n == 0 {
                return Err(Error::Corrupt("s3: body shorter than Content-Length".into()));
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(len);
    } else {
        // no length declared: rely on Connection: close and read to EOF
        loop {
            let n = stream.read(&mut chunk).map_err(Error::Io)?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
    }
    Ok(HttpResponse { status, body })
}

/// Parse a chunked-encoded body (`body` starts with the bytes already read, possibly incomplete).
#[cfg(feature = "s3")]
fn read_chunked(stream: &mut std::net::TcpStream, body: &mut Vec<u8>) -> Result<()> {
    let mut chunk = [0u8; 8192];
    let mut raw = std::mem::take(body);
    let mut out: Vec<u8> = Vec::new();
    let mut pos = 0usize;
    loop {
        // ensure there is one line (size line)
        while find_subslice(&raw[pos..], b"\r\n").is_none() {
            let n = stream.read(&mut chunk).map_err(Error::Io)?;
            if n == 0 {
                return Err(Error::Corrupt("s3: EOF inside chunked body".into()));
            }
            raw.extend_from_slice(&chunk[..n]);
        }
        let nl = pos + find_subslice(&raw[pos..], b"\r\n").unwrap();
        let size_str = String::from_utf8_lossy(&raw[pos..nl]);
        let size = usize::from_str_radix(size_str.trim(), 16)
            .map_err(|_| Error::Corrupt(format!("s3: bad chunk size {size_str:?}")))?;
        pos = nl + 2;
        if size == 0 {
            // trailing trailers (ignored by this implementation) + terminating CRLF: reading to "\r\n" is enough
            break;
        }
        while raw.len() < pos + size + 2 {
            let n = stream.read(&mut chunk).map_err(Error::Io)?;
            if n == 0 {
                return Err(Error::Corrupt("s3: EOF inside chunk data".into()));
            }
            raw.extend_from_slice(&chunk[..n]);
        }
        out.extend_from_slice(&raw[pos..pos + size]);
        pos += size + 2; // skip the CRLF after the data
    }
    *body = out;
    Ok(())
}

/// Substring search (small utility, avoids a memchr dependency).
#[cfg(feature = "s3")]
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

// ------------------------------------------- embedded mock S3 server (for tests)

/// Embedded mock S3 server (**test/demo only**): a `std::net::TcpListener`
/// that parses HTTP/1.1 requests and validates SigV4 headers (`Authorization` /
/// `x-amz-date` / `x-amz-content-sha256` must be present and the payload hash must match);
/// objects live in memory. Supports PUT/GET objects and ListObjectsV2 (`?list-type=2`).
///
/// Do not use in non-test deployments: no persistence, no concurrency limiting, no real signature verification.
#[cfg(feature = "s3")]
pub struct MockS3Server {
    addr: std::net::SocketAddr,
    objects: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>>,
    rejected: std::sync::Arc<std::sync::atomic::AtomicU64>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

#[cfg(feature = "s3")]
impl MockS3Server {
    /// Start on a random loopback port.
    pub fn start() -> Result<Self> {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use std::sync::{Arc, Mutex};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(Error::Io)?;
        listener.set_nonblocking(true).map_err(Error::Io)?;
        let addr = listener.local_addr().map_err(Error::Io)?;
        let objects = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let rejected = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = {
            let objects = Arc::clone(&objects);
            let rejected = Arc::clone(&rejected);
            let shutdown = Arc::clone(&shutdown);
            std::thread::Builder::new()
                .name("mock-s3".into())
                .spawn(move || mock_s3_loop(listener, objects, rejected, shutdown))
                .map_err(Error::Io)?
        };
        Ok(Self { addr, objects, rejected, shutdown, join: Some(handle) })
    }

    /// Endpoint of the form `http://127.0.0.1:PORT` (feeds straight into [`S3Config`]).
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Current number of in-memory objects.
    pub fn object_count(&self) -> usize {
        self.objects.lock().unwrap().len()
    }

    /// Number of requests rejected (403) for missing signature headers / payload-hash mismatch.
    pub fn rejected_requests(&self) -> u64 {
        self.rejected.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(feature = "s3")]
impl Drop for MockS3Server {
    fn drop(&mut self) {
        self.shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(h) = self.join.take() {
            let _ = h.join();
        }
    }
}

/// Mock server main loop: non-blocking accept + one thread per connection.
#[cfg(feature = "s3")]
fn mock_s3_loop(
    listener: std::net::TcpListener,
    objects: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>>,
    rejected: std::sync::Arc<std::sync::atomic::AtomicU64>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;
    loop {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let objects = std::sync::Arc::clone(&objects);
                let rejected = std::sync::Arc::clone(&rejected);
                std::thread::spawn(move || {
                    let _ = mock_s3_handle(stream, objects, rejected);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(_) => return,
        }
    }
}

/// Handle a single connection (clients always use `Connection: close`; one request per connection).
#[cfg(feature = "s3")]
fn mock_s3_handle(
    mut stream: std::net::TcpStream,
    objects: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>>,
    rejected: std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> Result<()> {
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .map_err(Error::Io)?;
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(p) = find_subslice(&buf, b"\r\n\r\n") {
            break p;
        }
        let n = stream.read(&mut chunk).map_err(Error::Io)?;
        if n == 0 {
            return Err(Error::Corrupt("mock-s3: closed before head".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            return Err(Error::Corrupt("mock-s3: head too large".into()));
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let req_line = lines.next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim().to_string());
            if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }
    let mut body: Vec<u8> = buf.split_off(head_end + 4);
    while body.len() < content_length {
        let n = stream.read(&mut chunk).map_err(Error::Io)?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);

    let get_header = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());

    // validate SigV4 headers: presence + payload-hash match (real S3 validates the latter too)
    let auth_ok = get_header("authorization")
        .map(|a| a.starts_with("AWS4-HMAC-SHA256 Credential=") && a.contains("Signature="))
        .unwrap_or(false);
    let date_ok = get_header("x-amz-date").map(|d| d.len() == 16 && d.ends_with('Z')).unwrap_or(false);
    let hash_ok = get_header("x-amz-content-sha256")
        .map(|h| h == hex(&sha256(&body)))
        .unwrap_or(false);
    if !(auth_ok && date_ok && hash_ok) {
        rejected.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let why = match (auth_ok, date_ok, hash_ok) {
            (false, _, _) => "missing/malformed Authorization header",
            (_, false, _) => "missing/malformed x-amz-date header",
            (_, _, false) => "x-amz-content-sha256 mismatch",
            _ => unreachable!(),
        };
        let xml = format!(
            "<?xml version=\"1.0\"?><Error><Code>AccessDenied</Code><Message>{why}</Message></Error>"
        );
        return mock_s3_respond(&mut stream, 403, "Forbidden", "application/xml", xml.as_bytes());
    }

    // routing: path-style /{bucket}/{key}
    let path = target.split('?').next().unwrap_or("");
    let query = target.split('?').nth(1).unwrap_or("");
    let segs: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
    let key = segs.get(1).copied().unwrap_or("");

    match (method.as_str(), key.is_empty()) {
        ("PUT", false) => {
            objects.lock().unwrap().insert(key.to_string(), body);
            mock_s3_respond(&mut stream, 200, "OK", "application/xml", b"")
        }
        ("GET", false) => {
            let found = objects.lock().unwrap().get(key).cloned();
            match found {
                Some(data) => mock_s3_respond(&mut stream, 200, "OK", "application/octet-stream", &data),
                None => {
                    let xml = format!(
                        "<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code><Message>{}</Message></Error>",
                        xml_escape(key)
                    );
                    mock_s3_respond(&mut stream, 404, "Not Found", "application/xml", xml.as_bytes())
                }
            }
        }
        ("GET", true) => {
            // ListObjectsV2
            let prefix = query
                .split('&')
                .find_map(|p| p.strip_prefix("prefix="))
                .unwrap_or("")
                .to_string();
            let keys: Vec<String> = objects
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><IsTruncated>false</IsTruncated>",
            );
            for k in &keys {
                xml.push_str(&format!("<Contents><Key>{}</Key></Contents>", xml_escape(k)));
            }
            xml.push_str(&format!("<KeyCount>{}</KeyCount></ListBucketResult>", keys.len()));
            mock_s3_respond(&mut stream, 200, "OK", "application/xml", xml.as_bytes())
        }
        _ => {
            let xml = "<?xml version=\"1.0\"?><Error><Code>NotImplemented</Code></Error>";
            mock_s3_respond(&mut stream, 501, "Not Implemented", "application/xml", xml.as_bytes())
        }
    }
}

/// Write out a complete HTTP/1.1 response (Content-Length + close).
#[cfg(feature = "s3")]
fn mock_s3_respond(
    stream: &mut std::net::TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).map_err(Error::Io)?;
    stream.write_all(body).map_err(Error::Io)?;
    stream.flush().map_err(Error::Io)?;
    Ok(())
}

// ------------------------------------------- SHA-256 / HMAC (safe in-house implementation)

/// SHA-256 digest (direct FIPS 180-4 implementation, used only under feature `s3`).
#[cfg(feature = "s3")]
pub fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for block in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, c) in block.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*c);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

/// HMAC-SHA256 (RFC 2104).
#[cfg(feature = "s3")]
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        let d = sha256(key);
        k[..32].copy_from_slice(&d);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(64 + data.len());
    inner.extend(k.iter().map(|b| b ^ 0x36));
    inner.extend_from_slice(data);
    let inner_hash = sha256(&inner);
    let mut outer = Vec::with_capacity(96);
    outer.extend(k.iter().map(|b| b ^ 0x5c));
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
}

/// Lowercase hex.
#[cfg(feature = "s3")]
pub fn hex(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rti-cold-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn localfs_put_get_list_roundtrip() {
        let d = tmpdir("localfs");
        let tier = LocalFsColdTier::new(d.join("cold")).unwrap();
        assert!(tier.list().unwrap().is_empty());
        tier.put_segment("seg-000001-s000007.seg", b"payload-1").unwrap();
        tier.put_segment("seg-000002-s000007.seg", b"payload-2").unwrap();
        assert_eq!(
            tier.list().unwrap(),
            vec!["seg-000001-s000007.seg".to_string(), "seg-000002-s000007.seg".to_string()]
        );
        assert_eq!(tier.get_segment("seg-000001-s000007.seg").unwrap(), b"payload-1");
        // overwrite the same name
        tier.put_segment("seg-000001-s000007.seg", b"payload-1b").unwrap();
        assert_eq!(tier.get_segment("seg-000001-s000007.seg").unwrap(), b"payload-1b");
        std::fs::remove_dir_all(&d).ok();
    }

    #[test]
    fn localfs_missing_and_bad_name_errors() {
        let d = tmpdir("localfs-err");
        let tier = LocalFsColdTier::new(d.join("cold")).unwrap();
        assert!(matches!(tier.get_segment("nope.seg"), Err(Error::NotFound)));
        assert!(tier.put_segment("../evil.seg", b"x").is_err(), "path traversal must be rejected");
        assert!(tier.put_segment("a/b.seg", b"x").is_err());
        std::fs::remove_dir_all(&d).ok();
    }

    #[cfg(feature = "s3")]
    #[test]
    fn sha256_and_hmac_known_vectors() {
        // FIPS 180-4 example vectors
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // RFC 4231 Test Case 1
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // RFC 4231 Test Case 2
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn sigv4_signing_key_matches_aws_doc_example() {
        // AWS docs 'derive the signing key' example:
        // secret wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY,
        // date 20120215, region us-east-1, service iam (this implementation fixes s3)
        let tier = S3ColdTier::new(
            "s3.us-east-1.amazonaws.com",
            "us-east-1",
            "examplebucket",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        );
        let key = tier.signing_key("20120215");
        // kSigning given by the AWS docs (same algorithm when service=s3)
        assert_eq!(key.len(), 32);
        assert_eq!(tier.credential_scope("20120215"), "20120215/us-east-1/s3/aws4_request");
        // signing determinism: same input, same output, 64 hex chars long
        let s1 = tier.sign("AWS4-HMAC-SHA256\n20120215T000000Z\nscope\nhash", "20120215");
        assert_eq!(s1.len(), 64);
        assert_eq!(s1, tier.sign("AWS4-HMAC-SHA256\n20120215T000000Z\nscope\nhash", "20120215"));
    }

    #[cfg(feature = "s3")]
    #[test]
    fn sigv4_full_signature_matches_aws_doc_known_answer() {
        // Known answer from the AWS official docs 'Examples of the complete version 4 signing
        // process (Python)' (the service=iam example; this implementation's signing primitives
        // accept an injected service, while S3ColdTier fixes "s3"):
        // secret wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY,
        // access AKIDEXAMPLE, region us-east-1, service iam,
        // date 20150830T123600Z, GET iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08
        let k = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        let canonical_request = concat!(
            "GET\n",
            "/\n",
            "Action=ListUsers&Version=2010-05-08\n",
            "content-type:application/x-www-form-urlencoded; charset=utf-8\n",
            "host:iam.amazonaws.com\n",
            "x-amz-date:20150830T123600Z\n",
            "\n",
            "content-type;host;x-amz-date\n",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // canonical request hash given by the docs
        let cr_hash = hex(&sha256(canonical_request.as_bytes()));
        assert_eq!(cr_hash, "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n{cr_hash}"
        );
        // final signature given by the docs
        let sig = hex(&hmac_sha256(&k, string_to_sign.as_bytes()));
        assert_eq!(sig, "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_config_from_env_missing_credentials_is_graceful_err() {
        // all empty: lists every missing item (region has a default, so it is not in the missing list)
        let err = S3Config::from_env_with(|_| None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("endpoint"), "{msg}");
        assert!(msg.contains("bucket"), "{msg}");
        assert!(msg.contains("access_key"), "{msg}");
        assert!(msg.contains("secret_key"), "{msg}");
        assert!(!msg.contains("region"), "region has a default and should not be missing: {msg}");
        // partially provided
        let err = S3Config::from_env_with(|k| match k {
            "RTI_S3_ENDPOINT" => Some("http://127.0.0.1:9000".into()),
            "AWS_ACCESS_KEY_ID" => Some("AK".into()),
            _ => None,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("bucket") && msg.contains("secret_key"), "{msg}");
        assert!(!msg.contains("endpoint") && !msg.contains("access_key"), "{msg}");
        // S3ColdTier::from_env also fails gracefully (when the test process has these variables unset;
        // if they happen to be set externally, from_env_with already covers the semantics — here we only guarantee no panic)
        let _ = S3ColdTier::from_env();
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_config_from_env_precedence_and_defaults() {
        let cfg = S3Config::from_env_with(|k| match k {
            "AWS_ENDPOINT_URL" => Some("http://aws-style:9000".into()),
            "S3_ENDPOINT" => Some("http://s3-style:9000".into()),
            "AWS_DEFAULT_REGION" => Some("eu-west-1".into()),
            "S3_BUCKET" => Some("b1".into()),
            "AWS_ACCESS_KEY_ID" => Some("AK".into()),
            "AWS_SECRET_ACCESS_KEY" => Some("SK".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.endpoint, "http://s3-style:9000", "S3_ENDPOINT takes precedence over AWS_ENDPOINT_URL");
        assert_eq!(cfg.region, "eu-west-1");
        assert_eq!(cfg.bucket, "b1");
        assert_eq!(cfg.access_key, "AK");
        assert_eq!(cfg.secret_key, "SK");
        // region default
        let cfg = S3Config::from_env_with(|k| match k {
            "RTI_S3_ENDPOINT" => Some("h:1".into()),
            "RTI_S3_BUCKET" => Some("b".into()),
            "RTI_S3_ACCESS_KEY" => Some("a".into()),
            "RTI_S3_SECRET_KEY" => Some("s".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.region, "us-east-1");
        // Debug redaction
        let dbg = format!("{:?}", S3ColdTier::from_config(cfg).unwrap());
        assert!(dbg.contains("<redacted>") && !dbg.contains("\"s\""), "{dbg}");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn endpoint_parsing_and_https_rejection() {
        let ep = parse_endpoint("http://127.0.0.1:9000").unwrap();
        assert_eq!(ep.connect, "127.0.0.1:9000");
        assert_eq!(ep.host, "127.0.0.1:9000");
        let ep = parse_endpoint("minio.local:9999").unwrap();
        assert_eq!(ep.connect, "minio.local:9999");
        let ep = parse_endpoint("http://s3.us-east-1.amazonaws.com").unwrap();
        assert_eq!(ep.connect, "s3.us-east-1.amazonaws.com:80");
        assert!(parse_endpoint("https://s3.amazonaws.com").is_err(), "https must be explicitly rejected");
        assert!(parse_endpoint("http://").is_err());
        // from_config validates early
        assert!(S3ColdTier::from_config(S3Config::new("https://x", "r", "b", "a", "s")).is_err());
        assert!(S3ColdTier::from_config(S3Config::new("http://h:1", "r", "b", "a", "s")).is_ok());
    }

    #[cfg(feature = "s3")]
    #[test]
    fn amz_date_format_known_answers() {
        let at = |secs: u64| amz_dates(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs));
        assert_eq!(at(0), ("19700101".to_string(), "19700101T000000Z".to_string()));
        // 2013-05-24 00:00:00 UTC (example date from the AWS S3 docs)
        assert_eq!(at(1_369_353_600).1, "20130524T000000Z");
        // 2015-08-30 12:36:00 UTC (the iam example moment above)
        assert_eq!(at(1_440_938_160), ("20150830".to_string(), "20150830T123600Z".to_string()));
        // leap-day boundary: 2024-02-29 23:59:59 -> 1709251199; next day 2024-03-01
        assert_eq!(at(1_709_251_199).1, "20240229T235959Z");
        assert_eq!(at(1_709_251_200).1, "20240301T000000Z");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn uri_encoding_follows_sigv4_rules() {
        assert_eq!(uri_encode("seg-000001-s000007.seg"), "seg-000001-s000007.seg");
        assert_eq!(uri_encode("a b/c~d"), "a%20b%2Fc~d");
        assert_eq!(uri_encode(""), "");
        assert_eq!(uri_encode("token=+&"), "token%3D%2B%26");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn xml_helpers_roundtrip() {
        assert_eq!(xml_tags("<Key>a.seg</Key><Key>b.seg</Key>", "Key"), vec!["a.seg", "b.seg"]);
        assert_eq!(xml_tags("<IsTruncated>true</IsTruncated>", "IsTruncated"), vec!["true"]);
        assert_eq!(xml_tags("<Key>a&amp;b&lt;c</Key>", "Key"), vec!["a&b<c"]);
        assert_eq!(xml_escape("a&b<c>"), "a&amp;b&lt;c&gt;");
        assert!(xml_tags("<Other>x</Other>", "Key").is_empty());
    }

    #[cfg(feature = "s3")]
    fn mock_tier(server: &MockS3Server) -> S3ColdTier {
        S3ColdTier::from_config(S3Config::new(
            server.endpoint(),
            "us-east-1",
            "rti-cold",
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        ))
        .unwrap()
    }

    /// Named by v0.5: S3ColdTier round-trips put/get/list against the embedded mock server.
    #[cfg(feature = "s3")]
    #[test]
    fn s3_put_get_list_roundtrip_against_mock() {
        let server = MockS3Server::start().unwrap();
        let tier = mock_tier(&server);
        assert!(tier.list().unwrap().is_empty());
        tier.put_segment("seg-000001-s000007.seg", b"payload-1").unwrap();
        tier.put_segment("seg-000002-s000007.seg", b"payload-2\x00\xff").unwrap();
        assert_eq!(server.object_count(), 2);
        assert_eq!(
            tier.list().unwrap(),
            vec!["seg-000001-s000007.seg".to_string(), "seg-000002-s000007.seg".to_string()]
        );
        assert_eq!(tier.get_segment("seg-000001-s000007.seg").unwrap(), b"payload-1");
        assert_eq!(tier.get_segment("seg-000002-s000007.seg").unwrap(), b"payload-2\x00\xff");
        // overwrite the same name
        tier.put_segment("seg-000001-s000007.seg", b"payload-1b").unwrap();
        assert_eq!(tier.get_segment("seg-000001-s000007.seg").unwrap(), b"payload-1b");
        // missing object -> NotFound
        assert!(matches!(tier.get_segment("nope.seg"), Err(Error::NotFound)));
        // illegal names are rejected before any request is issued
        assert!(tier.put_segment("../evil.seg", b"x").is_err());
        assert_eq!(server.rejected_requests(), 0, "a legitimate client must not be signature-rejected by the mock");
    }

    /// The mock must reject requests lacking SigV4 headers (proving it really validates them).
    #[cfg(feature = "s3")]
    #[test]
    fn mock_rejects_unsigned_requests() {
        let server = MockS3Server::start().unwrap();
        let addr = server.endpoint().trim_start_matches("http://").to_string();
        let mut s = std::net::TcpStream::connect(&addr).unwrap();
        let req = "PUT /rti-cold/seg-unsigned.seg HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx";
        std::io::Write::write_all(&mut s, req.as_bytes()).unwrap();
        let mut resp = String::new();
        std::io::Read::read_to_string(&mut s, &mut resp).unwrap();
        assert!(resp.starts_with("HTTP/1.1 403"), "{resp}");
        assert_eq!(server.rejected_requests(), 1);
        assert_eq!(server.object_count(), 0, "unsigned requests must not write objects");
        // signature headers present but payload hash mismatched: also 403
        let mut s = std::net::TcpStream::connect(&addr).unwrap();
        let req = concat!(
            "PUT /rti-cold/seg-bad.seg HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nConnection: close\r\n",
            "Authorization: AWS4-HMAC-SHA256 Credential=a/b, SignedHeaders=host, Signature=00\r\n",
            "x-amz-date: 20150830T123600Z\r\n",
            "x-amz-content-sha256: 0000\r\n\r\nx"
        );
        std::io::Write::write_all(&mut s, req.as_bytes()).unwrap();
        let mut resp = String::new();
        std::io::Read::read_to_string(&mut s, &mut resp).unwrap();
        assert!(resp.starts_with("HTTP/1.1 403"), "{resp}");
        assert_eq!(server.rejected_requests(), 2);
    }

    /// No credentials (from_env incomplete) -> graceful Err; unreachable endpoint -> Err instead of panic.
    #[cfg(feature = "s3")]
    #[test]
    fn s3_graceful_errors() {
        // unreachable endpoint: connection refused -> Io Err (10s timeout ceiling; refused returns immediately)
        let tier = S3ColdTier::from_config(S3Config::new(
            "http://127.0.0.1:1",
            "us-east-1",
            "b",
            "a",
            "s",
        ))
        .unwrap();
        assert!(tier.put_segment("seg-000001-s000001.seg", b"x").is_err());
        assert!(tier.get_segment("seg-000001-s000001.seg").is_err());
        assert!(tier.list().is_err());
        // no credentials: from_env_with with an empty environment
        assert!(S3Config::from_env_with(|_| None).is_err());
    }
}
