//! The control-plane client. Tools depend on [`Control`], so tests pass a script instead of a socket.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Largest control response this process will buffer. Larger replies are refused.
pub const MAX_HTTP_BODY: usize = 32 << 20;

/// One HTTP reply. Transport failures are `Err`; an HTTP error status is still `Ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawReply {
    pub status: u16,
    pub body: Vec<u8>,
}

/// A running fakeprint, or a test double.
pub trait Control {
    fn base(&self) -> &str;
    fn exchange(
        &mut self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<RawReply, String>;
}

/// Blocking HTTP/1.1 client for `http://host:port`. One connection per call, then close.
#[derive(Debug, Clone)]
pub struct HttpControl {
    host: String,
    port: u16,
    base: String,
}

impl HttpControl {
    /// `http://127.0.0.1:3921`. No path, no userinfo, no TLS (the control plane is plain HTTP).
    pub fn parse(base_url: &str) -> Result<Self, String> {
        let rest = base_url.trim().trim_end_matches('/');
        let Some(rest) = rest.strip_prefix("http://") else {
            return Err(
                "base URL must be http://host:port (the control plane has no TLS listener)".into(),
            );
        };
        if rest.is_empty()
            || rest.contains('/')
            || rest.contains('@')
            || rest.contains('?')
            || rest.contains('#')
        {
            return Err("base URL must be http://host:port with no path".into());
        }
        let (host, port) = split_host_port(rest)?;
        Ok(Self {
            host,
            port,
            base: format!("http://{rest}"),
        })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    fn host_header(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

impl Control for HttpControl {
    fn base(&self) -> &str {
        &self.base
    }

    fn exchange(
        &mut self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<RawReply, String> {
        guard_path(path)?;
        if let Some(b) = body {
            if b.len() > MAX_HTTP_BODY {
                return Err(format!(
                    "refusing to send {} bytes (cap is {MAX_HTTP_BODY})",
                    b.len()
                ));
            }
        }
        let addr = format!("{}:{}", self.host, self.port);
        let mut iter = addr
            .to_socket_addrs()
            .map_err(|e| format!("cannot resolve {addr}: {e}"))?;
        let sockaddr = iter
            .next()
            .ok_or_else(|| format!("cannot resolve {addr}"))?;
        let mut stream = TcpStream::connect_timeout(&sockaddr, Duration::from_secs(5))
            .map_err(|e| format!("connect {addr}: {e}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| format!("socket: {e}"))?;
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| format!("socket: {e}"))?;

        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAccept: application/json, application/octet-stream\r\nConnection: close\r\n",
            self.host_header()
        );
        if let Some(b) = body {
            req.push_str("Content-Type: application/json\r\n");
            req.push_str(&format!("Content-Length: {}\r\n", b.len()));
        }
        req.push_str("\r\n");
        stream
            .write_all(req.as_bytes())
            .map_err(|e| format!("write {addr}: {e}"))?;
        if let Some(b) = body {
            stream
                .write_all(b)
                .map_err(|e| format!("write {addr}: {e}"))?;
        }
        stream.flush().map_err(|e| format!("write {addr}: {e}"))?;
        let mut reader = BufReader::new(stream);
        read_http_response(&mut reader)
    }
}

pub fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|c| !c.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|c| !c.is_ascii_whitespace())
        .map_or(start, |i| i + 1);
    &bytes[start..end]
}

fn guard_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') || path.contains([' ', '\r', '\n', '\0']) {
        return Err("refusing a malformed control path".into());
    }
    Ok(())
}

fn split_host_port(rest: &str) -> Result<(String, u16), String> {
    if let Some(rest) = rest.strip_prefix('[') {
        let (host, port) = rest
            .split_once("]:")
            .ok_or("IPv6 base URL must look like http://[::1]:3921")?;
        if host.is_empty() {
            return Err("empty host".into());
        }
        let port = parse_port(port)?;
        return Ok((host.to_string(), port));
    }
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or("base URL needs a port, for example http://127.0.0.1:3921")?;
    if host.is_empty() || host.contains(':') {
        return Err("IPv6 base URL must be bracketed: http://[::1]:3921".into());
    }
    Ok((host.to_string(), parse_port(port)?))
}

fn parse_port(port: &str) -> Result<u16, String> {
    port.parse::<u16>()
        .map_err(|_| format!("bad port {port:?}"))
}

pub fn read_http_response(reader: &mut impl BufRead) -> Result<RawReply, String> {
    let status_line = read_line(reader, 8 * 1024)?;
    let mut parts = status_line.split_whitespace();
    let version = parts.next().ok_or("empty HTTP status line")?;
    if !version.starts_with("HTTP/1.") {
        return Err(format!("not an HTTP/1 response: {status_line}"));
    }
    let code: u16 = parts
        .next()
        .ok_or("HTTP status line has no code")?
        .parse()
        .map_err(|_| format!("bad HTTP status: {status_line}"))?;

    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    loop {
        let line = read_line(reader, 8 * 1024)?;
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(format!("bad HTTP header: {line}"));
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        if name == "content-length" {
            let n = value
                .parse::<usize>()
                .map_err(|_| format!("bad Content-Length {value:?}"))?;
            content_length = Some(n);
        } else if name == "transfer-encoding" && value.to_ascii_lowercase().contains("chunked") {
            chunked = true;
        }
    }

    let body = if chunked {
        read_chunked(reader)?
    } else if let Some(n) = content_length {
        read_exact_capped(reader, n)?
    } else {
        let mut body = Vec::new();
        reader
            .take((MAX_HTTP_BODY as u64) + 1)
            .read_to_end(&mut body)
            .map_err(|e| format!("reading control response: {e}"))?;
        if body.len() > MAX_HTTP_BODY {
            return Err(format!(
                "control response exceeds the {MAX_HTTP_BODY} byte cap"
            ));
        }
        body
    };
    Ok(RawReply { status: code, body })
}

fn read_line(reader: &mut impl BufRead, max: usize) -> Result<String, String> {
    let mut buf = Vec::new();
    loop {
        let (n, done) = {
            let avail = reader
                .fill_buf()
                .map_err(|e| format!("reading headers: {e}"))?;
            if avail.is_empty() {
                if buf.is_empty() {
                    return Err("connection closed before the HTTP response".into());
                }
                return Err("connection closed in the HTTP headers".into());
            }
            if let Some(i) = avail.iter().position(|b| *b == b'\n') {
                buf.extend_from_slice(&avail[..=i]);
                (i + 1, true)
            } else if buf.len() + avail.len() > max {
                return Err("HTTP header line is too long".into());
            } else {
                let n = avail.len();
                buf.extend_from_slice(avail);
                (n, false)
            }
        };
        reader.consume(n);
        if done {
            break;
        }
    }
    if buf.len() > max {
        return Err("HTTP header line is too long".into());
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    String::from_utf8(buf).map_err(|_| "HTTP headers are not UTF-8".into())
}

fn read_exact_capped(reader: &mut impl BufRead, n: usize) -> Result<Vec<u8>, String> {
    if n > MAX_HTTP_BODY {
        return Err(format!(
            "control response is {n} bytes, over the {MAX_HTTP_BODY} byte cap"
        ));
    }
    let mut buf = vec![0u8; n];
    reader
        .read_exact(&mut buf)
        .map_err(|e| format!("reading control response: {e}"))?;
    Ok(buf)
}

fn read_chunked(reader: &mut impl BufRead) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    loop {
        let line = read_line(reader, 1024)?;
        let size_hex = line.split(';').next().unwrap_or("").trim();
        let n =
            usize::from_str_radix(size_hex, 16).map_err(|_| format!("bad chunk size {line:?}"))?;
        if n == 0 {
            loop {
                let trailer = read_line(reader, 8 * 1024)?;
                if trailer.is_empty() {
                    break;
                }
            }
            break;
        }
        if body.len().saturating_add(n) > MAX_HTTP_BODY {
            return Err(format!(
                "chunked control response exceeds the {MAX_HTTP_BODY} byte cap"
            ));
        }
        let mut chunk = vec![0u8; n];
        reader
            .read_exact(&mut chunk)
            .map_err(|e| format!("reading chunk: {e}"))?;
        body.extend_from_slice(&chunk);
        let mut crlf = [0u8; 2];
        reader
            .read_exact(&mut crlf)
            .map_err(|e| format!("reading chunk end: {e}"))?;
        if &crlf != b"\r\n" {
            return Err("chunk did not end with CRLF".into());
        }
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn base_url_must_be_http_host_port() {
        assert!(HttpControl::parse("http://127.0.0.1:3921").is_ok());
        assert!(HttpControl::parse("http://127.0.0.1:3921/").is_ok());
        assert!(HttpControl::parse("http://[::1]:3921").is_ok());
        for bad in [
            "https://127.0.0.1:3921",
            "http://127.0.0.1",
            "http://127.0.0.1:3921/v1",
            "http://user:pw@127.0.0.1:3921",
            "127.0.0.1:3921",
        ] {
            assert!(HttpControl::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn chunked_and_length_prefixed_bodies_decode() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        let r = read_http_response(&mut &raw[..]).unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, b"hello");

        let raw = b"HTTP/1.0 404 Not Found\r\nContent-Length: 13\r\n\r\n{\"error\":\"x\"}";
        let r = read_http_response(&mut &raw[..]).unwrap();
        assert_eq!(r.status, 404);
        assert_eq!(r.body, b"{\"error\":\"x\"}");
    }

    #[test]
    fn a_declared_body_over_the_cap_is_refused_without_buffering_it() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n";
        let err = read_http_response(&mut &raw[..]).unwrap_err();
        assert!(err.contains("cap"), "{err}");
    }

    #[test]
    fn the_client_reads_a_live_loopback_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(sock.try_clone().unwrap());
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" || line == "\n" {
                    break;
                }
            }
            sock = reader.into_inner();
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let mut client = HttpControl::parse(&format!("http://127.0.0.1:{port}")).unwrap();
        let reply = client.exchange("GET", "/health", None).unwrap();
        assert_eq!(reply.status, 200);
        assert_eq!(reply.body, b"ok");
        server.join().unwrap();
    }
}
