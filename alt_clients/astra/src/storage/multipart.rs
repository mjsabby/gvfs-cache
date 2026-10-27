use super::*;
use base64::Engine as _;

fn query(url: &str, name: &str, value: &str) -> String {
    let mut out = String::with_capacity(
        url.len()
            .saturating_add(value.len().saturating_mul(3))
            .saturating_add(name.len())
            .saturating_add(2),
    );
    out.push_str(url);
    out.push(if url.contains('?') { '&' } else { '?' });
    out.push_str(name);
    out.push('=');
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            out.push('%');
            if let (Some(&hi), Some(&lo)) = (
                HEX.get(usize::from(byte >> 4)),
                HEX.get(usize::from(byte & 15)),
            ) {
                out.push(char::from(hi));
                out.push(char::from(lo));
            }
        }
    }
    out
}

fn xml_escape(value: &str, out: &mut String) {
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
}

fn response_text(response: &mut ureq::http::Response<ureq::Body>) -> Result<String> {
    let mut text = String::new();
    response
        .body_mut()
        .as_reader()
        .take(65537)
        .read_to_string(&mut text)?;
    if text.len() > 65536 {
        return Err(Error::Capacity("storage XML"));
    }
    Ok(text)
}

impl HttpStore {
    pub(super) fn multipart(&self, key: &str, path: &Path, part_bytes: usize) -> Result<()> {
        if part_bytes == 0 {
            return Err(Error::Capacity("upload part size"));
        }
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
        let parts = len.div_ceil(part_bytes as u64);
        if parts == 0 || parts > if self.azure { 50000 } else { 10000 } {
            return Err(Error::Capacity("multipart part count"));
        }
        let base = self.url(key);
        let upload = if self.azure {
            None
        } else {
            let request = self
                .request("POST", &query(&base, "uploads", ""))?
                .body(())
                .map_err(|_| Error::Invalid("multipart request"))?;
            let mut response = self.agent.run(request)?;
            status(response.status().as_u16(), 200)?;
            let text = response_text(&mut response)?;
            let id = text
                .split_once("<UploadId>")
                .and_then(|(_, tail)| tail.split_once("</UploadId>"))
                .map(|(id, _)| id)
                .filter(|id| !id.is_empty() && !id.contains('&') && !id.contains('<'))
                .ok_or(Error::Invalid("S3 upload ID"))?;
            Some(id.to_owned())
        };
        let result = self.multipart_parts(&base, upload.as_deref(), &mut file, len, part_bytes);
        if result.is_err()
            && let Some(upload) = upload
        {
            let abort = || -> Result<()> {
                let request = self
                    .request("DELETE", &query(&base, "uploadId", &upload))?
                    .body(())
                    .map_err(|_| Error::Invalid("abort request"))?;
                let response = self.agent.run(request)?;
                match response.status().as_u16() {
                    204 | 404 => Ok(()),
                    s => Err(Error::Http(s)),
                }
            };
            if let Err(error) = abort() {
                eprintln!("multipart abort failed: {error}");
            }
        }
        result
    }

    fn multipart_parts(
        &self,
        base: &str,
        upload: Option<&str>,
        file: &mut File,
        len: u64,
        part_bytes: usize,
    ) -> Result<()> {
        let mut buffer = crate::arena::Arena::new(part_bytes)?;
        let mut xml = String::with_capacity(
            usize::try_from(len.div_ceil(part_bytes as u64))
                .map_err(|_| Error::Capacity("parts"))?
                .saturating_mul(256)
                .saturating_add(128),
        );
        xml.push_str(if self.azure {
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>"
        } else {
            "<CompleteMultipartUpload>"
        });
        let mut offset = 0u64;
        let mut part = 1u32;
        while offset < len {
            let n = len.saturating_sub(offset).min(part_bytes as u64) as usize;
            let bytes = buffer
                .buffer()
                .get_mut(..n)
                .ok_or(Error::Capacity("upload buffer"))?;
            file.read_exact(bytes)?;
            let id = base64::prelude::BASE64_STANDARD.encode(part.to_be_bytes());
            let url = if self.azure {
                query(&query(base, "comp", "block"), "blockid", &id)
            } else {
                query(
                    &query(base, "partNumber", &part.to_string()),
                    "uploadId",
                    upload.ok_or(Error::Invalid("upload ID"))?,
                )
            };
            let request = self
                .request("PUT", &url)?
                .body(&*bytes)
                .map_err(|_| Error::Invalid("upload part request"))?;
            let response = self.agent.run(request)?;
            if self.azure {
                status(response.status().as_u16(), 201)?;
                xml.push_str("<Latest>");
                xml_escape(&id, &mut xml);
                xml.push_str("</Latest>");
            } else {
                status(response.status().as_u16(), 200)?;
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|e| e.to_str().ok())
                    .ok_or(Error::Invalid("part ETag"))?;
                xml.push_str("<Part><PartNumber>");
                xml.push_str(&part.to_string());
                xml.push_str("</PartNumber><ETag>");
                xml_escape(etag, &mut xml);
                xml.push_str("</ETag></Part>");
            }
            part = part.checked_add(1).ok_or(Error::Capacity("upload parts"))?;
            offset = offset
                .checked_add(n as u64)
                .ok_or(Error::Capacity("upload offset"))?;
        }
        xml.push_str(if self.azure {
            "</BlockList>"
        } else {
            "</CompleteMultipartUpload>"
        });
        let url = if self.azure {
            query(base, "comp", "blocklist")
        } else {
            query(base, "uploadId", upload.ok_or(Error::Invalid("upload ID"))?)
        };
        let request = self
            .request(if self.azure { "PUT" } else { "POST" }, &url)?
            .header("If-None-Match", "*")
            .header("Content-Type", "application/xml")
            .body(xml)
            .map_err(|_| Error::Invalid("multipart completion request"))?;
        let mut response = self.agent.run(request)?;
        match response.status().as_u16() {
            409 | 412 => return Err(Error::Conflict),
            s => status(s, if self.azure { 201 } else { 200 })?,
        }
        if !self.azure {
            let text = response_text(&mut response)?;
            if text.contains("<Error>") || !text.contains("<CompleteMultipartUploadResult") {
                return Err(Error::Invalid(
                    "S3 multipart completion returned an error body",
                ));
            }
        }
        Ok(())
    }
}
