//! Compressed text: `t.csv.gz`, `t.ndjson.zst`, `t.tsv.bz2`, `t.json.xz`, read by the text readers
//! as their decompressed bytes.
//!
//! A compressed file, local or in an object store, is handed to its reader as a
//! [`RemoteObject`] whose bytes are the decompressed ones: [`Decompressed`]. The text readers
//! already read an object as its bytes arrive, a pass at a time, so every one of them reads a
//! compressed file with no code of its own, and an object in a store is decompressed as it
//! streams rather than downloaded first. A compressed stream cannot be read from the middle, so a
//! range of it — a JSON records member — is read from the start each time, past the bytes before
//! it.
//!
//! Every read stops at [`max_decompressed`] bytes, the guard against a small file that inflates
//! without end: past it, the read fails with [`EngineError::TooLarge`], named for the limit.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::RemoteObject;
use crate::error::{EngineError, Result};
use crate::source::Codec;

/// What the decompressed bytes are read through: enough to keep a decoder fed without a read
/// per few bytes.
const BUFFER: usize = 64 * 1024;

/// Where a compressed file's own bytes are.
#[derive(Debug)]
pub(crate) enum Stored {
    File(PathBuf),
    Object(Arc<dyn RemoteObject>),
}

/// A compressed file read as its decompressed bytes — see the module docs.
///
/// Its [`size`](RemoteObject::size) is the size as stored, which with the
/// [`version`](RemoteObject::version) is what a reader keys what it learns by: the decompressed
/// size is not known until every byte has been read. A reader that holds the bytes it reads
/// bounds that by what it reads, not by this.
#[derive(Debug)]
pub(crate) struct Decompressed {
    stored: Stored,
    codec: Codec,
    /// The most bytes a read decompresses.
    cap: u64,
    uri: String,
    identity: String,
    version: String,
    size: u64,
    /// Set by a read that went past `cap`, so the error it ends with can be reported as the
    /// limit, whatever a reader made of it on the way out.
    tripped: AtomicBool,
}

impl Decompressed {
    /// `stored`, compressed with `codec`, read as at most `cap` decompressed bytes. Refused for a
    /// codec this build does not decode.
    pub(crate) fn new(stored: Stored, codec: Codec, cap: u64) -> Result<Decompressed> {
        if !codec.decodable() {
            return Err(undecodable(&stored.uri(), codec));
        }
        let (identity, version, size) = match &stored {
            Stored::File(path) => {
                let meta = std::fs::metadata(path)?;
                let modified = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_nanos());
                (
                    String::new(),
                    format!("{}@{modified}", meta.len()),
                    meta.len(),
                )
            }
            Stored::Object(object) => (
                object.identity().to_string(),
                object.version().to_string(),
                object.size(),
            ),
        };
        Ok(Decompressed {
            uri: stored.uri(),
            stored,
            codec,
            cap,
            identity,
            version: format!("{codec}:{version}"),
            size,
            tripped: AtomicBool::new(false),
        })
    }

    /// `e`, or — when a read of this file went past the cap, which is then why it failed — the
    /// error that names the limit.
    pub(crate) fn or_too_large(&self, e: EngineError) -> EngineError {
        if self.tripped.load(Ordering::SeqCst) {
            return EngineError::TooLarge(too_large(&self.uri, self.cap));
        }
        e
    }
}

impl Stored {
    /// `r`, with an error reading it marked as one, so that what goes wrong reading the stored bytes
    /// — a dropped connection, a disk — is not reported as the decoder's verdict on them.
    fn marked<R: BufRead>(r: R) -> Marked<R> {
        Marked(r)
    }

    fn uri(&self) -> String {
        match self {
            Stored::File(path) => path.display().to_string(),
            Stored::Object(object) => object.uri().to_string(),
        }
    }
}

impl RemoteObject for Decompressed {
    fn uri(&self) -> &str {
        &self.uri
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn version(&self) -> &str {
        &self.version
    }

    /// The decompressed bytes, or those in `range` of them: read from the start, as a compressed
    /// stream can only be.
    fn open(&self, range: Option<Range<u64>>) -> Result<Box<dyn BufRead + Send + '_>> {
        let raw: Box<dyn BufRead + Send + '_> = match &self.stored {
            Stored::File(path) => Box::new(Stored::marked(BufReader::with_capacity(
                BUFFER,
                File::open(path)?,
            ))),
            Stored::Object(object) => Box::new(Stored::marked(object.open(None)?)),
        };
        let mut decoded = BufReader::with_capacity(
            BUFFER,
            Capped {
                inner: decoder(self.codec, raw)?,
                left: self.cap,
                file: self,
            },
        );
        let Some(range) = range else {
            return Ok(Box::new(decoded));
        };
        std::io::copy(&mut (&mut decoded).take(range.start), &mut std::io::sink())?;
        Ok(Box::new(
            decoded.take(range.end.saturating_sub(range.start)),
        ))
    }
}

/// A decoder's output, stopped with an error at the file's cap.
struct Capped<'a, R> {
    inner: R,
    /// Bytes still allowed.
    left: u64,
    file: &'a Decompressed,
}

impl<R: Read> Read for Capped<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.left == 0 {
            // Every allowed byte is read: one more is over the cap, and none is the end.
            let mut one = [0u8; 1];
            if self.inner.read(&mut one)? == 0 {
                return Ok(0);
            }
            self.file.tripped.store(true, Ordering::SeqCst);
            return Err(std::io::Error::other(too_large(
                &self.file.uri,
                self.file.cap,
            )));
        }
        let want = buf
            .len()
            .min(usize::try_from(self.left).unwrap_or(usize::MAX));
        let n = self
            .inner
            .read(&mut buf[..want])
            .map_err(|e| self.file.decode_error(e))?;
        self.left -= n as u64;
        Ok(n)
    }
}

impl Decompressed {
    /// A decoder's error: one reading the stored bytes as it was, anything else the decoder's
    /// verdict that they are not what the name says, named with the file.
    fn decode_error(&self, e: std::io::Error) -> std::io::Error {
        if e.get_ref().is_some_and(|inner| inner.is::<StoredError>()) {
            let kind = e.kind();
            return match e.into_inner().map(|inner| inner.downcast::<StoredError>()) {
                Some(Ok(stored)) => stored.0,
                _ => std::io::Error::from(kind),
            };
        }
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} is not valid {}: {e}", self.uri, self.codec),
        )
    }
}

/// The stored bytes of a compressed file, as [`Stored::marked`] reads them.
struct Marked<R>(R);

/// An error reading a compressed file's stored bytes, as it passes through its decoder.
#[derive(Debug)]
struct StoredError(std::io::Error);

impl std::fmt::Display for StoredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for StoredError {}

fn mark(e: std::io::Error) -> std::io::Error {
    std::io::Error::new(e.kind(), StoredError(e))
}

impl<R: BufRead> Read for Marked<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf).map_err(mark)
    }
}

impl<R: BufRead> BufRead for Marked<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.0.fill_buf().map_err(mark)
    }

    fn consume(&mut self, n: usize) {
        self.0.consume(n);
    }
}

/// `r`'s bytes, decompressed with `codec`. Each decoder reads concatenated streams through to the
/// end, as `cat a.gz b.gz` and block-compressing tools (bgzip, pzstd) write them.
fn decoder<'a>(codec: Codec, r: Box<dyn BufRead + Send + 'a>) -> Result<Box<dyn Read + Send + 'a>> {
    Ok(match codec {
        Codec::Gzip => Box::new(flate2::bufread::MultiGzDecoder::new(r)),
        #[cfg(feature = "compression")]
        Codec::Zstd => Box::new(zstd::stream::read::Decoder::with_buffer(r)?),
        #[cfg(feature = "compression")]
        Codec::Bzip2 => Box::new(bzip2::bufread::MultiBzDecoder::new(r)),
        #[cfg(feature = "compression")]
        Codec::Xz => Box::new(liblzma::bufread::XzDecoder::new_multi_decoder(r)),
        #[cfg(not(feature = "compression"))]
        other => return Err(undecodable("this file", other)),
    })
}

/// The refusal for a codec this build does not decode, naming the feature that does.
pub(crate) fn undecodable(what: &str, codec: Codec) -> EngineError {
    EngineError::UnsupportedFormat {
        detail: format!(
            "{what} is {codec}-compressed, and this build decompresses gzip only: {codec} needs \
             the `compression` feature (`cargo install lakeleto --features compression`), which \
             the release binaries and the image have"
        ),
    }
}

/// The message for a read that went past `cap`.
fn too_large(uri: &str, cap: u64) -> String {
    format!(
        "{uri} decompresses to more than {cap} bytes, the most Lakeleto decompresses a file to \
         (`--max-decompressed`)"
    )
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::format::testing::MemObject;

    /// `body` compressed with `codec`.
    pub(crate) fn compress(codec: Codec, body: &[u8]) -> Vec<u8> {
        match codec {
            Codec::Gzip => {
                let mut w = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                w.write_all(body).unwrap();
                w.finish().unwrap()
            }
            #[cfg(feature = "compression")]
            Codec::Zstd => zstd::encode_all(body, 1).unwrap(),
            #[cfg(feature = "compression")]
            Codec::Bzip2 => {
                let mut w = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
                w.write_all(body).unwrap();
                w.finish().unwrap()
            }
            #[cfg(feature = "compression")]
            Codec::Xz => {
                let mut w = liblzma::write::XzEncoder::new(Vec::new(), 1);
                w.write_all(body).unwrap();
                w.finish().unwrap()
            }
            #[cfg(not(feature = "compression"))]
            other => panic!("this build cannot compress {other}"),
        }
    }

    /// The codecs this build decodes.
    fn decodable() -> Vec<Codec> {
        [Codec::Gzip, Codec::Zstd, Codec::Bzip2, Codec::Xz]
            .into_iter()
            .filter(Codec::decodable)
            .collect()
    }

    fn read_all(object: &dyn RemoteObject, range: Option<Range<u64>>) -> Vec<u8> {
        let mut out = Vec::new();
        object.open(range).unwrap().read_to_end(&mut out).unwrap();
        out
    }

    /// Each codec this build decodes reads a file and an object back to their bytes, whole and
    /// by range, and two streams one after the other read as one.
    #[test]
    fn each_codec_reads_back_whole_by_range_and_across_streams() {
        let dir = tempfile::tempdir().unwrap();
        let body: Vec<u8> = (0..200_000u32).flat_map(|i| i.to_le_bytes()).collect();
        for codec in decodable() {
            let path = dir.path().join(format!("t.bin.{codec}"));
            let mut bytes = compress(codec, &body[..400_000]);
            bytes.extend(compress(codec, &body[400_000..]));
            std::fs::write(&path, &bytes).unwrap();
            let file = Decompressed::new(Stored::File(path), codec, u64::MAX).unwrap();
            let object = Decompressed::new(
                Stored::Object(Arc::new(MemObject::new("s3://b/t", "v1", bytes.clone()))),
                codec,
                u64::MAX,
            )
            .unwrap();
            for read in [&file as &dyn RemoteObject, &object] {
                assert_eq!(read_all(read, None), body, "{codec}");
                assert_eq!(
                    read_all(read, Some(399_990..400_010)),
                    body[399_990..400_010],
                    "{codec}"
                );
                assert_eq!(
                    read.size(),
                    bytes.len() as u64,
                    "{codec}: the size as stored"
                );
            }
            assert!(object.version().starts_with(&format!("{codec}:")));
        }
    }

    /// A read stops at the cap with the limit's own error; the cap's worth of bytes, and no more,
    /// reads whole.
    #[test]
    fn a_read_past_the_cap_fails_as_too_large() {
        let body = vec![b'x'; 100_000];
        let gz = compress(Codec::Gzip, &body);
        let at = |cap| {
            Decompressed::new(
                Stored::Object(Arc::new(MemObject::new(
                    "s3://b/bomb.csv.gz",
                    "v1",
                    gz.clone(),
                ))),
                Codec::Gzip,
                cap,
            )
            .unwrap()
        };
        let exact = at(100_000);
        assert_eq!(read_all(&exact, None).len(), 100_000);
        assert!(!exact.tripped.load(Ordering::SeqCst));

        let over = at(99_999);
        let mut sink = Vec::new();
        let err = over.open(None).unwrap().read_to_end(&mut sink).unwrap_err();
        assert!(err.to_string().contains("--max-decompressed"), "{err}");
        assert_eq!(sink.len(), 99_999, "nothing past the cap is handed over");
        match over.or_too_large(EngineError::Io(err)) {
            EngineError::TooLarge(msg) => {
                assert!(
                    msg.contains("s3://b/bomb.csv.gz") && msg.contains("99999"),
                    "{msg}"
                )
            }
            other => panic!("{other:?}"),
        }
        // An error that is not the cap's stays what it was.
        assert!(matches!(
            exact.or_too_large(EngineError::Other("x".into())),
            EngineError::Other(_)
        ));
    }

    /// Bytes that are not what the name says fail as such, naming the file; a connection that drops
    /// mid-read fails as the dropped connection it is.
    #[test]
    fn a_corrupt_file_says_so_and_a_dropped_connection_stays_one() {
        let not_gzip = Decompressed::new(
            Stored::Object(Arc::new(MemObject::new(
                "s3://b/t.csv.gz",
                "v1",
                b"id,name\n1,a\n".to_vec(),
            ))),
            Codec::Gzip,
            u64::MAX,
        )
        .unwrap();
        let err = not_gzip
            .open(None)
            .unwrap()
            .read_to_end(&mut Vec::new())
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .starts_with("s3://b/t.csv.gz is not valid gzip: "),
            "{err}"
        );

        let body: Vec<u8> = (0..100_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let gz = compress(Codec::Gzip, &body);
        let dropping = Decompressed::new(
            Stored::Object(Arc::new(
                MemObject::new("s3://b/t.csv.gz", "v1", gz.clone())
                    .failing_after(gz.len() as u64 / 2),
            )),
            Codec::Gzip,
            u64::MAX,
        )
        .unwrap();
        let err = dropping
            .open(None)
            .unwrap()
            .read_to_end(&mut Vec::new())
            .unwrap_err();
        assert_eq!(err.to_string(), "connection reset by peer");
    }

    /// A build without `compression` refuses zstd, bzip2 and xz up front, naming the feature.
    #[cfg(not(feature = "compression"))]
    #[test]
    fn a_build_without_compression_refuses_its_codecs() {
        for codec in [Codec::Zstd, Codec::Bzip2, Codec::Xz] {
            let object = Arc::new(MemObject::new("s3://b/t.csv", "v1", b"x".to_vec()));
            let err = Decompressed::new(Stored::Object(object), codec, u64::MAX).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("--features compression"), "{msg}");
            assert!(msg.contains(codec.as_str()), "{msg}");
        }
    }
}
