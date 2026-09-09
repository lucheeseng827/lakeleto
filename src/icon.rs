//! Writing the Strata mark out as `.png` and `.ico`, with no image dependency.
//!
//! The installers need real icon files: Windows wants a multi-size `.ico` for the
//! Start Menu shortcut and Add/Remove Programs, macOS wants a folder of PNGs that
//! `iconutil` folds into an `.icns`. Both come from [`crate::desktop::strata_icon_at`],
//! so the art has exactly one definition — the same one the tray icon draws.
//!
//! The encoder is deliberately dumb. PNG's container is a handful of length-prefixed,
//! CRC'd chunks, and its only mandatory compression is zlib — which permits *stored*
//! (uncompressed) deflate blocks. Taking that option costs a few KB per icon and
//! saves a compression dependency in a crate that ships as a lean static binary.
//! Icons are tens of kilobytes; nobody will notice, and every PNG decoder must
//! accept stored blocks.

/// A PNG, RGBA8, no compression.
pub fn png(rgba: &[u8], width: u32, height: u32) -> Vec<u8> {
    assert_eq!(
        rgba.len(),
        (width * height * 4) as usize,
        "pixel buffer does not match {width}x{height}"
    );

    let mut out = Vec::new();
    out.extend_from_slice(&[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]);

    // IHDR: 8-bit RGBA, no interlace.
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.extend_from_slice(&[8, 6, 0, 0, 0]);
    chunk(&mut out, b"IHDR", &ihdr);

    // Raw scanlines, each prefixed with filter type 0 (None).
    let mut raw = Vec::with_capacity((height * (1 + width * 4)) as usize);
    for row in rgba.chunks_exact((width * 4) as usize) {
        raw.push(0);
        raw.extend_from_slice(row);
    }
    chunk(&mut out, b"IDAT", &zlib_stored(&raw));
    chunk(&mut out, b"IEND", &[]);
    out
}

/// A Windows `.ico` holding each image as PNG.
///
/// PNG-compressed entries are the Vista-and-later convention and are what keeps a
/// 256×256 entry from costing 256 KB as a raw DIB. Every supported Windows version
/// reads them.
pub fn ico(images: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let encoded: Vec<(u32, Vec<u8>)> = images
        .iter()
        .map(|(size, rgba)| (*size, png(rgba, *size, *size)))
        .collect();

    let mut out = Vec::new();
    // ICONDIR: reserved, type 1 (icon), image count.
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&(encoded.len() as u16).to_le_bytes());

    // Directory entries come first, so offsets start past all of them.
    let mut offset = 6 + 16 * encoded.len() as u32;
    for (size, data) in &encoded {
        // 256 is encoded as 0 — the field is one byte.
        let dim = if *size >= 256 { 0u8 } else { *size as u8 };
        out.push(dim); // width
        out.push(dim); // height
        out.push(0); // palette size (0 = truecolour)
        out.push(0); // reserved
        out.extend_from_slice(&1u16.to_le_bytes()); // colour planes
        out.extend_from_slice(&32u16.to_le_bytes()); // bits per pixel
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(&offset.to_le_bytes());
        offset += data.len() as u32;
    }
    for (_, data) in &encoded {
        out.extend_from_slice(data);
    }
    out
}

fn chunk(out: &mut Vec<u8>, kind: &[u8; 4], body: &[u8]) {
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(body);
    let mut crc_input = Vec::with_capacity(4 + body.len());
    crc_input.extend_from_slice(kind);
    crc_input.extend_from_slice(body);
    out.extend_from_slice(&crc32(&crc_input).to_be_bytes());
}

/// A zlib stream whose deflate blocks are all *stored*: the escape hatch in the
/// format that lets a writer skip compression entirely.
fn zlib_stored(data: &[u8]) -> Vec<u8> {
    let mut out = vec![0x78, 0x01]; // CMF/FLG: deflate, 32K window, no dict
                                    // A stored block's LEN field is 16 bits, so long inputs need several blocks.
    const MAX: usize = 65_535;
    let mut chunks = data.chunks(MAX).peekable();
    if data.is_empty() {
        out.extend_from_slice(&[0x01, 0x00, 0x00, 0xff, 0xff]);
    }
    while let Some(part) = chunks.next() {
        let final_block = chunks.peek().is_none();
        out.push(u8::from(final_block));
        let len = part.len() as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes()); // one's complement, per spec
        out.extend_from_slice(part);
    }
    out.extend_from_slice(&adler32(data).to_be_bytes());
    out
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            // 0xedb88320 is the reversed CRC-32 polynomial PNG specifies.
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn adler32(data: &[u8]) -> u32 {
    let (mut a, mut b) = (1u32, 0u32);
    for &byte in data {
        a = (a + byte as u32) % 65_521;
        b = (b + a) % 65_521;
    }
    (b << 16) | a
}

#[cfg(test)]
mod tests {
    use super::*;

    fn solid(size: u32) -> Vec<u8> {
        vec![0x40; (size * size * 4) as usize]
    }

    #[test]
    fn a_png_has_the_signature_and_the_required_chunks() {
        let out = png(&solid(8), 8, 8);
        assert_eq!(&out[..8], &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]);
        let body = String::from_utf8_lossy(&out);
        for kind in ["IHDR", "IDAT", "IEND"] {
            assert!(body.contains(kind), "missing {kind} chunk");
        }
    }

    /// The CRC and Adler checksums are the two things a decoder rejects outright,
    /// and a wrong one is invisible until an OS refuses the icon. Pin them against
    /// values computed independently of this implementation.
    #[test]
    fn the_checksums_match_known_vectors() {
        // Standard CRC-32/ADLER-32 check values for "123456789".
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(adler32(b"123456789"), 0x091e_01de);
    }

    /// A stored deflate block carries LEN and its one's complement; a decoder
    /// treats a mismatch as corruption.
    #[test]
    fn stored_blocks_carry_a_valid_length_pair() {
        let out = zlib_stored(b"hello");
        assert_eq!(&out[..2], &[0x78, 0x01]);
        assert_eq!(out[2], 1, "single block should be marked final");
        let len = u16::from_le_bytes([out[3], out[4]]);
        let nlen = u16::from_le_bytes([out[5], out[6]]);
        assert_eq!(len, 5);
        assert_eq!(nlen, !len);
    }

    /// Inputs above 65535 bytes must split, or LEN silently truncates. A 256×256
    /// icon is ~263 KB of scanlines, so this is the real case, not a corner one.
    #[test]
    fn oversized_input_splits_into_several_blocks() {
        let data = vec![0u8; 70_000];
        let out = zlib_stored(&data);
        assert_eq!(out[2], 0, "first of two blocks is not final");
        // 2 header + 2 blocks of (5 header + payload) + 4 adler
        assert_eq!(out.len(), 2 + (5 + 65_535) + (5 + 4_465) + 4);
    }

    #[test]
    fn an_ico_directory_points_at_its_images() {
        let images = vec![(16u32, solid(16)), (256u32, solid(256))];
        let out = ico(&images);

        assert_eq!(u16::from_le_bytes([out[2], out[3]]), 1, "type = icon");
        assert_eq!(u16::from_le_bytes([out[4], out[5]]), 2, "two entries");
        // 256 must be written as 0 in the one-byte dimension fields.
        assert_eq!(out[6], 16, "first entry is 16px");
        assert_eq!(out[6 + 16], 0, "256px entry encodes its size as 0");

        // Every entry's offset/length must land inside the file, on a PNG header.
        for i in 0..2usize {
            let entry = 6 + 16 * i;
            let len = u32::from_le_bytes(out[entry + 8..entry + 12].try_into().unwrap()) as usize;
            let off = u32::from_le_bytes(out[entry + 12..entry + 16].try_into().unwrap()) as usize;
            assert!(off + len <= out.len(), "entry {i} runs past the end");
            assert_eq!(
                &out[off..off + 4],
                &[0x89, b'P', b'N', b'G'],
                "entry {i} is not a PNG"
            );
        }
    }
}
