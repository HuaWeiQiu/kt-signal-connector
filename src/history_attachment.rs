// SPDX-License-Identifier: AGPL-3.0-only

//! Link-time history attachment download (§4.38 attachv2 step 3): the
//! transit-CDN byte chain the imported pointer faces were stored for.
//!
//! One chain takes a [`crate::store::HistoryAttachmentLocator`] plus the
//! attachment descriptor's plaintext size and walks the official pipeline:
//! assemble the CDN URL (cdnNumber → cdn base, the official
//! `getCheckedCdnUrl`/`cdnUrlObject` semantics), stream the ciphertext body
//! to a temp file under hard byte bounds, verify the ciphertext layer,
//! decrypt (AES-256-CBC, IV prefixed to the body, PKCS7 padding), trim the
//! log-padded plaintext down to the declared size, verify the plaintext
//! hash, and land the plaintext in the governed media directory under the
//! attachment descriptor's stable id — the exact file
//! `messages.attachments.open` already resolves.
//!
//! Protocol anchors (official Signal-Desktop source, AGPL, read-only
//! reference — zero code copied; verified 2026-10-06 against
//! `ts/AttachmentCrypto.node.ts`, `ts/textsecure/WebAPI.preload.ts`,
//! `ts/textsecure/downloadAttachment.preload.ts`,
//! `ts/util/decipherWithAesKey.node.ts`, `ts/util/appendMacStream.node.ts`,
//! `ts/util/trimPadding.node.ts`, `ts/util/Attachment.std.ts`,
//! `config/production.json`):
//!
//! - key split (`splitKeys`): the 64-byte attachment key is AES key
//!   (first 32) + HMAC key (last 32).
//! - body layout: `IV(16) || AES-256-CBC ciphertext || trailing MAC(32)`;
//!   the MAC is HMAC-SHA256(macKey, IV || ciphertext) — the transit
//!   protocol's terminal HMAC (`getMacAndUpdateHmac` + "Bad MAC").
//! - ciphertext integrity face: `digest` (base64) is SHA-256 over the
//!   complete body INCLUDING the trailing MAC — the official pipeline feeds
//!   the raw stream into the digest hash before the MAC is split off and
//!   compares it in `checkIntegrity` type 'encrypted'. (The §4.38 step-3
//!   pre-ruling summarized this layer as "streaming HMAC-SHA256 vs digest";
//!   the official primitive at this layer is the terminal-HMAC gate plus the
//!   whole-body SHA-256 digest gate — both are verified here, streaming.)
//! - plaintext integrity face: `plaintextHash` (lowercase hex) is SHA-256
//!   over the first `size` bytes of the decrypted plaintext (the hash sits
//!   behind `trimPadding`), checked by `checkIntegrity` type 'plaintext'.
//!   A row carrying only `plaintextHash` is the media-tier face and stays
//!   downloadable on the transit pointer (§4.38 ruling: the mismatch is
//!   terminal because no digest re-check remains possible).
//! - size bounds: `getAttachmentCiphertextSize` (padded plaintext →
//!   `(1 + floor(padded/16))·16` ciphertext bytes, plus IV and MAC) bounds
//!   the body from above; downloads past it fail before the disk grows.
//! - gating: `isDownloadableFromTransitTier` (an integrity face + key +
//!   cdnKey + cdnNumber) decides downloadability; a locator missing any
//!   piece is the honest INVALID_REQUEST hole, never a guessed download.
//!
//! Bounded discipline (ADR 0004): the ciphertext streams to disk in chunks
//! (resident memory is one chunk plus the 32-byte MAC holdback, never the
//! body), the decrypt pass re-reads the temp file block-by-block with a
//! one-block carry, and every temp file is unlinked on failure. A failure at
//! any stage is terminal for the call — the connector never auto-retries a
//! download (§4.38 honest-degradation ruling; the official tap-to-retry face
//! is a human action, a fresh call).

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

use aes::Aes256;
use cbc::cipher::generic_array::GenericArray;
use cbc::cipher::{BlockDecryptMut, KeyIvInit};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::store::HistoryAttachmentLocator;

/// Official crypto constants (`ts/types/Crypto.std.ts`): IV_LENGTH 16,
/// AES_KEY_LENGTH 32, KEY_SET_LENGTH 64, ATTACHMENT_MAC_LENGTH 32 (also
/// `appendMacStream.node.ts` MAC_KEY_SIZE/MAC_SIZE = 32). The plaintext hash
/// and the digest are both SHA-256 (32 bytes).
pub const IV_LENGTH: usize = 16;
pub const AES_KEY_LENGTH: usize = 32;
pub const KEY_SET_LENGTH: usize = 64;
pub const MAC_LENGTH: usize = 32;
pub const SHA256_LENGTH: usize = 32;

/// Minimum ciphertext body: IV + one AES block + trailing MAC.
pub const MIN_BODY_BYTES: u64 = (IV_LENGTH + 16 + MAC_LENGTH) as u64;

/// Hard plaintext ceiling (ADR 0004): the server's own 100 MiB
/// per-attachment cap — a locator claiming more is refused before any
/// network traffic.
pub const MAX_PLAINTEXT_BYTES: u64 = 100 * 1024 * 1024;

/// Network bounds for one CDN GET: a bounded connect so a black-holed CDN
/// cannot pin a send-lane slot, and a generous total so a legitimate large
/// attachment on a slow link still lands.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const TOTAL_TIMEOUT: Duration = Duration::from_secs(120);

/// Streaming chunk for the download and the decrypt pass — resident memory
/// per chain is one chunk, never the body (ADR 0004).
const CHUNK_BYTES: usize = 64 * 1024;

type HmacSha256 = Hmac<Sha256>;
type CbcDecryptor = cbc::Decryptor<Aes256>;

/// Why a download chain failed. Content-free by construction — no URLs,
/// paths, ids, or key material — so the supervisor maps every variant onto
/// the wire codes directly.
#[derive(Debug, Eq, PartialEq)]
pub enum DownloadFailure {
    /// The pointer face cannot name a downloadable attachment (missing or
    /// malformed key/cdn/integrity/size fields). Caller-facing:
    /// INVALID_REQUEST, before any network traffic.
    InvalidLocator(&'static str),
    /// The CDN answered a non-200 status. Terminal (a 404 is the transit
    /// object's expiry — official `AttachmentUndownloadableFromTransitTier`).
    Http(u16),
    /// The transport failed (connect, reset, timeout, body read). Terminal.
    Network,
    /// The body violated its size bounds. Terminal.
    BodyTooLarge { expected_max: u64 },
    /// The trailing MAC did not match HMAC-SHA256(macKey, IV||ciphertext).
    BadMac,
    /// The whole-body SHA-256 did not match the locator's `digest` face.
    DigestMismatch,
    /// AES-CBC padding was malformed. Terminal (corrupt or hostile body).
    BadPadding,
    /// The decrypted plaintext is shorter than the declared size.
    PlaintextTooShort { declared: u64 },
    /// The plaintext SHA-256 did not match the locator's `plaintextHash`.
    PlaintextHashMismatch,
    /// Local filesystem work failed (temp write, rename). The established
    /// local-IO answer: UPSTREAM_ERROR with the advisory retryable flag.
    Io,
}

/// The official production CDN base map (`config/production.json`: cdn
/// `{"0": "https://cdn.signal.org", "2": "https://cdn2.signal.org",
/// "3": "https://cdn3.signal.org"}`) plus the official unknown-number
/// fallback (`getCheckedCdnUrl`: `cdnUrlObject[cdnNumber] ??
/// cdnUrlObject['0']`).
pub fn cdn_base(cdn_number: u32) -> &'static str {
    match cdn_number {
        2 => "https://cdn2.signal.org",
        3 => "https://cdn3.signal.org",
        _ => "https://cdn.signal.org",
    }
}

/// `encodeURIComponent` parity for the CDN key path segment: everything
/// outside the official unreserved set (`A-Za-z0-9-_.!~*'()`) is
/// percent-encoded, so a hostile cdnKey cannot change the path shape.
fn percent_encode_cdn_key(cdn_key: &str) -> String {
    let mut encoded = String::with_capacity(cdn_key.len());
    for byte in cdn_key.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => encoded.push(byte as char),
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

/// The transit GET URL for one locator face (official `getAttachment`:
/// `cdnPath = /attachments/${encodeURIComponent(cdnKey)}`,
/// `cdnNumber ?? 0`). `base_override` replaces the base entirely — the test
/// seam that aims the chain at a local mock CDN (the official desktop
/// carries the same seam as its window config values).
pub fn cdn_attachment_url(
    cdn_number: Option<u32>,
    cdn_key: &str,
    base_override: Option<&str>,
) -> String {
    if let Some(base) = base_override {
        return format!(
            "{}/attachments/{}",
            base.trim_end_matches('/'),
            percent_encode_cdn_key(cdn_key)
        );
    }
    format!(
        "{}/attachments/{}",
        cdn_base(cdn_number.unwrap_or(0)),
        percent_encode_cdn_key(cdn_key)
    )
}

/// The validated, decoded download inputs for one locator face: exactly what
/// the official gate `isDownloadableFromTransitTier` requires (an integrity
/// face + key + cdnKey + cdnNumber) plus the plaintext size the descriptor
/// carries. Built fail-closed — every malformed field is an
/// [`DownloadFailure::InvalidLocator`], never a guess.
#[derive(Clone)]
pub struct TransitFace {
    pub aes_key: [u8; AES_KEY_LENGTH],
    pub mac_key: [u8; AES_KEY_LENGTH],
    pub url: String,
    /// Whole-body SHA-256 expectation (the `digest` face, standard base64).
    pub digest: Option<[u8; SHA256_LENGTH]>,
    /// Trimmed-plaintext SHA-256 expectation (the `plaintextHash` face,
    /// lowercase hex).
    pub plaintext_hash: Option<[u8; SHA256_LENGTH]>,
    pub plaintext_size: u64,
}

impl TransitFace {
    /// Decode and validate one stored locator face plus its descriptor's
    /// plaintext size. `cdn_base_override` is the test seam (see
    /// [`cdn_attachment_url`]).
    pub fn build(
        locator: &HistoryAttachmentLocator,
        plaintext_size: Option<u64>,
        cdn_base_override: Option<&str>,
    ) -> Result<Self, DownloadFailure> {
        let Some(key) = locator.key.as_deref() else {
            return Err(DownloadFailure::InvalidLocator(
                "attachment has no attachment key on its pointer face",
            ));
        };
        let Some(cdn_key) = locator.cdn_key.as_deref() else {
            return Err(DownloadFailure::InvalidLocator(
                "attachment has no transit CDN key on its pointer face",
            ));
        };
        let Some(cdn_number) = locator.cdn_number else {
            return Err(DownloadFailure::InvalidLocator(
                "attachment has no transit CDN number on its pointer face",
            ));
        };
        // The integrity face is one-of digest/plaintextHash, and at least
        // one must ride (official `hasIntegrityCheck`); both faces verify
        // when both ride (§4.38 two-layer ruling).
        let digest = match locator.digest.as_deref() {
            Some(text) => {
                let mut decoded = Vec::new();
                crate::service::base64_decode_to_vec(text, &mut decoded).map_err(|_| {
                    DownloadFailure::InvalidLocator("attachment digest is not valid base64")
                })?;
                Some(decoded.try_into().map_err(|_: Vec<u8>| {
                    DownloadFailure::InvalidLocator("attachment digest is not a SHA-256 value")
                })?)
            }
            None => None,
        };
        let plaintext_hash = match locator.plaintext_hash.as_deref() {
            Some(text) => {
                let mut hash = [0_u8; SHA256_LENGTH];
                if hex::decode_to_slice(text, &mut hash).is_err() {
                    return Err(DownloadFailure::InvalidLocator(
                        "attachment plaintextHash is not a SHA-256 hex value",
                    ));
                }
                Some(hash)
            }
            None => None,
        };
        if digest.is_none() && plaintext_hash.is_none() {
            return Err(DownloadFailure::InvalidLocator(
                "attachment has no integrity face on its pointer face",
            ));
        }
        let Some(plaintext_size) = plaintext_size else {
            return Err(DownloadFailure::InvalidLocator(
                "attachment has no plaintext size on its descriptor",
            ));
        };
        if plaintext_size == 0 || plaintext_size > MAX_PLAINTEXT_BYTES {
            return Err(DownloadFailure::InvalidLocator(
                "attachment size is outside the downloadable range",
            ));
        }
        let mut decoded = Vec::new();
        crate::service::base64_decode_to_vec(key, &mut decoded)
            .map_err(|_| DownloadFailure::InvalidLocator("attachment key is not valid base64"))?;
        let key_set: [u8; KEY_SET_LENGTH] = decoded.try_into().map_err(|_: Vec<u8>| {
            DownloadFailure::InvalidLocator("attachment key is not a 64-byte key set")
        })?;
        let mut aes_key = [0_u8; AES_KEY_LENGTH];
        let mut mac_key = [0_u8; AES_KEY_LENGTH];
        aes_key.copy_from_slice(&key_set[..AES_KEY_LENGTH]);
        mac_key.copy_from_slice(&key_set[AES_KEY_LENGTH..]);
        Ok(Self {
            aes_key,
            mac_key,
            url: cdn_attachment_url(Some(cdn_number), cdn_key, cdn_base_override),
            digest,
            plaintext_hash,
            plaintext_size,
        })
    }

    /// The body-length ceiling derived from the declared plaintext size: the
    /// official `getAttachmentCiphertextSize` padded-plaintext form (the
    /// largest legitimate encoding). The floor is [`MIN_BODY_BYTES`].
    pub fn body_max_bytes(&self) -> u64 {
        let padded = self.plaintext_size.next_power_of_two();
        IV_LENGTH as u64 + (1 + padded / 16) * 16 + MAC_LENGTH as u64
    }
}

/// Constant-time byte equality (the official `constantTimeEqual` posture).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

/// The streaming sink for one ciphertext body: raw bytes to a temp file, the
/// whole-body SHA-256 (the `digest` gate's input), and the terminal HMAC
/// over everything except the trailing 32 bytes (held back until the stream
/// ends — the official `getMacAndUpdateHmac` holdback). Resident memory: one
/// chunk plus the holdback.
pub struct BodySink {
    file: BufWriter<File>,
    digest: Sha256,
    hmac: HmacSha256,
    holdback: [u8; MAC_LENGTH],
    holdback_len: usize,
    total: u64,
    max: u64,
}

impl BodySink {
    pub fn create(path: &Path, max: u64) -> Result<Self, DownloadFailure> {
        let file = File::create(path).map_err(|_| DownloadFailure::Io)?;
        Ok(Self {
            file: BufWriter::new(file),
            digest: Sha256::new(),
            hmac: HmacSha256::new_from_slice(&[0_u8; AES_KEY_LENGTH])
                .expect("hmac accepts any key length"),
            holdback: [0_u8; MAC_LENGTH],
            holdback_len: 0,
            total: 0,
            max,
        })
    }

    pub fn set_mac_key(&mut self, mac_key: &[u8; AES_KEY_LENGTH]) {
        self.hmac = HmacSha256::new_from_slice(mac_key).expect("hmac accepts any key length");
    }

    /// Consume one streamed chunk. Past-cap data fails the whole sink
    /// (bounded disk, bounded pass) — the official `checkSize` posture,
    /// without its resume slack (this chain never resumes).
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), DownloadFailure> {
        if self.total + chunk.len() as u64 > self.max {
            return Err(DownloadFailure::BodyTooLarge {
                expected_max: self.max,
            });
        }
        let mut rest = chunk;
        // Fill a partial holdback first.
        if self.holdback_len < MAC_LENGTH {
            let take = (MAC_LENGTH - self.holdback_len).min(rest.len());
            self.holdback[self.holdback_len..self.holdback_len + take]
                .copy_from_slice(&rest[..take]);
            self.holdback_len += take;
            rest = &rest[take..];
        }
        // The combined stream is holdback(32) ++ rest; its final 32 bytes
        // stay held, everything before them is HMAC input.
        if !rest.is_empty() {
            if rest.len() >= MAC_LENGTH {
                self.hmac.update(&self.holdback);
                let (fed, next) = rest.split_at(rest.len() - MAC_LENGTH);
                self.hmac.update(fed);
                self.holdback.copy_from_slice(next);
            } else {
                // The oldest `rest.len()` held bytes leave the holdback and
                // become HMAC input; the holdback slides forward over them.
                let feed = rest.len();
                self.hmac.update(&self.holdback[..feed]);
                self.holdback.copy_within(feed.., 0);
                self.holdback[MAC_LENGTH - feed..].copy_from_slice(rest);
            }
        }
        self.digest.update(chunk);
        self.file
            .write_all(chunk)
            .map_err(|_| DownloadFailure::Io)?;
        self.total += chunk.len() as u64;
        Ok(())
    }

    /// Close the body: the holdback is the trailing MAC. Flushes but keeps
    /// the file on disk for the decrypt pass (the caller owns cleanup).
    pub fn finish(mut self) -> Result<FinishedBody, DownloadFailure> {
        self.file.flush().map_err(|_| DownloadFailure::Io)?;
        if self.total < MIN_BODY_BYTES {
            return Err(DownloadFailure::BadMac);
        }
        let mac = self.holdback;
        let mac_check: [u8; MAC_LENGTH] = self
            .hmac
            .finalize()
            .into_bytes()
            .as_slice()
            .try_into()
            .expect("hmac-sha256 output is 32 bytes");
        Ok(FinishedBody {
            digest: self
                .digest
                .finalize()
                .as_slice()
                .try_into()
                .expect("sha256 output is 32 bytes"),
            mac,
            mac_check,
        })
    }
}

/// One completed ciphertext body: its size, its whole-body SHA-256 (the
/// `digest` gate's computed value), the trailing MAC the body declared, and
/// the HMAC the body actually carried.
#[derive(Debug)]
pub struct FinishedBody {
    pub digest: [u8; SHA256_LENGTH],
    pub mac: [u8; MAC_LENGTH],
    pub mac_check: [u8; MAC_LENGTH],
}

impl FinishedBody {
    /// The ciphertext-layer gates: terminal HMAC first (the transport's own
    /// integrity), then the whole-body digest when the locator carries the
    /// `digest` face. Failure is terminal — no re-fetch, no fallback (§4.38).
    pub fn verify_ciphertext(&self, face: &TransitFace) -> Result<(), DownloadFailure> {
        if !constant_time_eq(&self.mac_check, &self.mac) {
            return Err(DownloadFailure::BadMac);
        }
        if let Some(expected) = face.digest
            && !constant_time_eq(&self.digest, &expected)
        {
            return Err(DownloadFailure::DigestMismatch);
        }
        Ok(())
    }
}

/// The decrypt pass: re-read the temp ciphertext (the IV-stripped,
/// MAC-stripped region — the reader is bounded to `IV + ciphertext_len`, so
/// the trailing MAC bytes are never consumed), stream AES-256-CBC blocks
/// through a one-block carry, validate PKCS7 on the final block, trim the
/// log-padded plaintext to the declared size, hash the kept bytes, and land
/// them at `plaintext_path`. Returns the kept plaintext size. Resident
/// memory: one chunk plus one block.
pub fn decrypt_to_file(
    ciphertext_path: &Path,
    plaintext_path: &Path,
    face: &TransitFace,
) -> Result<u64, DownloadFailure> {
    let mut reader = File::open(ciphertext_path).map_err(|_| DownloadFailure::Io)?;
    let total = reader.metadata().map_err(|_| DownloadFailure::Io)?.len();
    if total < MIN_BODY_BYTES || !(total - IV_LENGTH as u64 - MAC_LENGTH as u64).is_multiple_of(16)
    {
        return Err(DownloadFailure::BadPadding);
    }
    let ciphertext_len = total - IV_LENGTH as u64 - MAC_LENGTH as u64;
    let mut iv = [0_u8; IV_LENGTH];
    reader
        .read_exact(&mut iv)
        .map_err(|_| DownloadFailure::Io)?;
    reader
        .seek(SeekFrom::Start(IV_LENGTH as u64))
        .map_err(|_| DownloadFailure::Io)?;

    let mut decipher = CbcDecryptor::new((&face.aes_key).into(), (&iv).into());
    let mut hasher = Sha256::new();
    let mut writer = BufWriter::new(File::create(plaintext_path).map_err(|_| DownloadFailure::Io)?);
    let size = face.plaintext_size;
    let mut kept: u64 = 0;

    // Everything except the final block streams straight through; the final
    // block is held for the PKCS7 check. The read is bounded to the
    // ciphertext region: the trailing MAC bytes past IV + ciphertext_len
    // are never consumed.
    let mut carry: Vec<u8> = Vec::with_capacity(CHUNK_BYTES + 16);
    let mut buf = [0_u8; CHUNK_BYTES];
    let mut left = ciphertext_len;
    loop {
        let want = left.min(buf.len() as u64) as usize;
        if want == 0 {
            break;
        }
        let read = reader
            .read(&mut buf[..want])
            .map_err(|_| DownloadFailure::Io)?;
        if read == 0 {
            break;
        }
        left -= read as u64;
        carry.extend_from_slice(&buf[..read]);
        let complete = carry.len() / 16;
        // Decrypt every complete block except one held back for the end.
        let decryptable = complete.saturating_sub(1).min(complete);
        if decryptable > 0 {
            let byte_count = decryptable * 16;
            let (blocks, _) = carry.split_at_mut(byte_count);
            for block in blocks.as_chunks_mut::<16>().0 {
                decipher.decrypt_block_mut(GenericArray::from_mut_slice(block));
            }
            emit(&mut writer, &mut hasher, &mut kept, size, blocks)?;
            carry.drain(..byte_count);
        }
    }
    // The held-back final block: validate PKCS7, keep the unpadded tail.
    if carry.len() != 16 {
        return Err(DownloadFailure::BadPadding);
    }
    decipher.decrypt_block_mut(GenericArray::from_mut_slice(&mut carry));
    let pad = carry[15] as usize;
    if pad == 0 || pad > 16 || carry[16 - pad..].iter().any(|byte| *byte as usize != pad) {
        return Err(DownloadFailure::BadPadding);
    }
    emit(
        &mut writer,
        &mut hasher,
        &mut kept,
        size,
        &carry[..16 - pad],
    )?;
    writer.flush().map_err(|_| DownloadFailure::Io)?;
    if kept < size {
        return Err(DownloadFailure::PlaintextTooShort { declared: size });
    }
    if let Some(expected) = face.plaintext_hash {
        let computed: [u8; SHA256_LENGTH] = hasher
            .finalize()
            .as_slice()
            .try_into()
            .expect("sha256 output is 32 bytes");
        if !constant_time_eq(&computed, &expected) {
            return Err(DownloadFailure::PlaintextHashMismatch);
        }
    }
    Ok(kept)
}

/// Emit one decrypted stretch the official `trimPadding` way: exactly the
/// first `size` bytes are written and hashed — log-pad filler and anything
/// past the declared plaintext never reaches the output file.
fn emit(
    writer: &mut BufWriter<File>,
    hasher: &mut Sha256,
    kept: &mut u64,
    size: u64,
    bytes: &[u8],
) -> Result<(), DownloadFailure> {
    if *kept >= size {
        return Ok(());
    }
    let within = (size - *kept).min(bytes.len() as u64) as usize;
    writer
        .write_all(&bytes[..within])
        .map_err(|_| DownloadFailure::Io)?;
    hasher.update(&bytes[..within]);
    *kept += within as u64;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cbc::cipher::BlockEncryptMut;
    use cbc::cipher::block_padding::Pkcs7;

    const KEY: [u8; KEY_SET_LENGTH] = [0x11; KEY_SET_LENGTH];

    /// Official-shape fixture: log-pad, PKCS7, AES-256-CBC, prefix IV,
    /// append HMAC — returning (body, plaintext-expecting-hash).
    fn sealed(plaintext: &[u8]) -> (Vec<u8>, [u8; SHA256_LENGTH]) {
        let padded_size = plaintext.len().next_power_of_two();
        let mut padded = vec![0_u8; padded_size];
        padded[..plaintext.len()].copy_from_slice(plaintext);
        let iv = [0x42_u8; IV_LENGTH];
        // PKCS7 in place over a buffer sized for the worst case (log-pad
        // length + one padding block).
        padded.resize(padded_size + 16, 16_u8);
        let ciphertext = cbc::Encryptor::<Aes256>::new((&KEY[..32]).into(), (&iv).into())
            .encrypt_padded_mut::<Pkcs7>(&mut padded, padded_size)
            .unwrap()
            .to_vec();
        let mut body = iv.to_vec();
        body.extend_from_slice(&ciphertext);
        let mut mac = HmacSha256::new_from_slice(&KEY[32..]).unwrap();
        mac.update(&body);
        let mac = mac.finalize().into_bytes();
        body.extend_from_slice(&mac);
        let mut hash = Sha256::new();
        hash.update(plaintext);
        let hash: [u8; SHA256_LENGTH] = hash.finalize().as_slice().try_into().unwrap();
        (body, hash)
    }

    fn make_face(
        plaintext_size: u64,
        digest: Option<[u8; 32]>,
        hash: Option<[u8; 32]>,
    ) -> TransitFace {
        TransitFace {
            aes_key: KEY[..32].try_into().unwrap(),
            mac_key: KEY[32..].try_into().unwrap(),
            url: String::new(),
            digest,
            plaintext_hash: hash,
            plaintext_size,
        }
    }

    #[test]
    fn cdn_urls_follow_the_official_map_and_fallback() {
        assert_eq!(cdn_base(0), "https://cdn.signal.org");
        assert_eq!(cdn_base(2), "https://cdn2.signal.org");
        assert_eq!(cdn_base(3), "https://cdn3.signal.org");
        // Unknown numbers fall back to CDN 0 (official `getCheckedCdnUrl`).
        assert_eq!(cdn_base(1), "https://cdn.signal.org");
        assert_eq!(cdn_base(9), "https://cdn.signal.org");
        assert_eq!(
            cdn_attachment_url(Some(3), "abc/123==", None),
            "https://cdn3.signal.org/attachments/abc%2F123%3D%3D"
        );
        // Absent cdnNumber is the official `?? 0` branch; the official
        // unreserved set passes through unescaped.
        assert_eq!(
            cdn_attachment_url(None, "key-1_x.~*()'!", None),
            "https://cdn.signal.org/attachments/key-1_x.~*()'!"
        );
        // The test seam replaces the whole base and survives a trailing slash.
        assert_eq!(
            cdn_attachment_url(Some(2), "k", Some("http://127.0.0.1:1/")),
            "http://127.0.0.1:1/attachments/k"
        );
    }

    #[test]
    fn body_bounds_cover_both_encodings() {
        // 100 bytes of plaintext: the padded form (128) needs
        // 16 + (1+8)*16 + 32 = 192; the legacy unpadded form
        // (ceil(101/16)=7 blocks = 112 + 48) fits inside.
        let face = make_face(100, None, None);
        assert_eq!(face.body_max_bytes(), 16 + 9 * 16 + 32);
        // Block-aligned plaintext still reserves the PKCS7 padding block.
        assert_eq!(make_face(16, None, None).body_max_bytes(), 16 + 2 * 16 + 32);
    }

    #[test]
    fn constant_time_eq_is_exact() {
        assert!(constant_time_eq(b"abcd", b"abcd"));
        assert!(!constant_time_eq(b"abcd", b"abce"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn body_sink_holdback_survives_every_chunking() {
        let temp = tempfile::tempdir().unwrap();
        let body = vec![9_u8; 300];
        let expected_mac: [u8; 32] = {
            let mut mac = HmacSha256::new_from_slice(&[7_u8; 32]).unwrap();
            mac.update(&body[..body.len() - 32]);
            mac.finalize().into_bytes().as_slice().try_into().unwrap()
        };
        let expected_digest: [u8; 32] = {
            let mut hash = Sha256::new();
            hash.update(&body);
            hash.finalize().as_slice().try_into().unwrap()
        };
        // Every chunk shape must land the same MAC: exactly 32, sub-32,
        // larger-than-32, and one-shot.
        for chunk_size in [1_usize, 16, 32, 33, 300] {
            let path = temp.path().join(format!("body-{chunk_size}"));
            let mut sink = BodySink::create(&path, 1024).unwrap();
            sink.set_mac_key(&[7_u8; 32]);
            for chunk in body.chunks(chunk_size) {
                sink.push(chunk).unwrap();
            }
            let finished = sink.finish().unwrap();
            // The synthetic body is uniform filler, so its trailing bytes
            // are not the correct MAC — the assertion is that the sink's
            // HMAC over everything before the holdback equals the expected
            // HMAC (which a sealed body would have carried in those bytes),
            // and that the whole-body digest matches.
            assert_eq!(finished.mac_check, expected_mac, "chunks of {chunk_size}");
            assert_eq!(finished.digest, expected_digest, "chunks of {chunk_size}");
        }
    }

    #[test]
    fn body_sink_refuses_bodies_past_the_cap() {
        let temp = tempfile::tempdir().unwrap();
        let mut sink = BodySink::create(&temp.path().join("body"), 64).unwrap();
        sink.set_mac_key(&[7_u8; 32]);
        assert_eq!(
            sink.push(&[0_u8; 65]),
            Err(DownloadFailure::BodyTooLarge { expected_max: 64 })
        );
        // An under-sized body is a bad-MAC answer (there is no separable
        // trailing MAC to trust).
        let small = BodySink::create(&temp.path().join("small"), 1024).unwrap();
        small
            .finish()
            .expect_err("an empty body cannot carry a MAC");
    }

    #[test]
    fn finished_body_gates_mac_then_digest() {
        let face = make_face(32, Some([1; 32]), None);
        let mut finished = FinishedBody {
            digest: [1; 32],
            mac: [2; 32],
            mac_check: [2; 32],
        };
        assert_eq!(finished.verify_ciphertext(&face), Ok(()));
        // MAC first: a wrong HMAC fails even with a matching digest.
        finished.mac_check = [3; 32];
        assert_eq!(
            finished.verify_ciphertext(&face),
            Err(DownloadFailure::BadMac)
        );
        finished.mac_check = [2; 32];
        finished.digest = [9; 32];
        assert_eq!(
            finished.verify_ciphertext(&face),
            Err(DownloadFailure::DigestMismatch)
        );
    }

    #[test]
    fn decrypt_round_trips_padded_and_trimmed_plaintext() {
        let temp = tempfile::tempdir().unwrap();
        let plaintext: Vec<u8> = (0..100_u8).collect();
        let (body, hash) = sealed(&plaintext);
        let ct_path = temp.path().join("body");
        std::fs::write(&ct_path, &body).unwrap();

        // Both integrity faces ride: digest = whole-body SHA-256 (MAC
        // included), plaintextHash = trimmed-plaintext SHA-256.
        let mut digest = Sha256::new();
        digest.update(&body);
        let digest: [u8; SHA256_LENGTH] = digest.finalize().as_slice().try_into().unwrap();
        let out_path = temp.path().join("plain");
        let kept = decrypt_to_file(
            &ct_path,
            &out_path,
            &make_face(100, Some(digest), Some(hash)),
        )
        .unwrap();
        assert_eq!(kept, 100);
        assert_eq!(std::fs::read(&out_path).unwrap(), plaintext);
    }

    #[test]
    fn decrypt_rejects_corrupt_padding_and_short_plaintext() {
        let temp = tempfile::tempdir().unwrap();
        // A single zero block seals to one block of valid 0x10 padding: the
        // decrypted plaintext (0 bytes) is short against a declared size.
        let iv = [0_u8; IV_LENGTH];
        // PKCS7-encrypt an empty message: exactly one full padding block.
        let mut empty = [16_u8; 16];
        let encrypted = cbc::Encryptor::<Aes256>::new((&KEY[..32]).into(), (&iv).into())
            .encrypt_padded_mut::<Pkcs7>(&mut empty, 0)
            .unwrap()
            .to_vec();
        let mut body = iv.to_vec();
        body.extend_from_slice(&encrypted);
        body.extend_from_slice(&[0_u8; 32]);
        let ct_path = temp.path().join("body");
        std::fs::write(&ct_path, &body).unwrap();
        assert_eq!(
            decrypt_to_file(
                &ct_path,
                &temp.path().join("out"),
                &make_face(32, None, None)
            ),
            Err(DownloadFailure::PlaintextTooShort { declared: 32 })
        );
        // Corrupt the padding byte: the final block no longer unpads.
        let last = body.len() - 32 - 1;
        body[last] ^= 0xFF;
        std::fs::write(&ct_path, &body).unwrap();
        assert_eq!(
            decrypt_to_file(
                &ct_path,
                &temp.path().join("out2"),
                &make_face(32, None, None)
            ),
            Err(DownloadFailure::BadPadding)
        );
    }

    #[test]
    fn a_wrong_plaintext_hash_is_the_terminal_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let (body, _hash) = sealed(b"the real plaintext bytes here!!");
        let ct_path = temp.path().join("body");
        std::fs::write(&ct_path, &body).unwrap();
        let wrong = [0xEE_u8; 32];
        assert_eq!(
            decrypt_to_file(
                &ct_path,
                &temp.path().join("out"),
                &make_face(30, None, Some(wrong))
            ),
            Err(DownloadFailure::PlaintextHashMismatch)
        );
    }
}
