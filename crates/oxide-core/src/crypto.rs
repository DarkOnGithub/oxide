use crate::{OxideError, Result};
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng as AeadOsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};
use rand::{RngCore, rngs::OsRng};

// Security constants
pub const SALT_SIZE: usize = 16;
pub const KEY_SIZE: usize = 32; // 32 bytes = 256 bits (For AES-256)
pub type EncryptionKey = [u8; KEY_SIZE];

#[derive(Debug, Clone, Copy)]
pub struct EncryptionContext {
    pub salt: [u8; SALT_SIZE],
    pub key: EncryptionKey,
}

impl EncryptionContext {
    pub fn from_password(password: &str) -> Result<Self> {
        let salt = generate_salt();
        let key = derive_key(password, &salt)?;
        Ok(Self { salt, key })
    }

    pub fn from_password_and_salt(password: &str, salt: &[u8; SALT_SIZE]) -> Result<Self> {
        let key = derive_key(password, salt)?;
        Ok(Self { salt: *salt, key })
    }
}

/// Generates a cryptographically secure salt using the OS random number generator.
pub fn generate_salt() -> [u8; SALT_SIZE] {
    let mut salt = [0u8; SALT_SIZE];
    OsRng.fill_bytes(&mut salt);
    salt
}

/// Derives a robust AES-256 key from a human-readable password using Argon2id.
pub fn derive_key(password: &str, salt: &[u8; SALT_SIZE]) -> Result<EncryptionKey> {
    // Argon2id configuration (OWASP / ANSSI recommendations):
    // - m_cost: 65536 KB (64 MB of RAM required to mitigate GPU attacks)
    // - t_cost: 3 iterations (good balance between time and security)
    // - p_cost: 4 threads (parallelization)
    let params = Params::new(65536, 3, 4, Some(KEY_SIZE))
        .map_err(|_| OxideError::InvalidFormat("failed to init Argon2 parameters"))?;

    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let mut key = [0u8; KEY_SIZE];

    // Hash the password + salt directly into our 'key' array
    argon2
        .hash_password_into(password.as_bytes(), salt, &mut key)
        .map_err(|_| OxideError::InvalidFormat("failed to derive cryptographic key"))?;

    Ok(key)
}

// Size of the Nonce for AES-GCM (96 bits)
pub const NONCE_SIZE: usize = 12;

/// Encrypts a block of data using AES-256-GCM.
/// Returns a vector containing: [Nonce (12 bytes)] + [Ciphertext] + [16-byte MAC Tag].
pub fn encrypt_block(key: &[u8; KEY_SIZE], plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(key.into());

    // Generate a unique random 96-bit nonce for this specific block
    let nonce = Aes256Gcm::generate_nonce(&mut AeadOsRng);

    // Encrypt the data. The 'encrypt' method automatically computes and appends the 16-byte MAC tag.
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| OxideError::InvalidFormat("encryption failed"))?;

    // Prepend the nonce to the ciphertext so the reader can extract it later
    let mut final_payload = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    final_payload.extend_from_slice(&nonce);
    final_payload.extend_from_slice(&ciphertext);

    Ok(final_payload)
}

/// Decrypts a block of data using AES-256-GCM.
/// Expects the input payload to be formatted as: [Nonce (12 bytes)] + [Ciphertext + MAC Tag].
pub fn decrypt_block(key: &[u8; KEY_SIZE], payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() < NONCE_SIZE {
        return Err(OxideError::InvalidFormat(
            "encrypted block is too short to contain a nonce",
        ));
    }

    let cipher = Aes256Gcm::new(key.into());

    // Split the payload to extract the 12-byte nonce and the actual ciphertext
    let (nonce_bytes, ciphertext) = payload.split_at(NONCE_SIZE);
    let nonce = Nonce::from_slice(nonce_bytes);

    // Decrypt the data. This automatically verifies the 16-byte MAC tag to ensure integrity.
    let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| {
        OxideError::InvalidFormat("decryption failed or data is corrupted (invalid password?)")
    })?;

    Ok(plaintext)
}
