use crate::format::oxz::headers::RecoveryMetadata;

/// Bouchon (Stub) pour le moteur Reed-Solomon.
/// Le Développeur B (Ton collègue) remplira la vraie logique mathématique ici !
#[derive(Debug)]
pub struct RecoveryEncoder {
    pub percentage: u8,
    pub data_block_count: u32,
}

impl RecoveryEncoder {
    pub fn new(percentage: u8) -> Self {
        Self {
            percentage,
            data_block_count: 0,
        }
    }

    /// Appelé par le Writer à chaque fois qu'un bloc est écrit sur le disque
    pub fn push_data(&mut self, _data: &[u8]) {
        // Plus tard, ton collègue mettra à jour la RAM de Reed-Solomon ici
        self.data_block_count += 1;
    }

    /// Appelé par le Writer à la toute fin pour récupérer les blocs de secours
    pub fn finish(self) -> (RecoveryMetadata, Vec<u8>) {
        // FAUSSES DONNÉES pour que tu puisses tester l'écriture
        let fake_parity_bytes = vec![0u8; 1024]; // 1 Ko de fausse parité

        let meta = RecoveryMetadata {
            percentage: self.percentage,
            data_block_count: self.data_block_count,
            parity_block_count: 1, // Faux nombre pour l'instant
            parity_bytes_len: fake_parity_bytes.len() as u64,
        };

        (meta, fake_parity_bytes)
    }
}