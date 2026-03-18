use crate::Result;
//use crate::preprocessing::utils;

/// Converts raw text bytes into integer symbol ids for BWT-style processing.
/*
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_symbols(data)
}
*/

/// Applies a blockwise Burrows-Wheeler transform.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    // 1. Gestion des cas particuliers (fichiers vides ou minuscules)
    if data.is_empty() {
        return Ok(Vec::new());
    }
    
    let n = data.len();

    // 2. L'astuce d'optimisation (Le tableau doublé)
    // On copie les données deux fois (ex: BANANA -> BANANABANANA)
    // Ça permet au processeur de comparer les rotations en ligne droite, sans faire de modulo.
    let mut doubled = Vec::with_capacity(n * 2);
    doubled.extend_from_slice(data);
    doubled.extend_from_slice(data);

    // 3. La matrice virtuelle (Les index)
    // On crée juste un tableau contenant [0, 1, 2, ..., n-1]
    let mut indices: Vec<usize> = (0..n).collect();

    // On trie ces index en comparant les blocs dans notre tableau doublé.
    // C'est ici que la magie de la BWT opère à la vitesse de la lumière !
    indices.sort_unstable_by(|&a, &b| {
        doubled[a..a + n].cmp(&doubled[b..b + n])
    });

    // 4. Recherche de l'index primaire
    // Après le tri, on doit retrouver à quelle ligne a atterri notre mot d'origine (l'index 0)
    let primary_index = indices.iter().position(|&idx| idx == 0).unwrap() as u32;

    // 5. Création du fichier final
    // Format attendu : [Index Primaire sur 4 octets] + [Texte transformé]
    let mut output = Vec::with_capacity(4 + n);
    
    // On écrit l'index (en Little-Endian, comme pour le BPE)
    output.extend_from_slice(&primary_index.to_le_bytes());

    // On génère la "dernière colonne" de la matrice.
    // Mathématiquement, la dernière colonne correspond toujours à la lettre qui précédait
    // le début de la rotation actuelle.
    for &idx in &indices {
        let bwt_byte = if idx == 0 {
            data[n - 1] // Si on est à l'index 0, la lettre d'avant est la toute dernière du fichier
        } else {
            data[idx - 1] // Sinon, c'est juste la lettre précédente
        };
        output.push(bwt_byte);
    }

    Ok(output)
}

/// Reverses the blockwise Burrows-Wheeler transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    // 1. Sécurité de base
    if data.len() < 4 {
        return Ok(data.to_vec());
    }

    // 2. Extraction des données
    let primary_index = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let bwt_data = &data[4..];

    if bwt_data.is_empty() {
        return Ok(Vec::new());
    }

    if primary_index >= bwt_data.len() {
        return Ok(data.to_vec());
    }

    // 3. Compter les occurrences (L'octet est notre "clé" d'indexation)
    let mut counts = [0usize; 256];
    for &byte in bwt_data {
        counts[byte as usize] += 1;
    }

    // 4. Trouver les points de départ (Méthode idiomatique avec zip)
    let mut starts = [0usize; 256];
    let mut sum = 0;
    for (start, &count) in starts.iter_mut().zip(counts.iter()) {
        *start = sum;
        sum += count;
    }

    // 5. LF-Mapping (Tableau de Saut)
    let mut next_row = vec![0usize; bwt_data.len()];
    
    // enumerate() nous donne l'index de la lettre dans bwt_data, car on DOIT
    // le sauvegarder dans notre tableau de saut.
    for (i, &byte) in bwt_data.iter().enumerate() {
        let b = byte as usize;
        next_row[starts[b]] = i;
        starts[b] += 1;
    }

    // 6. Reconstruction du texte
    let mut output = Vec::with_capacity(bwt_data.len());
    let mut current_row = primary_index;

    for _ in 0..bwt_data.len() {
        let next = next_row[current_row];
        output.push(bwt_data[next]);
        current_row = next;
    }

    Ok(output)
}
