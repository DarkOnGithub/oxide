use crate::Result;
use crate::preprocessing::utils;
use std::collections::HashMap;

/// Converts raw text bytes into integer symbol ids for BPE-style processing.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_symbols(data)
}

fn find_most_frequent_pair(data: &[u32]) -> Option<((u32, u32), usize)> {
    if data.len() < 2 {
        return None;
    }

    let mut frequencies = HashMap::new();
    let mut best_pair = (0, 0);
    let mut max_count = 0;

    for window in data.windows(2) {
        let pair = (window[0], window[1]);
        let count = frequencies.entry(pair).or_insert(0);
        *count += 1;

        if *count > max_count {
            max_count = *count;
            best_pair = pair;
        }
    }

    if max_count >= 2 {
        Some((best_pair, max_count))
    } else {
        None
    }
}

/// Replaces all occurrences of `target_pair` with `new_symbol`.
fn replace_pair(data: &[u32], target_pair: (u32, u32), new_symbol: u32) -> Vec<u32> {
    let mut new_data = Vec::with_capacity(data.len());
    
    let mut iter = data.iter().copied().peekable();

    while let Some(current) = iter.next() {
        if current == target_pair.0 && iter.peek() == Some(&target_pair.1) {
            new_data.push(new_symbol);
            iter.next(); 
        } else {
            new_data.push(current);
        }
    }

    new_data.shrink_to_fit();

    new_data
}

/// Applies a minimal byte-pair substitution transform.
///
/// The transform is only emitted when the transformed payload + header is
/// smaller than the input payload.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    let mut symbols = bytes_to_data(data);
    let mut dictionary = Vec::new();
    let mut next_symbol = 256; // Standard bytes are 0-255

    // 2. The BPE Loop
    loop {
        if let Some((best_pair, count)) = find_most_frequent_pair(&symbols) {
            // Replace the pair in the data
            symbols = replace_pair(&symbols, best_pair, next_symbol);
            
            // Record the rule in our dictionary
            dictionary.push(best_pair);
            
            // Prepare the next unique symbol
            next_symbol += 1;
        } else {
            // No more repeating pairs found, we stop!
            break;
        }
    }

    // 3. Profitability Check (The Oxide Rule)
    // How much space will our new file take?
    // - Header: 4 bytes (to store the number of rules) + 8 bytes per rule (two u32)
    // - Payload: 4 bytes per compressed symbol
    let header_size = 4 + (dictionary.len() * 8);
    let payload_size = symbols.len() * 4;
    let total_size = header_size + payload_size;

    // If the transformation didn't reduce the file size, abort and return original data
    if total_size >= data.len() {
        return Ok(data.to_vec());
    }

    // 4. Serialization
    let mut output = Vec::with_capacity(total_size);
    
    // Write dictionary length
    output.extend_from_slice(&(dictionary.len() as u32).to_le_bytes());
    
    // Write dictionary rules
    for (left, right) in dictionary {
        output.extend_from_slice(&left.to_le_bytes());
        output.extend_from_slice(&right.to_le_bytes());
    }
    
    // Write compressed symbols
    for sym in symbols {
        output.extend_from_slice(&sym.to_le_bytes());
    }

    Ok(output)
}

/// Reverses the BPE transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    // 1. Sécurité : Un fichier valide BPE fait au moins 4 octets et est un multiple de 4
    if data.len() < 4 || data.len() % 4 != 0 {
        return Ok(data.to_vec());
    }

    // On crée notre itérateur magique qui va découper les données en paquets de 4 octets.
    // Il gère sa position tout seul en mémoire !
    let mut chunks = data.chunks_exact(4);

    // 2. Lecture de la taille du dictionnaire
    // chunks.next() nous donne le tout premier paquet de 4 octets.
    // Le unwrap() est 100% sûr ici car on a vérifié data.len() >= 4 juste au-dessus.
    let dict_len_chunk = chunks.next().unwrap();
    let dict_len = u32::from_le_bytes(dict_len_chunk.try_into().unwrap()) as usize;

    let mut expansions: Vec<Vec<u8>> = Vec::with_capacity(dict_len);

    // 3. Pré-calcul des expansions du dictionnaire
    for _ in 0..dict_len {
        // On consomme les deux paquets suivants pour la règle (gauche et droite).
        // Si l'itérateur est vide avant la fin, c'est que le fichier ment sur sa taille -> on annule tout !
        let left_chunk = match chunks.next() {
            Some(c) => c,
            None => return Ok(data.to_vec()), 
        };
        let right_chunk = match chunks.next() {
            Some(c) => c,
            None => return Ok(data.to_vec()),
        };

        let left = u32::from_le_bytes(left_chunk.try_into().unwrap());
        let right = u32::from_le_bytes(right_chunk.try_into().unwrap());

        let mut expansion = Vec::new();

        // Décodage de la partie gauche
        if left < 256 {
            expansion.push(left as u8);
        } else {
            let index = (left - 256) as usize;
            if index < expansions.len() {
                expansion.extend_from_slice(&expansions[index]);
            } else {
                return Ok(data.to_vec());
            }
        }

        // Décodage de la partie droite
        if right < 256 {
            expansion.push(right as u8);
        } else {
            let index = (right - 256) as usize;
            if index < expansions.len() {
                expansion.extend_from_slice(&expansions[index]);
            } else {
                return Ok(data.to_vec());
            }
        }

        expansions.push(expansion);
    }

    // 4. Décompression du texte final
    let mut output = Vec::new();

    // L'itérateur a consommé l'en-tête et le dictionnaire. 
    // Tout ce qui lui reste maintenant, c'est notre texte compressé ! 
    // On boucle simplement sur la fin.
    for chunk in chunks {
        let sym = u32::from_le_bytes(chunk.try_into().unwrap());

        if sym < 256 {
            output.push(sym as u8);
        } else {
            let index = (sym - 256) as usize;
            if index < expansions.len() {
                output.extend_from_slice(&expansions[index]);
            } else {
                return Ok(data.to_vec());
            }
        }
    }

    Ok(output)
}
