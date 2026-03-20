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

    if max_count >= 3 {
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
    let mut next_symbol = 256; 
    const MAX_DICT_SIZE: usize = 256;
    loop {
        
        if dictionary.len() >= MAX_DICT_SIZE {
            break;
        }

        if let Some((best_pair, count)) = find_most_frequent_pair(&symbols) {
            symbols = replace_pair(&symbols, best_pair, next_symbol);
            dictionary.push(best_pair);
            next_symbol += 1;
        } else {
            break;
        }
    }

    
    let header_size = 4 + (dictionary.len() * 8);
    let payload_size = symbols.len() * 4;
    let total_size = header_size + payload_size;

    
    if total_size >= data.len() {
        return Ok(data.to_vec());
    }

    let mut output = Vec::with_capacity(total_size);
    
    output.extend_from_slice(&(dictionary.len() as u32).to_le_bytes());
    
    for (left, right) in dictionary {
        output.extend_from_slice(&left.to_le_bytes());
        output.extend_from_slice(&right.to_le_bytes());
    }
    
    for sym in symbols {
        output.extend_from_slice(&sym.to_le_bytes());
    }
    
    Ok(output)
}

/// Reverses the BPE transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 4 || data.len() % 4 != 0 {
        return Ok(data.to_vec());
    }

    let mut chunks = data.chunks_exact(4);

    let dict_len_chunk = chunks.next().unwrap();
    let dict_len = u32::from_le_bytes(dict_len_chunk.try_into().unwrap()) as usize;

    let mut expansions: Vec<Vec<u8>> = Vec::with_capacity(dict_len);

    for _ in 0..dict_len {
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

    let mut output = Vec::new();

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
