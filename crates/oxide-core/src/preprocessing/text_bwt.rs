use crate::Result;

/// Applies a blockwise Burrows-Wheeler transform.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let n = data.len();

    let mut doubled = Vec::with_capacity(n * 2);
    doubled.extend_from_slice(data);
    doubled.extend_from_slice(data);

    let mut indices: Vec<usize> = (0..n).collect();

    indices.sort_unstable_by(|&a, &b| {
        doubled[a..a + n].cmp(&doubled[b..b + n])
    });

    let primary_index = indices.iter().position(|&idx| idx == 0).unwrap() as u32;

    let mut output = Vec::with_capacity(4 + n);

    output.extend_from_slice(&primary_index.to_le_bytes());

    for &idx in &indices {
        let bwt_byte = if idx == 0 {
            data[n - 1] 
        } else {
            data[idx - 1] 
        };
        output.push(bwt_byte);
    }

    Ok(output)
}

/// Reverses the blockwise Burrows-Wheeler transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 4 {
        return Ok(data.to_vec());
    }

    let primary_index = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let bwt_data = &data[4..];

    if bwt_data.is_empty() {
        return Ok(Vec::new());
    }

    if primary_index >= bwt_data.len() {
        return Ok(data.to_vec());
    }

    let mut counts = [0usize; 256];
    for &byte in bwt_data {
        counts[byte as usize] += 1;
    }

    let mut starts = [0usize; 256];
    let mut sum = 0;
    for (start, &count) in starts.iter_mut().zip(counts.iter()) {
        *start = sum;
        sum += count;
    }

    let mut next_row = vec![0usize; bwt_data.len()];
    
    for (i, &byte) in bwt_data.iter().enumerate() {
        let b = byte as usize;
        next_row[starts[b]] = i;
        starts[b] += 1;
    }

    let mut output = Vec::with_capacity(bwt_data.len());
    let mut current_row = primary_index;

    for _ in 0..bwt_data.len() {
        let next = next_row[current_row];
        output.push(bwt_data[next]);
        current_row = next;
    }

    Ok(output)
}
