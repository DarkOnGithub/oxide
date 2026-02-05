# Preprocessing Strategies

## Overview

Preprocessing transforms data to reduce entropy before compression, enabling general-purpose compressors to achieve significantly better ratios. Oxide implements domain-specific filters based on data type detection.

## Strategy Selection Matrix

| Content Type | Strategy | Compression Gain | CPU Cost | Best With |
|--------------|----------|------------------|----------|-----------|
| x86 Binary | BCJ | 10-30% | Low | LZMA |
| Text (DNA) | BWT | 20-50% | High | LZMA |
| Text (Natural) | BPE | 15-25% | Medium | Deflate |
| Raw Audio | LPC | 20-50% | Low | LZ4/Deflate |
| Raw Image | Paeth | 10-20% | Low | Deflate/LZMA |
| Color Image | YCoCgR | 5-15% | Low | Any |

---

## Binary Strategies

### BCJ (Branch Call Jump)

**Purpose**: Improve compression of x86/x64 executable files by converting relative branch instructions to absolute addresses.

**Problem**: x86 code uses relative offsets for jumps (E8/E9 opcodes). After relocation, these offsets appear random to the compressor, reducing LZ match efficiency.

**Solution**: Convert relative offsets to absolute addresses before compression.

**Algorithm Details**:

```rust
/// x86 BCJ filter
/// Detects: E8/E9 xx xx xx xx (CALL/JMP rel32)
pub struct BcjFilter;

impl Preprocessor for BcjFilter {
    fn transform(&self, data: &[u8]) -> Cow<[u8]> {
        let mut modified = false;
        let mut output = Vec::with_capacity(data.len());
        output.extend_from_slice(data);
        
        for i in 0..data.len().saturating_sub(5) {
            // Check for CALL (0xE8) or JMP (0xE9) near
            if data[i] == 0xE8 || data[i] == 0xE9 {
                let offset = i32::from_le_bytes([
                    data[i+1], data[i+2], 
                    data[i+3], data[i+4]
                ]);
                
                // Convert relative to absolute address
                let absolute = (i as i32 + 5 + offset) as u32;
                let bytes = absolute.to_le_bytes();
                
                output[i+1] = bytes[0];
                output[i+2] = bytes[1];
                output[i+3] = bytes[2];
                output[i+4] = bytes[3];
                modified = true;
            }
        }
        
        if modified {
            Cow::Owned(output)
        } else {
            Cow::Borrowed(data)
        }
    }
    
    fn reverse(&self, data: &mut [u8]) {
        // Inverse operation: absolute back to relative
        for i in 0..data.len().saturating_sub(5) {
            if data[i] == 0xE8 || data[i] == 0xE9 {
                let absolute = u32::from_le_bytes([
                    data[i+1], data[i+2],
                    data[i+3], data[i+4]
                ]);
                
                let offset = (absolute as i32) - (i as i32 + 5);
                let bytes = offset.to_le_bytes();
                
                data[i+1] = bytes[0];
                data[i+2] = bytes[1];
                data[i+3] = bytes[2];
                data[i+4] = bytes[3];
            }
        }
    }
}
```

**Detection Heuristics**:
- File extension: `.exe`, `.dll`, `.so`, `.o`
- Magic bytes: ELF (0x7F ELF), PE (MZ), Mach-O (0xFEEDFACE)
- Pattern match: Frequency of 0xE8/0xE9 opcodes > 1%

**Performance**: 
- O(n) single pass
- ~2-3 GB/s throughput
- 10-30% compression improvement on executables

---

## Text Strategies

### BWT (Burrows-Wheeler Transform)

**Purpose**: Group similar characters together to create long runs for LZ compressors.

**Algorithm**:

```rust
pub struct BwtFilter;

impl Preprocessor for BwtFilter {
    fn transform(&self, data: &[u8]) -> Cow<[u8]> {
        if data.len() < 100 {
            return Cow::Borrowed(data); // Too small for benefit
        }
        
        let n = data.len();
        let mut suffixes: Vec<usize> = (0..n).collect();
        
        // Sort suffixes using comparison
        // Note: For production, use SA-IS or divsufsort algorithm
        suffixes.sort_by(|&a, &b| data[a..].cmp(&data[b..]));
        
        // Build BWT string (last column of sorted rotations)
        let mut bwt = Vec::with_capacity(n + 4); // +4 for primary index
        let mut primary_index: u32 = 0;
        
        for (i, &suffix) in suffixes.iter().enumerate() {
            if suffix == 0 {
                bwt.push(data[n-1]);
                primary_index = i as u32;
            } else {
                bwt.push(data[suffix-1]);
            }
        }
        
        // Prepend primary index for inverse transform
        let mut result = Vec::with_capacity(n + 4);
        result.extend_from_slice(&primary_index.to_le_bytes());
        result.extend(bwt);
        
        Cow::Owned(result)
    }
}
```

**Use Cases**:
- DNA sequences (highly repetitive)
- Log files with similar patterns
- Source code with repeated keywords

**Performance**:
- O(n log n) with naive sort, O(n) with SA-IS
- 100-500 KB/s for BWT transform
- 20-50% better compression on repetitive text

---

## Audio Strategies

### LPC (Linear Predictive Coding)

**Purpose**: Remove temporal redundancy in audio samples.

**Algorithm**:

```rust
pub struct LpcFilter {
    order: usize, // Prediction order (typically 4-10)
}

impl Preprocessor for LpcFilter {
    fn transform(&self, samples: &[i16]) -> Cow<[u8]> {
        let mut residuals = Vec::with_capacity(samples.len() * 2);
        let mut coeffs = vec![0.0f32; self.order];
        
        // Burg algorithm for coefficient estimation
        self.estimate_coeffs_burg(samples, &mut coeffs);
        
        // Store coefficients in output (order * 4 bytes)
        for &c in &coeffs {
            residuals.extend_from_slice(&c.to_le_bytes());
        }
        
        // Compute and store residuals
        for i in 0..samples.len() {
            let predicted = if i < self.order {
                0i16
            } else {
                let mut sum = 0.0f32;
                for j in 0..self.order {
                    sum += coeffs[j] * samples[i-j-1] as f32;
                }
                sum as i16
            };
            
            let residual = samples[i].wrapping_sub(predicted);
            residuals.extend_from_slice(&residual.to_le_bytes());
        }
        
        Cow::Owned(residuals)
    }
}
```

**Detection**:
- WAV/AIFF headers
- PCM audio format markers
- Sample rate metadata

**Performance**:
- O(n * order) complexity
- 20-50% compression improvement
- Near-lossless quality

---

## Image Strategies

### Paeth Filter

**Purpose**: Spatial prediction for raw bitmap data (PNG-style filtering).

**Algorithm**:

```rust
pub struct PaethFilter;

impl PaethFilter {
    fn paeth_predictor(a: u8, b: u8, c: u8) -> u8 {
        let pa = (b as i16 - c as i16).abs();
        let pb = (a as i16 - c as i16).abs();
        let pc = ((a as i16 + b as i16 - 2 * c as i16)).abs();
        
        if pa <= pb && pa <= pc { a }
        else if pb <= pc { b }
        else { c }
    }
    
    fn transform(&self, image: &[u8], width: usize) -> Vec<u8> {
        let height = image.len() / width;
        let mut filtered = Vec::with_capacity(image.len());
        
        for y in 0..height {
            for x in 0..width {
                let idx = y * width + x;
                let current = image[idx];
                
                let left = if x > 0 { image[idx - 1] } else { 0 };
                let up = if y > 0 { image[idx - width] } else { 0 };
                let upleft = if x > 0 && y > 0 { 
                    image[idx - width - 1] 
                } else { 0 };
                
                let predicted = Self::paeth_predictor(left, up, upleft);
                filtered.push(current.wrapping_sub(predicted));
            }
        }
        
        filtered
    }
}
```

**Use Cases**:
- Uncompressed BMP files
- Raw camera sensor data
- Medical imaging (DICOM)

**Performance**:
- O(n) single pass
- 10-20% compression improvement
- Very low CPU overhead

---

## Implementation Guidelines

### Creating a New Preprocessor

```rust
pub trait Preprocessor: Send + Sync {
    /// Transform data before compression
    /// Returns Cow to avoid allocation when not needed
    fn transform(&self, data: &[u8]) -> Cow<[u8]>;
    
    /// Reverse transformation for decompression
    fn reverse(&self, data: &mut [u8]);
    
    /// Cost estimate (CPU cycles per byte)
    fn cost(&self) -> f64;
    
    /// Expected compression improvement (%)
    fn expected_gain(&self) -> f64;
}
```

### Registration

```rust
pub struct PreprocessorRegistry {
    filters: HashMap<PreProcessingStrategy, Box<dyn Preprocessor>>,
}

impl PreprocessorRegistry {
    pub fn new() -> Self {
        let mut filters = HashMap::new();
        filters.insert(
            PreProcessingStrategy::Binary(BinaryStrategy::BCJ),
            Box::new(BcjFilter) as Box<dyn Preprocessor>
        );
        // ... register other filters
        
        Self { filters }
    }
    
    pub fn get(&self, strategy: &PreProcessingStrategy) -> Option<&dyn Preprocessor> {
        self.filters.get(strategy).map(|b| b.as_ref())
    }
}
```

---

## Testing Preprocessors

### Roundtrip Testing

```rust
#[test]
fn test_bcj_roundtrip() {
    let filter = BcjFilter;
    let original = generate_x86_code();
    
    let transformed = filter.transform(&original);
    let mut reversed = transformed.into_owned();
    filter.reverse(&mut reversed);
    
    assert_eq!(original, reversed);
}
```

### Compression Efficacy

```rust
#[test]
fn test_bwt_improvement() {
    let data = load_dna_sequence();
    let filter = BwtFilter;
    
    let compressed_before = lzma_compress(&data);
    let transformed = filter.transform(&data);
    let compressed_after = lzma_compress(&transformed);
    
    let improvement = 1.0 - (compressed_after.len() as f64 
        / compressed_before.len() as f64);
    
    assert!(improvement > 0.20, "Expected >20% improvement");
}
```

---

## References

1. **BCJ**: x86 Branch Converter from 7-Zip
2. **BWT**: Burrows, M. and Wheeler, D.J. (1994) A Block-sorting Lossless Data Compression Algorithm
3. **BPE**: Sennrich et al. (2016) Neural Machine Translation of Rare Words with Subword Units
4. **LPC**: Rabiner, L.R. and Schafer, R.W. (1978) Digital Processing of Speech Signals
5. **Paeth**: Paeth, A.W. (1991) Image File Compression Made Easy

---

*Version: 1.0*
*Last Updated: 2026-02-05*
