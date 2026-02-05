# Oxide File Format Specification (.oxz)

## Overview

The `.oxz` (Oxide eXtended Zip) format is a self-describing archival format that supports content-aware preprocessing and multiple compression algorithms. Each block contains metadata describing the transformations applied, enabling correct decompression without external configuration.

## Design Goals

1. **Self-Describing**: All metadata embedded in the file
2. **Random Access**: Fixed-size headers enable seeking to any block
3. **Integrity**: Per-block and global CRC32 checksums
4. **Extensibility**: Reserved fields for future expansion
5. **Streaming**: Can be written/read sequentially without seeking

## File Structure

```
┌─────────────────────────────────────────────────────────┐
│                    GLOBAL HEADER (16 bytes)              │
├─────────────────────────────────────────────────────────┤
│  Magic         "OXZ\0"          4 bytes                  │
│  Version       u16              2 bytes                  │
│  Reserved      u16              2 bytes                  │
│  Flags         u32              4 bytes                  │
│  Block Count   u32              4 bytes                  │
├─────────────────────────────────────────────────────────┤
│                    BLOCK 1 HEADER (24 bytes)             │
├─────────────────────────────────────────────────────────┤
│  Block ID      u64              8 bytes                  │
│  Original Size u32              4 bytes                  │
│  Compressed    u32              4 bytes                  │
│  Strategy      u8               1 byte                   │
│  Compression   u8               1 byte                   │
│  Reserved      u16              2 bytes                  │
│  CRC32         u32              4 bytes                  │
├─────────────────────────────────────────────────────────┤
│                    BLOCK 1 DATA (variable)               │
├─────────────────────────────────────────────────────────┤
│  Compressed payload data                                 │
├─────────────────────────────────────────────────────────┤
│                    BLOCK 2 HEADER (24 bytes)             │
│                    BLOCK 2 DATA                          │
│                    ...                                   │
├─────────────────────────────────────────────────────────┤
│                    FOOTER (8 bytes)                      │
├─────────────────────────────────────────────────────────┤
│  End Magic     "END\0"          4 bytes                  │
│  Global CRC32  u32              4 bytes                  │
└─────────────────────────────────────────────────────────┘
```

## Detailed Specification

### Global Header (16 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 4 | Magic | ASCII "OXZ\0" (0x4F 0x58 0x5A 0x00) |
| 0x04 | 2 | Version | Format version (current: 0x0001) |
| 0x06 | 2 | Reserved | Reserved for future use (must be 0) |
| 0x08 | 4 | Flags | Global archive flags (see below) |
| 0x0C | 4 | Block Count | Total number of blocks in archive |

#### Global Flags

| Bit | Name | Description |
|-----|------|-------------|
| 0 | ENCRYPTED | Archive is encrypted |
| 1-7 | Reserved | Must be 0 |
| 8-31 | Custom | Application-specific flags |

### Block Header (24 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 8 | Block ID | Unique sequential identifier |
| 0x08 | 4 | Original Size | Uncompressed size in bytes |
| 0x0C | 4 | Compressed Size | Compressed size in bytes |
| 0x10 | 1 | Strategy Flags | Preprocessing strategy (see below) |
| 0x11 | 1 | Compression Flags | Compression algorithm (see below) |
| 0x12 | 2 | Reserved | Reserved for future use |
| 0x14 | 4 | CRC32 | CRC32 of compressed data |

#### Strategy Flags (1 byte)

Bits 0-2: Category
```
000 = None
001 = Text
010 = Image
011 = Audio
100 = Binary
101-111 = Reserved
```

Bits 3-5: Sub-strategy (category-specific)

**Text (Category 001)**:
```
000 = BPE (Byte Pair Encoding)
001 = BWT (Burrows-Wheeler Transform)
010-111 = Reserved
```

**Image (Category 010)**:
```
000 = YCoCgR (Color space transform)
001 = Paeth (PNG predictor)
010 = LocoI (JPEG-LS predictor)
011-111 = Reserved
```

**Audio (Category 011)**:
```
000 = LPC (Linear Predictive Coding)
001-111 = Reserved
```

**Binary (Category 100)**:
```
000 = BCJ (Branch Call Jump filter)
001 = BCJ2 (Two-pass BCJ)
010-111 = Reserved
```

Bits 6-7: Reserved

#### Compression Flags (1 byte)

```
0x01 = LZ4
0x02 = LZMA
0x03 = Deflate (Huffman)
0x04-0xFF = Reserved
```

### Block Data

Raw compressed bytes as produced by the selected compression algorithm. No additional framing.

### Footer (8 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 4 | End Magic | ASCII "END\0" (0x45 0x4E 0x44 0x00) |
| 0x04 | 4 | Global CRC32 | CRC32 of all preceding data |

## Example File Layout

```
Offset  Content
────────────────────────────────────────
0x0000  Global Header
        4F 58 5A 00        Magic "OXZ\0"
        01 00              Version 1
        00 00              Reserved
        00 00 00 00        Flags (none)
        03 00 00 00        3 blocks

0x0010  Block 1 Header
        00 00 00 00 00 00 00 00  ID = 0
        00 10 00 00              Original = 4096 bytes
        05 02 00 00              Compressed = 517 bytes
        21                       Strategy = Text | BWT
        02                       Compression = LZMA
        00 00                    Reserved
        XX XX XX XX              CRC32

0x0028  Block 1 Data (517 bytes)
        ...

0x022D  Block 2 Header
        01 00 00 00 00 00 00 00  ID = 1
        00 10 00 00              Original = 4096 bytes
        00 08 00 00              Compressed = 2048 bytes
        40                       Strategy = Binary | BCJ
        01                       Compression = LZ4
        00 00                    Reserved
        XX XX XX XX              CRC32

0x0245  Block 2 Data (2048 bytes)
        ...

0x0A45  Block 3 Header
        02 00 00 00 00 00 00 00  ID = 2
        00 10 00 00              Original = 4096 bytes
        03 01 00 00              Compressed = 259 bytes
        00                       Strategy = None
        02                       Compression = LZMA
        00 00                    Reserved
        XX XX XX XX              CRC32

0x0A5D  Block 3 Data (259 bytes)
        ...

0x0B60  Footer
        45 4E 44 00        Magic "END\0"
        XX XX XX XX        Global CRC32
```

## Decompression Algorithm

```rust
fn decompress_oxz(reader: impl Read) -> Result<Vec<u8>> {
    // Read and verify global header
    let header = read_global_header(&mut reader)?;
    verify_magic(&header.magic)?;
    
    let mut output = Vec::new();
    
    // Process each block
    for _ in 0..header.block_count {
        let block_header = read_block_header(&mut reader)?;
        let mut compressed = vec![0u8; block_header.compressed_size as usize];
        reader.read_exact(&mut compressed)?;
        
        // Verify CRC32
        let crc = crc32fast::hash(&compressed);
        if crc != block_header.crc32 {
            return Err(Error::ChecksumMismatch);
        }
        
        // Decompress
        let decompressed = match block_header.compression {
            0x01 => decompress_lz4(&compressed)?,
            0x02 => decompress_lzma(&compressed)?,
            0x03 => decompress_deflate(&compressed)?,
            _ => return Err(Error::UnknownCompression),
        };
        
        // Reverse preprocessing
        let final_data = reverse_preprocessing(
            decompressed, 
            block_header.strategy
        )?;
        
        output.extend(final_data);
    }
    
    // Verify footer
    let footer = read_footer(&mut reader)?;
    verify_global_crc32(&output, footer.global_crc32)?;
    
    Ok(output)
}

fn reverse_preprocessing(data: Vec<u8>, strategy: u8) -> Result<Vec<u8>> {
    let category = strategy & 0x07;
    let substrategy = (strategy >> 3) & 0x07;
    
    match (category, substrategy) {
        (0x00, _) => Ok(data), // None
        (0x01, 0x00) => reverse_bpe(data),
        (0x01, 0x01) => reverse_bwt(data),
        (0x02, 0x00) => reverse_ycocgr(data),
        (0x02, 0x01) => reverse_paeth(data),
        (0x03, 0x00) => reverse_lpc(data),
        (0x04, 0x00) => reverse_bcj(data),
        _ => Err(Error::UnknownStrategy),
    }
}
```

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0x0001 | 2026-02-05 | Initial specification |

## Future Extensions

Reserved fields enable future enhancements:

1. **Encryption**: Bit 0 of global flags indicates encrypted payload
2. **Compression Levels**: Use reserved bits in compression flags
3. **Dictionary Compression**: Add dictionary ID field
4. **Streaming**: Support for indefinite-length archives

## Compatibility

- **Forward Compatible**: Older readers can skip unknown strategies
- **Backward Compatible**: New versions maintain version 1 support
- **Graceful Degradation**: Unknown compression algorithms return error

## References

- CRC32: ISO 3309 / ITU-T V.42
- LZ4: [lz4.github.io/lz4](https://lz4.github.io/lz4)
- LZMA: [7-zip.org/7z.html](https://7-zip.org/7z.html)
- Deflate: RFC 1951

---

*Version: 1.0*
*Last Updated: 2026-02-05*
