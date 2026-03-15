from pathlib import Path
from PIL import Image


def get_image_files(directory):
    """Find all image files in directory recursively."""
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.tiff', '.webp'}
    files = []
    for ext in image_exts:
        files.extend(Path(directory).rglob(f'*{ext}'))
        files.extend(Path(directory).rglob(f'*{ext.upper()}'))
    return files


def get_audio_files(directory):
    """Find all audio files in directory recursively."""
    audio_exts = {'.wav', '.mp3', '.ogg', '.flac', '.aac', '.m4a', '.wma'}
    files = []
    for ext in audio_exts:
        files.extend(Path(directory).rglob(f'*{ext}'))
        files.extend(Path(directory).rglob(f'*{ext.upper()}'))
    return files


def convert_image_to_bmp_inplace(input_path):
    """Convert image to uncompressed BMP format in-place (replaces original)."""
    try:
        img = Image.open(input_path)
        
        # Convert to RGB if necessary (BMP doesn't support all modes like RGBA, P)
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Create output path (same directory, .bmp extension)
        output_path = input_path.with_suffix('.bmp')
        
        # Save as BMP (uncompressed)
        img.save(output_path, 'BMP')
        
        # Remove original file if different from output
        if input_path != output_path:
            input_path.unlink()
        
        new_size = output_path.stat().st_size
        
        print(f"  {input_path.name} -> {output_path.name} ({new_size:,} bytes)")
        
        return output_path
    except Exception as e:
        print(f"  Error converting {input_path}: {e}")
        return None


def main():
    # Configuration - in-place conversion
    datasets_dir = Path("./datasets")
    MAX_IMAGE_SIZE_BYTES = 500 * 1024 * 1024  # 0.5 GB limit for images
    
    print("=" * 60)
    print("Dataset Raw Conversion Tool (In-Place)")
    print("Converting to uncompressed formats:")
    print("  - Images: BMP (uncompressed bitmap, max ~0.5GB total)")
    print("  - Audio: WAV (PCM uncompressed)")
    print("=" * 60)
    
    total_images = 0
    total_image_bytes = 0
    
    # Process Images
    print("\n--- Processing Images ---")
    images_dir = datasets_dir / "IMAGES"
    
    if images_dir.exists():
        image_files = get_image_files(images_dir)
        print(f"Found {len(image_files)} image files")
        print(f"Size limit: {MAX_IMAGE_SIZE_BYTES / (1024*1024):.0f} MB")
        
        for img_path in image_files:
            # Check if adding this image would exceed the limit
            original_size = img_path.stat().st_size
            
            # Estimate BMP size (roughly width * height * 3 bytes for RGB)
            try:
                with Image.open(img_path) as img:
                    width, height = img.size
                    estimated_bmp_size = width * height * 3 + 54  # +54 for BMP header
            except:
                estimated_bmp_size = original_size * 10  # rough estimate if can't open
            
            if total_image_bytes + estimated_bmp_size > MAX_IMAGE_SIZE_BYTES:
                print(f"  Stopping: reached ~0.5GB limit ({total_image_bytes / (1024*1024):.1f} MB converted)")
                break
            
            result = convert_image_to_bmp_inplace(img_path)
            if result:
                total_images += 1
                total_image_bytes += result.stat().st_size
                print(f"    Total: {total_image_bytes / (1024*1024):.1f} MB")
    else:
        print(f"Images directory not found: {images_dir}")
    
    print("\n" + "=" * 60)
    print("Conversion complete!")
    print(f"  Images converted: {total_images}")
    print(f"  Total size: {total_image_bytes / (1024*1024):.1f} MB")
    print("=" * 60)


if __name__ == "__main__":
    main()
