use core::mem::size_of;
use core::ptr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyKernel {
    Rle,
    Repeat2,
    Repeat4,
    Repeat8,
    NonOverlapping,
    Overlap,
}

#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
pub(super) unsafe fn copy_match(dst: *mut u8, offset: usize, len: usize) -> CopyKernel {
    let src = dst.sub(offset);

    if offset == 1 {
        ptr::write_bytes(dst, src.read(), len);
        return CopyKernel::Rle;
    }

    if offset == 2 && len >= 8 {
        copy_repeat_u16(dst, src, len);
        return CopyKernel::Repeat2;
    }

    if offset == 4 && len >= 8 {
        copy_repeat_u32(dst, src, len);
        return CopyKernel::Repeat4;
    }

    if offset == 8 && len >= 16 {
        copy_repeat_u64(dst, src, len);
        return CopyKernel::Repeat8;
    }

    if offset >= len {
        ptr::copy_nonoverlapping(src, dst, len);
        return CopyKernel::NonOverlapping;
    }

    copy_overlap(dst, src, len, offset);
    CopyKernel::Overlap
}

#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn copy_repeat_u16(dst: *mut u8, src: *const u8, len: usize) {
    let seed = ptr::read_unaligned(src as *const u16) as u64;
    let pattern = seed.wrapping_mul(0x0001_0001_0001_0001);
    let mut written = 0usize;

    while written + size_of::<u64>() <= len {
        ptr::write_unaligned(dst.add(written) as *mut u64, pattern);
        written += size_of::<u64>();
    }

    while written < len {
        *dst.add(written) = *src.add(written & 1);
        written += 1;
    }
}

#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn copy_repeat_u32(dst: *mut u8, src: *const u8, len: usize) {
    let seed = ptr::read_unaligned(src as *const u32) as u64;
    let pattern = (seed << 32) | seed;
    let mut written = 0usize;

    while written + size_of::<u64>() <= len {
        ptr::write_unaligned(dst.add(written) as *mut u64, pattern);
        written += size_of::<u64>();
    }

    while written < len {
        *dst.add(written) = *src.add(written & 3);
        written += 1;
    }
}

#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn copy_repeat_u64(dst: *mut u8, src: *const u8, len: usize) {
    let pattern = ptr::read_unaligned(src as *const u64);
    let mut written = 0usize;

    while written + size_of::<u64>() <= len {
        ptr::write_unaligned(dst.add(written) as *mut u64, pattern);
        written += size_of::<u64>();
    }

    while written < len {
        *dst.add(written) = *src.add(written & 7);
        written += 1;
    }
}

#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn copy_overlap(dst: *mut u8, src: *const u8, len: usize, offset: usize) {
    let seed = offset.min(len);
    ptr::copy_nonoverlapping(src, dst, seed);

    let mut written = seed;
    while written < len {
        let chunk = written.min(len - written);
        ptr::copy_nonoverlapping(dst, dst.add(written), chunk);
        written += chunk;
    }
}
