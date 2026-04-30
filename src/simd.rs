use std::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_movemask_epi8, _mm_set1_epi8};

pub unsafe fn simd_match_byte(simd_data: __m128i, byte: u8) -> u16 {
    unsafe {
        let hash_vec = _mm_set1_epi8(byte as i8);

        let cmp = _mm_cmpeq_epi8(simd_data, hash_vec);
        _mm_movemask_epi8(cmp) as u16
    }
}
