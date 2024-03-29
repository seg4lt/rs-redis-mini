use std::time::SystemTime;

pub fn generate_random_string() -> String {
    let chars: &[u8] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".as_bytes();
    let hash = (0..40)
        .map(|i| {
            let random = pcg32(i + 1) as usize;
            chars[(random % chars.len()) as usize]
        })
        .collect::<Vec<u8>>();
    let hash = String::from_utf8(hash).unwrap();
    hash
}
// Loose implementation of https://en.wikipedia.org/wiki/Permuted_congruential_generator#Example_code
fn pcg32(out_seed: usize) -> u32 {
    let org_state: u64 = 0x4d595df4d0f33173;
    let multiplier: u64 = 6364136223846793005;
    let increment: u64 = 1442695040888963407;

    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let state = org_state
        .wrapping_pow(out_seed as u32)
        .wrapping_mul(out_seed as u64)
        .wrapping_mul(seed)
        .wrapping_add(increment)
        .wrapping_mul(multiplier);

    let mut x: u64 = state;
    let count: u32 = (x >> 59) as u32;
    x ^= x >> 18;
    let result = (x >> 27) as u32;
    return result.rotate_right(count);
}
