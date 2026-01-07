use ahash::RandomState;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::hash::BuildHasher;

static BLOOM_HASHER: RandomState =
    RandomState::with_seeds(0x12345678, 0x87654321, 0xdeadbeef, 0xcafebabe);
use std::hash::Hasher;

pub const BLOOM_FILTER_SUFFIX: &str = "bf";
pub const BLOOM_FP_RATE: f64 = 0.01;

#[inline]
fn hash64(key: &str, seed: u64) -> u64 {
    let mut hasher = BLOOM_HASHER.build_hasher();
    hasher.write_u64(seed);
    hasher.write(key.as_bytes());
    hasher.finish()
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
pub struct BloomFilter {
    bits: Vec<u8>,
    m: u64, // Total number of bits
    k: u8,  // Number of hash functions
}

impl BloomFilter {
    pub fn new(sst_size: usize, fp_rate: f64) -> Self {
        let (m, k) = Self::optimal_m_k(sst_size, fp_rate);
        let byte_len = ((m + 7) / 8) as usize;
        return Self {
            bits: vec![0; byte_len],
            m,
            k,
        };
    }

    #[inline]
    pub fn insert(&mut self, key: &str) {
        let h1 = hash64(key, 0);
        let h2 = hash64(key, 1);
        for i in 0..self.k {
            let bit = ((h1.wrapping_add(h2.wrapping_mul(i as u64))) % self.m) as usize;
            self.set_bit(bit);
        }
    }

    #[inline]
    pub fn might_contain(&self, key: &str) -> bool {
        let h1 = hash64(key, 0);
        let h2 = hash64(key, 1);
        for i in 0..self.k {
            let bit = ((h1.wrapping_add(h2.wrapping_mul(i as u64))) % self.m) as usize;
            if !self.get_bit(bit) {
                return false;
            }
        }
        return true;
    }

    #[inline]
    pub fn set_bit(&mut self, idx: usize) {
        let byte = idx / 8;
        let bit = idx % 8;
        self.bits[byte] |= 1 << bit;
    }

    #[inline]
    pub fn get_bit(&self, idx: usize) -> bool {
        let byte = idx / 8;
        let bit = idx % 8;
        let b = self.bits[byte] & (1 << bit);
        return b != 0;
    }

    fn optimal_m_k(n: usize, fp_rate: f64) -> (u64, u8) {
        let ln2 = std::f64::consts::LN_2;
        let m = (-(n as f64) * fp_rate.ln() / (ln2 * ln2)).ceil() as u64;
        let k = ((m as f64 / n as f64) * ln2).round() as u8;

        (m.max(64), k.clamp(1, 16))
    }
}
