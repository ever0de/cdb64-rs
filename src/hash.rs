pub struct CDBHash(pub u64);

impl Default for CDBHash {
    fn default() -> Self {
        Self::new()
    }
}

impl CDBHash {
    const START: u64 = 5_381;

    pub const fn new() -> Self {
        Self(Self::START)
    }

    pub fn reset(&mut self) {
        self.0 = Self::START;
    }

    pub const fn size(self) -> usize {
        8
    }

    pub fn write(&mut self, data: &[u8]) {
        let mut v = self.0;
        for b in data {
            v = ((v << 5) + v) ^ u64::from(*b);
        }
        self.0 = v;
    }
}
