#[derive(Debug, Default)]
pub struct Counter {
    current: u64,
}

impl Counter {
    pub fn inc_and_get(&mut self) -> u64 {
        let next = self.current;
        self.current = self.current.wrapping_add(1);
        next
    }

    pub fn current(&self) -> u64 {
        self.current
    }
}

impl From<u64> for Counter {
    fn from(current: u64) -> Self {
        Self { current }
    }
}
