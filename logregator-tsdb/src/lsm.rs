use std::{collections::VecDeque, sync::Arc};

use crate::{memtable::{FrozenMemtable, Memtable}, record::{Key, Value}};

pub struct Lsm {
    _state: LsmState
}

pub struct LsmState {
    active_memtable: Memtable,
    frozen_memtables: VecDeque<Arc<FrozenMemtable>>
}

impl LsmState {
    pub fn append_kv(&mut self, key: Key, value: Value) {
        if self.active_memtable.append(key, value) {
            self.frozen_memtables.push_front(self.active_memtable.freeze());
        }
    }

    pub fn pop_frozen(&mut self) -> Option<Arc<FrozenMemtable>> {
        self.frozen_memtables.pop_back()
    }
}