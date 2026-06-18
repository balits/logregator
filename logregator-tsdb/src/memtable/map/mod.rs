mod btree_impl;
mod skiplist_impl;

#[derive(thiserror::Error, Debug)]
pub enum MapError {
    #[error("map impl does not support inserts through a read-only (&self) referenc")]
    InsertWithImmutableRef,

    #[error("rwlock poisoned")]
    RwlockPoisoned,
}



#[cfg(not(feature = "crossbeam-skiplist"))]
pub(super) use btree_impl::MapOwned as Map;
#[cfg(not(feature = "crossbeam-skiplist"))]
pub(super) use btree_impl::MapFrozen as MapFrozen;

#[cfg(feature = "crossbeam-skiplist")]
use skiplist_impl::MapImpl as Map;