pub mod policy;
pub mod lru;
pub mod mining_aware;
pub use lru::LruPolicy;
pub use mining_aware::{MiningAwarePolicy, EvictionWeights};
pub use policy::EvictionPolicy;
