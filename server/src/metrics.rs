use lazy_static::lazy_static;
use prometheus::{Counter, Registry};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref TOTAL_REQUESTS: Counter = Counter::new("total_requests", "Total requests").unwrap();
    pub static ref TOTAL_KEYS: Counter = Counter::new("total_keys", "Total keys").unwrap();
    pub static ref TOTAL_MISSES: Counter = Counter::new("total_misses", "Total misses").unwrap();
    pub static ref TOTAL_ERRORS: Counter = Counter::new("total_errors", "Total errors").unwrap();
}

pub fn init_metrics() {
    REGISTRY.register(Box::new(TOTAL_REQUESTS.clone())).unwrap();
    REGISTRY.register(Box::new(TOTAL_ERRORS.clone())).unwrap();
    REGISTRY.register(Box::new(TOTAL_KEYS.clone())).unwrap();
    REGISTRY.register(Box::new(TOTAL_MISSES.clone())).unwrap();
} 