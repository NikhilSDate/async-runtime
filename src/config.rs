pub struct Config {
    pub events_capacity: usize,
    pub worker_threads: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            events_capacity: 1024,
            worker_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        }
    }
}
