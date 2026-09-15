pub mod fixed;
// Linux-only: splice(2) and its pipe are the whole point of this module.
#[cfg(target_os = "linux")]
pub(crate) mod pipe_pool;
pub mod send_copy;
#[cfg(has_io_uring)]
pub mod send_slab;
