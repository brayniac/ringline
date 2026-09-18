pub mod fixed;
pub(crate) mod prefault;
pub mod send_copy;
#[cfg(has_io_uring)]
pub mod send_slab;
