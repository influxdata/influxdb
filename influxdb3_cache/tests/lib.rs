pub mod single_threaded;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;
