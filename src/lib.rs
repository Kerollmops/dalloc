use std::mem::{self, size_of};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs::File, io};

use bytemuck::{Contiguous, Pod, Zeroable};
use memmap2::Mmap;
use parking_lot::{FairMutex, FairMutexGuard};

// The page size of the environment, as determined by the build script.
// const PAGE_SIZE: usize = {page size at build time};
include!(concat!(env!("OUT_DIR"), "/page_size.rs"));

pub struct Environment {
    path: PathBuf,
    file: FairMutex<File>,
    mmap: Mmap,
}

impl Environment {
    // TODO: change this to an Into<PathBuf>
    pub fn new(path: PathBuf) -> io::Result<Self> {
        assert_eq!(
            PAGE_SIZE,
            page_size::get(),
            "The runtime page size does not match the build-time page size"
        );

        let file = File::create_new(&path)?;
        file.try_lock()?;
        file.set_len(1024 * 1024 * 1024)?; // 1GiB
        // unsafe: we know the file is locked and we own it, so we can safely map it.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Environment { path, file: FairMutex::new(file), mmap })
    }

    // TODO introduce a custom error type
    pub fn write_txn<'e>(&'e self) -> io::Result<RwTxn<'e>> {
        let file_lock = self.file.lock();
        Ok(RwTxn { rtxn: RoTxn { env: self }, file_lock })
    }

    // TODO introduce a custom error type
    pub fn try_write_txn<'e>(&'e self) -> io::Result<RwTxn<'e>> {
        todo!()
    }

    // TODO introduce a custom error type
    pub fn read_txn<'e>(&'e self) -> io::Result<RoTxn<'e>> {
        Ok(RoTxn { env: self })
    }
}

/// A transaction that can read and write pages.
pub struct RwTxn<'e> {
    rtxn: RoTxn<'e>,
    file_lock: FairMutexGuard<'e, File>,
}

impl<'e> RwTxn<'e> {
    /// Allocates a new page and returns it's PageId.
    pub fn alloc_page(&mut self) -> io::Result<PageId> {
        todo!()
    }

    pub unsafe fn write_page(&mut self, page_id: PageId, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.file_lock.write_all_at(data, page_id.offset_u64())
    }

    /// Frees a page by its PageId and puts it in the free list.
    pub unsafe fn free_page(&mut self, page_id: PageId) -> io::Result<()> {
        todo!()
    }

    /// FSync the page changes to disk, swaps the root
    /// and fsync the root changes to make the changes visible.
    pub fn commit(mut self) -> io::Result<()> {
        // TODO should I call sync_all here when I increased the size of the file?
        // Sync the user data that we previously wrote to disk.
        self.file_lock.sync_data()?;

        // Swap
        // safety: We are reading the root page, which is guaranteed to exist.
        let root_page = unsafe { self.rtxn.read_page(PageId::root())? };
        let mut root_page = root_page.as_root_page().clone();
        root_page.swap_writable_root()?;
        unsafe { self.write_page(PageId::root(), bytemuck::cast_ref(&root_page))? };
        self.file_lock.sync_data()?;

        Ok(())
    }
}

/// A transaction that can only read pages.
pub struct RoTxn<'e> {
    env: &'e Environment,
}

impl<'e> RoTxn<'e> {
    pub unsafe fn read_page<'m>(&'m self, page_id: PageId) -> io::Result<Page<'m>> {
        Ok(Page(self.env.mmap[page_id.offset()..][..PAGE_SIZE].try_into().unwrap()))
    }
}

#[repr(transparent)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub struct PageId(pub usize);

impl PageId {
    fn offset(self) -> usize {
        self.0 * PAGE_SIZE
    }

    fn offset_u64(self) -> u64 {
        // TODO check only in debug builds
        self.offset().try_into().unwrap()
    }
}

impl PageId {
    /// Creates the root page id.
    fn root() -> Self {
        Self(0)
    }
}

pub struct Page<'m>(&'m [u8; PAGE_SIZE]);

impl<'m> Page<'m> {
    pub fn as_root_page(&self) -> &'m RootPage {
        bytemuck::from_bytes(self.0)
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Contiguous)]
enum WritableRootPage {
    Alpha = 0,
    Beta = 1,
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct RootPage {
    writable_root: u8,
    _padding: [u8; 7],
    root_alpha: PageId,
    root_beta: PageId,
    remaining: [u8; PAGE_SIZE - (2 * size_of::<PageId>() + size_of::<usize>())],
}

impl RootPage {
    fn swap_writable_root(&mut self) -> io::Result<()> {
        self.writable_root = match self.writable_root_page()? {
            WritableRootPage::Alpha => WritableRootPage::Beta as u8,
            WritableRootPage::Beta => WritableRootPage::Alpha as u8,
        };

        mem::swap(&mut self.root_alpha, &mut self.root_beta);

        Ok(())
    }

    // TODO actually return an error
    fn writable_root_page_id(&self) -> io::Result<PageId> {
        match self.writable_root_page()? {
            WritableRootPage::Alpha => Ok(self.root_alpha),
            WritableRootPage::Beta => Ok(self.root_beta),
        }
    }

    fn writable_root_page(&self) -> io::Result<WritableRootPage> {
        match WritableRootPage::from_integer(self.writable_root) {
            Some(root) => Ok(root),
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("invalid writable root page `{}`", self.writable_root),
            )),
        }
    }
}

// The only reason we need `unsafe impl Pod for RootPage` is because `RootPage`
// contains a non-Pod `[u8; PAGE_SIZE - (2 * core::mem::size_of::<PageId>())]`
// field. The reason this array is not pod is only because it has a size out
// of the standard. On macOS with M-type processors it is 16k.
//
// <https://docs.rs/bytemuck/latest/src/bytemuck/pod.rs.html#92-97>
//
// NOTE: no need? derive works?
// unsafe impl Pod for RootPage {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let temp = tempfile::tempdir().unwrap();
        let env = Environment::new(temp.path().join("data.ddb").to_path_buf()).unwrap();

        let wtxn = env.write_txn().unwrap();
        wtxn.commit().unwrap();
    }
}
