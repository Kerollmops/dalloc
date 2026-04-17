use std::ops::Deref;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs::File, io};

use memmap2::Mmap;
use parking_lot::{FairMutex, FairMutexGuard};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

// The page size of the environment, as determined by the build script.
// const PAGE_SIZE: usize = {page size at build time};
include!(concat!(env!("OUT_DIR"), "/page_size.rs"));

pub struct Environment {
    _path: PathBuf,
    file: FairMutex<File>,
    mmap: Mmap,
}

impl Environment {
    // TODO: change this to an Into<PathBuf>
    pub fn new(path: PathBuf, size: u64) -> io::Result<Self> {
        assert_eq!(
            PAGE_SIZE,
            page_size::get(),
            "The runtime page size does not match the build-time page size"
        );

        assert!(size.is_multiple_of(PAGE_SIZE as u64));

        let file = File::create_new(&path)?;
        file.try_lock()?;
        // Note that setting the length bzero the file contents
        file.set_len(size)?;

        // Initialize the root page and write it to the file
        let root_page = RootPage::init();
        let page = Page(PageUnion { root: &root_page });
        file.write_all_at(page.as_raw(), 0)?;

        // unsafe: we know the file is locked and we own it, so we can safely map it.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Environment { _path: path, file: FairMutex::new(file), mmap })
    }

    // TODO introduce a custom error type
    pub fn write_txn<'e>(&'e self) -> io::Result<RwTxn<'e>> {
        let file_lock = self.file.lock();
        let rtxn = RoTxn { env: self };

        let page_id = PageId::root();
        let root_page = unsafe { rtxn.read_page(page_id)? };
        let root_page_copy = root_page.as_root_page().clone();

        Ok(RwTxn { rtxn, root_page_copy, file_lock })
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
    root_page_copy: RootPage,
    file_lock: FairMutexGuard<'e, File>,
}

impl<'e> RwTxn<'e> {
    /// Allocates a new page and returns it's PageId.
    pub fn alloc_page(&mut self) -> io::Result<Option<PageId>> {
        let RootPage { last_page_id, .. } = &mut self.root_page_copy;

        let next_page_id =
            last_page_id.next().ok_or_else(|| io::Error::other("page id overflow"))?;

        if next_page_id.offset() < self.rtxn.env.mmap.len() {
            *last_page_id = next_page_id;
            Ok(Some(next_page_id))
        } else {
            Ok(None)
        }
    }

    pub unsafe fn write_page(&mut self, page_id: PageId, data: Page<'_>) -> io::Result<()> {
        self.file_lock.write_all_at(data.as_raw(), page_id.offset_u64())
    }

    /// Frees a page by its PageId and puts it in the free list.
    pub unsafe fn free_page(&mut self, _page_id: PageId) -> io::Result<()> {
        todo!()
    }

    pub fn set_writable_page_root(&mut self, root_page_id: PageId) {
        let RootPage { writable_root_page, root_alpha, root_beta, .. } = &mut self.root_page_copy;
        match writable_root_page {
            WritableRootPage::Alpha => *root_alpha = root_page_id,
            WritableRootPage::Beta => *root_beta = root_page_id,
        }
    }

    pub fn writable_root_page_id(&self) -> PageId {
        self.root_page_copy.writable_root_page_id()
    }

    pub fn readable_root_page_id(&self) -> PageId {
        self.root_page_copy.readable_root_page_id()
    }

    /// FSync the page changes to disk, swaps the root
    /// and fsync the root changes to make the changes visible.
    pub fn commit(mut self) -> io::Result<()> {
        // TODO should I call sync_all here when I increased the size of the file?
        // Sync the user data that we previously wrote to disk.
        self.file_lock.sync_data()?;

        // Swap
        // safety: We are reading the root page, which is guaranteed to exist.
        self.root_page_copy.swap_writable_root();
        let page_to_write = Page::from_root_page(&self.root_page_copy);
        let root_page_id = PageId::root();
        self.file_lock.write_all_at(page_to_write.as_raw(), root_page_id.offset_u64())?;
        self.file_lock.sync_data()?;

        Ok(())
    }
}

impl<'r> Deref for RwTxn<'r> {
    type Target = RoTxn<'r>;

    fn deref(&self) -> &Self::Target {
        &self.rtxn
    }
}

/// A transaction that can only read pages.
pub struct RoTxn<'e> {
    env: &'e Environment,
}

impl<'e> RoTxn<'e> {
    pub unsafe fn read_page<'m>(&'m self, page_id: PageId) -> io::Result<Page<'m>> {
        Ok(Page(PageUnion {
            raw: self.env.mmap[page_id.offset()..][..PAGE_SIZE].try_into().unwrap(),
        }))
    }
}

#[repr(transparent)]
#[derive(Debug, FromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
pub struct PageId(pub usize);

impl PageId {
    fn offset(self) -> usize {
        self.0 * PAGE_SIZE
    }

    fn offset_u64(self) -> u64 {
        // TODO check only in debug builds
        self.offset().try_into().unwrap()
    }

    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl PageId {
    /// Creates the root page id.
    fn root() -> Self {
        Self(0)
    }
}

#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct Page<'m>(PageUnion<'m>);

#[repr(C)]
#[derive(Copy, Clone)]
pub union PageUnion<'m> {
    raw: &'m [u8; PAGE_SIZE],
    root: &'m RootPage,
}

impl<'m> Page<'m> {
    pub fn from_root_page(root: &'m RootPage) -> Self {
        Self(PageUnion { root })
    }

    fn as_root_page(&self) -> &'m RootPage {
        unsafe { self.0.root }
    }

    fn as_raw(&self) -> &'m [u8; PAGE_SIZE] {
        unsafe { self.0.raw }
    }
}

#[repr(u8)]
#[derive(TryFromBytes, Immutable, Copy, Clone)]
enum WritableRootPage {
    Alpha = 0,
    Beta = 1,
}

#[repr(C)]
#[derive(TryFromBytes, KnownLayout, Immutable, Clone)]
pub struct RootPage {
    last_page_id: PageId,
    writable_root_page: WritableRootPage,
    // TODO remove this padding?
    //      + padding will depend on the alignment of PageId
    _padding: [u8; 7],
    root_alpha: PageId,
    root_beta: PageId,
}

impl RootPage {
    pub fn init() -> Self {
        Self {
            last_page_id: PageId::root(),
            writable_root_page: WritableRootPage::Alpha,
            _padding: [0u8; 7],
            root_alpha: PageId::root(),
            root_beta: PageId::root(),
        }
    }

    fn swap_writable_root(&mut self) {
        self.writable_root_page = match self.writable_root_page {
            WritableRootPage::Alpha => WritableRootPage::Beta,
            WritableRootPage::Beta => WritableRootPage::Alpha,
        };
    }

    fn writable_root_page_id(&self) -> PageId {
        match self.writable_root_page {
            WritableRootPage::Alpha => self.root_alpha,
            WritableRootPage::Beta => self.root_beta,
        }
    }

    fn readable_root_page_id(&self) -> PageId {
        match self.writable_root_page {
            WritableRootPage::Alpha => self.root_beta,
            WritableRootPage::Beta => self.root_alpha,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use zerocopy::IntoBytes;

    use super::*;

    #[test]
    fn it_works() {
        let temp = tempfile::tempdir().unwrap();
        let env =
            Environment::new(temp.path().join("data.ddb").to_path_buf(), PAGE_SIZE as u64 * 30)
                .unwrap();

        let wtxn = env.write_txn().unwrap();
        wtxn.commit().unwrap();
    }

    #[test]
    fn my_first_linked_list() {
        let temp = tempfile::tempdir().unwrap();
        let env =
            Environment::new(temp.path().join("data.ddb").to_path_buf(), PAGE_SIZE as u64 * 20)
                .unwrap();

        #[repr(C)]
        #[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Clone)]
        pub struct Link {
            next: Option<NonZeroUsize>,
            my_number: u32,
            _padding: [u8; 4],
        }

        let mut my_number = 42u32;

        loop {
            let mut wtxn = env.write_txn().unwrap();
            let Some(page_id) = wtxn.alloc_page().unwrap() else { break };
            eprintln!("Allocated {page_id:?}");

            let mut page_buffer = [0u8; PAGE_SIZE];
            let (link, _remaining) = Link::try_mut_from_prefix(&mut page_buffer).unwrap();
            // The first page is considered the root but also None (stop) in the linked list
            link.next = NonZeroUsize::new(wtxn.readable_root_page_id().0);
            eprintln!("Made it point to {:?} and stored {my_number:?} in it", link.next);
            link.my_number = my_number;
            my_number += 1;
            wtxn.set_writable_page_root(page_id);

            let my_page = Page(PageUnion { raw: &page_buffer });
            unsafe { wtxn.write_page(page_id, my_page).unwrap() };
            wtxn.commit().unwrap();
        }

        eprintln!("Last stored number is {:?}", my_number - 1);

        my_number -= 1;

        let rtxn = env.read_txn().unwrap();
        let root_page = unsafe { rtxn.read_page(PageId::root()).unwrap() };
        let mut page_id_to_read = root_page.as_root_page().readable_root_page_id();

        assert_ne!(page_id_to_read.0, 0);

        loop {
            eprintln!("Found {page_id_to_read:?} to read");
            let page = unsafe { rtxn.read_page(page_id_to_read).unwrap() };
            let (link, _remaining) = Link::try_ref_from_prefix(page.as_raw()).unwrap();
            eprintln!("Found {:?} number in it", link.my_number);
            assert_eq!(link.my_number, my_number);

            eprintln!("Next page must be {:?}", link.next);

            match link.next {
                Some(page_id) => page_id_to_read = PageId(page_id.get()),
                None => break,
            }

            my_number -= 1;
        }

        assert_eq!(my_number, 42);
    }
}
