# dalloc

A disk allocator that uses memory mapping as a page cache and pwrite to write to disk. All read and write accesses use transactions. It doesn't support concurrent read and write operations yet. Do not use this in production for now; wait for an announcement blog post.
