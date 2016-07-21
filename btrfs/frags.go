package btrfs

import "os"

// size 56
type fiemap_extent struct {
	fe_logical    uint64  /* logical offset in bytes for the start of the extent from the beginning of the file */
	fe_physical   uint64  /* physical offset in bytes for the start of the extent from the beginning of the disk */
	fe_length     uint64; /* length in bytes for this extent */
	fe_reserved64 [2]uint64;
	fe_flags      uint32  /* FIEMAP_EXTENT_* flags for this extent */
	fe_reserved   [3]uint32;
}

// size 32
type fiemap struct {
	fm_start          uint64 /* logical offset (inclusive) at which to start mapping (in) */
	fm_length         uint64 /* logical length of mapping which userspace wants (in) */
	fm_flags          uint32 /* FIEMAP_FLAG_* flags for request (in/out) */
	fm_mapped_extents uint32 /* number of extents that were mapped (out) */
	fm_extent_count   uint32 /* size of fm_extents array (in) */
	fm_reserved       uint32
				 //struct fiemap_extent fm_extents[0]; /* array of mapped extents (out) */
}

func filefrag_fiemap(fd uintptr, blk_shift int, num_extents *int, ext2fs_struct_stat *st) int
{
__u64 buf[2048]; /* __u64 for proper field alignment */
struct fiemap *fiemap = (struct fiemap *)buf;
struct fiemap_extent *fm_ext = &fiemap->fm_extents[0];
struct fiemap_extent fm_last = {0};
int count = (sizeof(buf) - sizeof(*fiemap)) /
sizeof(struct fiemap_extent);
unsigned long long expected = 0;
unsigned long long expected_dense = 0;
unsigned long flags = 0;
unsigned int i;
int fiemap_header_printed = 0;
int tot_extents = 0, n = 0;
int last = 0;
int rc;

memset(fiemap, 0, sizeof(struct fiemap));

if (sync_file)
flags |= FIEMAP_FLAG_SYNC;

if (xattr_map)
flags |= FIEMAP_FLAG_XATTR;

do {
fiemap->fm_length = ~0ULL;
fiemap->fm_flags = flags;
fiemap->fm_extent_count = count;
rc = ioctl(fd, FS_IOC_FIEMAP, (unsigned long) fiemap);
if (rc < 0) {
static int fiemap_incompat_printed;

rc = -errno;
if (rc == -EBADR && !fiemap_incompat_printed) {
fprintf(stderr, "FIEMAP failed with unknown "
"flags %x\n",
fiemap->fm_flags);
fiemap_incompat_printed = 1;
}
return rc;
}

/* If 0 extents are returned, then more ioctls are not needed */
if (fiemap->fm_mapped_extents == 0)
break;

if (verbose && !fiemap_header_printed) {
print_extent_header();
fiemap_header_printed = 1;
}

for (i = 0; i < fiemap->fm_mapped_extents; i++) {
expected_dense = fm_last.fe_physical +
fm_last.fe_length;
expected = fm_last.fe_physical +
fm_ext[i].fe_logical - fm_last.fe_logical;
if (fm_ext[i].fe_logical != 0 &&
fm_ext[i].fe_physical != expected &&
fm_ext[i].fe_physical != expected_dense) {
tot_extents++;
} else {
expected = 0;
if (!tot_extents)
tot_extents = 1;
}
if (verbose)
print_extent_info(&fm_ext[i], n, expected,
blk_shift, st);
if (fm_ext[i].fe_flags & FIEMAP_EXTENT_LAST)
last = 1;
fm_last = fm_ext[i];
n++;
}

fiemap->fm_start = (fm_ext[i - 1].fe_logical +
fm_ext[i - 1].fe_length);
} while (last == 0);

*num_extents = tot_extents;

return 0;
}


func Fragments(file os.File) {

}

