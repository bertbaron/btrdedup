# BTRFS Deduplication tool

Deduplication tool like [bedup](https://github.com/g2p/bedup). I wrote it quite some time ago
 already because bedup had problems with my volume and the number of snapshots (crashes, database corruption etc.)

Btrdedup uses much less resources especially in case of many snapshots. The limitation is that it only deduplicates
 files that start with the same content. By inspecting the fragmentation before offering the files for deduplication
 to the kernel (using the btrfs deduplication ioctl) data that is already shared will not be deduplicated again.

Btrdedup does not maintain state between runs. This makes it less suitable for incremental deduplication. On the other
 hand it makes the tool very robust and because of its efficiency in detecting already deduplicated files it can easily
 be scheduled to run once a month for example.

# Snapshot-aware deduplication

Since version 0.2.0 there is an option to defragment files before deduplication. This acts like a snapshot-aware
 defragmentation. The system defragmentation tool will be used to defragement one of the copies after which the
 dedpuclication process will deduplicate all copies to the defragmentated one.

# Installation

Download the latest release:

![GitHub release](https://img.shields.io/github/release/bertbaron/btrdedup.svg)

Make executable using: ```chmod +x btrdedup```

# Usage

Typically you want to run the program as root on the complete mounted btrfs pool with a command like this:

```shell
nice -n 10 ./btrdedup /mnt 2>dedup.log
```

or in the background

```shell
nice -n 10 ./btrdedup /mnt >dedup.out 2>dedup.log &
```

It is also possible to specify specific files or folders for deduplication and defragmentation. Make sure that all
potential copies are on the given paths because the deduplication process might actually break reflinks to copies
that aren't seen.

This is an example where certain folders and snapshots are deduplicated and defragmented, excluding files smaller
 than 256 blocks (1MB):

```shell
./btrdedup -minsize 256 -defrag /data/media /snapshots/data*/media 2>dedup.log
```

The scanning phase may still take a long time depending on the number of files. The -minsize option may help a lot
 when there are many small files for which deduplication will not help much. The most expensive part however,
 the deduplication itself, is only called when necessary.
 
Btrfdedup is very memory efficient and doesn't require a database. It can be instructed to use even less memory
 by providing the `-lowmem` option. This may require a few more minutes, but it may also be faster because of reduced
 memory management. Future versions might default to this option.

Use ```btrdedup -h``` for the full list of options.

# Under the hood

Btrdedup works by first reading the file tree(s) in memory in an efficient data structure. It then processes these
 files in three passes:
  
 * Pass 1: Read the fragmentation table for each file.

   Sort the result on the offset of the first block

 * Pass 2: Calculate the hash of the first block of each file. Because the files are sorted on the first block
   offset, any block is only loaded and hashed once.
   
   Sort the result on the hash of the first block 

 * Pass 3: Files that have the first block in common are offered for deduplication. The deduplication phase will
   first check if blocks are already shared to only offer data for actual deduplication if necessary. 

In lowmem mode, the output of each pass is written to an encoded temporary text file which is then sorted using the
 systems `sort` tool.

# Future improvements

The last pass still needs some improvents. Currently files with the same hashcode for the first block are assumed to be
equal to the size of the smallest file. In the future the blocks should be more thoroughly checked for duplicates, by
comparing the hash codes of all blocks.
