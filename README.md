# BTRFS Deduplication tool

Deduplication tool like [bedup](https://github.com/g2p/bedup). I wrote it quite some time ago
 already because bedup had problems with my volume and the number of snapshots (crashes, database corruption etc.)

Btrdedup uses much less resources especially in case of many snapshots. The limitation is that it only deduplicates
 files that start with the same content. By inspecting the fragmentation before offering the files for deduplication
 to the kernel (using the btrfs deduplication ioctl) data that is already shared will not be deduplicated again.

Btrdedup does not maintain state between runs. This makes it less suitable for incremental deduplication. On the other
 hand it makes the tool very robust and because of its efficiency in detecting already deduplicated files it can easily
 be scheduled to run once a month for example.

# Installation

Download the latest release:

[![release](http://github-release-version.herokuapp.com/github/bertbaron/btrdedup/release.svg)](https://github.com/bertbaron/btrdedup/releases/latest)

Make executable using: ```chmod +x btrdedup```

# Usage

Typically you want to run the program as root on the complete mounted btrfs pool with a command like this:

```shell
nice -n 10 ./btrdedup /mnt 2>dedup.log
```

or

```shell
nice -n 10 ./btrdedup /mnt >dedup.out 2>dedup.log &
```

The scanning phase may still take a long time depending on the number of files. The most expensive part however,
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
  
The last pass is still to be improved. Currently only the prefixes of files are deduplicated. As soon as blocks of files
 differ the deduplication assumes the remainder of the files doesn't share blocks. 
  
In lowmem mode, the output of each pass is written to an encoded temporary text file which is then sorted using the
 systems `sort` tool.