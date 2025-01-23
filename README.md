# Parallel LZ4
[![godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/prequel-dev/plz4)
[![license](http://img.shields.io/badge/license-BSD--2-red.svg?style=flat)](https://raw.githubusercontent.com/prequel-dev/plz4/main/LICENSE)
[![Build Status](https://github.com/prequel-dev/plz4/actions/workflows/test.yml/badge.svg)](https://github.com/prequel-dev/plz4/actions/workflows/test.yml)
[![Go Coverage](https://github.com/prequel-dev/plz4/wiki/coverage.svg)](https://raw.githack.com/wiki/prequel-dev/plz4/coverage.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/prequel-dev/plz4)](https://goreportcard.com/report/github.com/prequel-dev/plz4)
[![GitHub Tag](https://img.shields.io/github/tag/prequel-dev/plz4.svg?style=social)](https://github.com/prequel-dev/plz4/tags)

The plz4 package provides a fast and simple golang library to encode and decode the [LZ4 Frame Format](./docs/lz4_Frame_Format.md) in parallel.  

In addition, it provides the [plz4](./cmd/plz4) command line tool to generate and decode LZ4.


## Features

The primary goal of the plz4 project is performance, speed in particular. Multi-core machines are now commonplace, and LZ4's independent block mode is well suited to fully take advantage of multiple cores with some [caveats](#caveats).

This project attempts to support all of the features enumerated in the [LZ4 Frame Format ](./docs/lz4_Frame_Format.md) specification.  In addition to the baseline features such as checksums and variable compression levels, the library supports the following;

- User-provided [dictionary](./docs/lz4_Frame_Format.md?plain=1#L220) to improve compression ratio
- [Linked blocks](./docs/lz4_Frame_Format.md?plain=1#L154) (ie. non-independent) to improve compression ratio
- [Skippable frame](./docs/lz4_Frame_Format.md?plain=1#L308) support
- [Frame concatenation](./docs/lz4_Frame_Format.md?plain=1#L106) support
- User-specified parallelism and optional worker pool
- Progress callbacks
- [Sparse](./pkg/sparse) write support
- Random read access (see [caveats](#random-read-access))



## Design

The library is written in [Go](https://go.dev/), which provides a fantastic framework for parallel execution.  For maximum feature compatibility, the underlying engine leverages the canonical [lz4 library](https://github.com/lz4/lz4) via CGO.  As an alternative; there is an excellent [pure golang](https://github.com/pierrec/lz4) implementation by Pierre Curto.

The library runs in two modes; either synchronous or asynchronous mode.  The asynchronous mode executes the encoding/decoding work in one or more goroutines to parallelize.  In both modes, a memory pool is employed to minimize data allocations on the heap.  Data blocks are reused from the memory pool when possible.  On account of the minimal heap allocations, plz4 puts little pressure on the heap.  As such, it performs well as a compression engine for long-running processes such as servers.

There is an inherent tradeoff between speed and memory in the LZ4 design.  LZ4 compresses best with large blocks and as such the 4 Mib block size is the default.  However, the more work done in parallel increases the amount of instantaneous RAM used.   For example, a compression job using 8 cores and 4 MiB blocks could use upwards of 32 Mibs at one time (more than that when you consider both read and write blocks).  A compression job using 8 cores and 64 KiB blocks would use much less, upwards of 512Kib.

To manage this tradeoff, there are a few knobs:

- When compressing, tune the block size given the environment.
- For each job, the maximum number of goroutines may be specified.  This, coupled with the block size, will limit overall RAM usage.
- There is additionally an option to provide a user-specified WorkerPool.  The advantage here is that the overall number of cores is limited without having to manage the maximum parallel count on each job.


## Caveats

### Linked Blocks

While LZ4 Frames using independent blocks parallelizes well, the linked blocks feature does not.  This is because each block is dependent on the data from the previous block.  While plz4 can compress linked frames in parallel, it cannot decompress in parallel because of this dependency.

### Content Checksum

There is another LZ4 Frame feature that is problematic at scale.  By default, plz4 enables the content checksum feature, as recommended in the spec.   This feature uses a 32-bit checksum to validate that the content stream produced during decompression has the same checksum as the original source.  Because the checksum must be calculated in serial, a decompression job running highly parallel may fall behind during this calculation.  To improve parallel throughput, disabled the content checksum feature on decompress.

### Random read access

Another advantage of independent blocks is the potential to support random read access.  This is possible because each block can be independently decompressed.  To support this, plz4 provides an optional progress callback that emits both the source offset and corresponding block offset during compression.  An implementation can use this information to build lookup tables that can later be used to skip ahead during decompression to a known block offset.  plz4 provides the 'WithReadOffset' option on the NewReader API to skip ahead and start decompression at a known block offset.


## Install

To install the 'plz4' command line tool use the following command:

```
go install github.com/prequel-dev/plz4/cmd/plz4
```

Use the 'bakeoff' command to determine whether your particular payload performs better using plz4 or the native go [implementation](https://github.com/pierrec/lz4).  The two implementations differ on the relation between compression level and output size.


