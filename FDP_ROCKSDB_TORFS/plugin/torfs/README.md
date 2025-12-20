# TorFS: RocksDB Storage Backend for FDP SSDs

TorFS is a plugin that enables RocksDB to access Flexible Data Placement (FDP) SSDs

# Getting started

## Build

Download RocksDB and TorFS:

```shell
git clone git@github.com:facebook/rocksdb.git
cd rocksdb
git clone git@github.com:SamsungDS/TorFS.git plugin/torfs
```

Install and enable TorFS in micro-bench tool of RocksDB

```shell
DEBUG_LEVEL=0 ROCKSDB_PLUGINS=torfs make -j8 db_bench
```



## Testing with db_bench

TorFS supports xNVMe back-end that provides unified interfaces for multiple IO paths.

|           | io_uring_cmd (character device) | io_uring (block device) | libaio (block device) | SPDK |
| :-------: | :-----------------------------: | :---------------------: | :-------------------: | :--: |
| **xNVMe** |                √                |            O            |           O           |  √   |
|  **raw**  |                X                |            X            |           X           |  X   |

- √: Support IO path and data directive			
- O: Support IO path only, not data directive
- X: Not support IO path and data directive

```bash
./db_bench --benchmarks=<IO pattern> --fs_uri=torfs:<backend>:<dev>?be=<IO path>
e.g.
./db_bench --benchmarks=fillseq --fs_uri=torfs:xnvme:/dev/ng0n1?be=io_uring_cmd
```

sudo DEBUG_LEVEL=0 ROCKSDB_PLUGINS=torfs make -j$(nproc) db_bench install



sudo /home/femu/FDP_ROCKSDB/db_bench --benchmarks="fillrandom,stats" -subcompactions=8 -histogram -max_background_compactions=4 -max_background_flushes=4 -num=83886080 -stream_option=0 --fs_uri=torfs:xnvme:/dev/nvme0n1?be=io_uring



sudo /home/femu/FDP_ROCKSDB_TORFS/db_bench --benchmarks="fillrandom,stats" --histogram -subcompactions=8 -max_background_flushes=4 -max_background_compactions=4 -subcompactions=8 -value_size=1024 -num=20971520 -stream_option=2 --fs_uri=torfs:xnvme:/dev/nvme0n1?be=thrpool





<!-- const char *async_str[] = {"thrpool",
                            "libaio",
                            "io_uring",
                            "io_uring_cmd",
                            "nvme"}; -->
