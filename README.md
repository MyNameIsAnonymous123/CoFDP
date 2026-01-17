# User-Kernel Cooperative FDP Storage

1. **Currently Under Review**
   - User-Kernel Cooperative FDP Storage

2. **Dependency & Environment**
   - Ubuntu 20.04 (Host, Emulator Both)
   - xNVMe: https://xnvme.io/getting_started/index.html

3. **Source Code Description and Step-by-Step**
   - **linux-fdp**: Modified kernel source code including PiDir-Filesystem (based on F2FS, `fs/f2fs`)
   - **f2fs-tools-fdp**: Modified `mkfs.f2fs` tools (depends on xNVMe)
   - **ConfFDP**: FDP emulator based on FEMU  
     - Reflects NAND latency  
     - PID / RG configurable via `femu-scripts/run-fdp.sh`
     - FEMU official image file (u20s.qcow)[https://github.com/MoatLab/FEMU]
6. **Simple Example Executing CoFDP (Solana Blockchain)**
   - Note that this script is executed in Emulating Environment (/dev/nvme0n1 = Emulated FDP)

   ```bash
   sudo /home/femu/fdp_reset /dev/nvme0n1

   sudo /home/femu/f2fs-tools-fdp/mkfs/mkfs.f2fs -f -O lost_found /dev/nvme0n1
   # -O lost_found enables CoFDP formatting

   sudo /home/femu/f2fs-tools-fdp/fdp_f2fs_mount 8
   # 8 = number of PIDs

   sudo rm -rf /home/femu/tenant0
   sudo rm -rf /home/femu/tenant1
   sudo mkdir -p /home/femu/tenant0
   sudo mkdir -p /home/femu/tenant1

   sudo mkdir -p /home/femu/tenant0/p0 /home/femu/tenant0/p1 /home/femu/tenant0/p2 \
                /home/femu/tenant0/p3 /home/femu/tenant0/p4 /home/femu/tenant0/p5 \
                /home/femu/tenant0/p6 /home/femu/tenant0/p7

   sudo mount --bind /home/femu/f2fs_fdp_mount/p7 /home/femu/tenant0/p7
   sudo mount --bind /home/femu/f2fs_fdp_mount/p6 /home/femu/tenant0/p6
   sudo mount --bind /home/femu/f2fs_fdp_mount/p5 /home/femu/tenant0/p5
   sudo mount --bind /home/femu/f2fs_fdp_mount/p4 /home/femu/tenant0/p4
   sudo mount --bind /home/femu/f2fs_fdp_mount/p3 /home/femu/tenant0/p3
   sudo mount --bind /home/femu/f2fs_fdp_mount/p2 /home/femu/tenant0/p2
   sudo mount --bind /home/femu/f2fs_fdp_mount/p1 /home/femu/tenant0/p1
   sudo mount --bind /home/femu/f2fs_fdp_mount/p0 /home/femu/tenant0/p0

   sudo chmod 777 /home/femu/tenant0
   sudo chmod 777 /home/femu/tenant0/*

   sudo mkdir -p /home/femu/tenant0/p1/snapshot /home/femu/tenant0/p1/accounts \
                /home/femu/tenant0/p1/banking_trace \
                /home/femu/tenant0/p1/accounts_hash_cache \
                /home/femu/tenant0/p1/accounts_index \
                /home/femu/tenant0/p1/rocksdb

   sudo chmod 777 /home/femu/tenant0/p1/*

   sudo mount --bind /home/femu/tenant0/p2 /home/femu/f2fs_fdp_mount/p1/accounts
   sudo mount --bind /home/femu/tenant0/p2 /home/femu/f2fs_fdp_mount/p1/accounts_hash_cache
   sudo mount --bind /home/femu/tenant0/p2 /home/femu/f2fs_fdp_mount/p1/accounts_index
   sudo mount --bind /home/femu/tenant0/p4 /home/femu/f2fs_fdp_mount/p1/rocksdb
   sudo mount --bind /home/femu/tenant0/p5 /home/femu/f2fs_fdp_mount/p1/snapshot
7. **How to measure WA?**
    - After a application benchmark, you should execute sudo ```fdp_reset /dev/nvme0n1```
    - You can check it at ConfFDP/femu-scripts/log, which prints out the ConfFDP printf log.
```
------------------------------------
RG 0 channel 0-7 / lun 0-7
RG 0 STREAM 0 : written : 219842
RG 0 STREAM 1 : written : 9182075
RG 0 STREAM 2 : written : 7034207
RG 0 STREAM 3 : written : 7282002
RG 0 STREAM 4 : written : 9815855
RG 0 STREAM 5 : written : 4020042
RG 0 STREAM 6 : written : 13792495
RG 0 STREAM 7 : written : 8821657
print_sungjin(ssd->sungjin_stat.block_erased) : {13184}
print_sungjin(ssd->sungjin_stat.copied) : {9114394}
print_sungjin(ssd->sungjin_stat.discard) : {48501926}
print_sungjin(ssd->sungjin_stat.discard_ignored) : {16795960}
print_sungjin(ssd->sungjin_stat.invalidated) : {283604}
print_sungjin(sum_written) : {0}
print_sungjin(ssd->sungjin_stat.write_io_n) : {661999}
print_sungjin(ssd->sungjin_stat.read_io_n) : {4810594}
print_sungjin(ssd->sungjin_stat.read_sum) : {27612177}
INITIALIZED current time: Wed Jan 14 14:18:39 2026
```
   - WAF = 1 + (219842+9182075+7034207+7282002+9815855+4020042+137924958821657)/9114394
