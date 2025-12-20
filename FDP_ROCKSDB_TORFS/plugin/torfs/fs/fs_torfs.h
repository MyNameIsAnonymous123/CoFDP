/* TorFS: RocksDB Storage Backend for FDP SSDs
 *
 * Copyright 2024 Samsung Electronics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#pragma once
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "io_torfs.h"
#include "meta_torfs.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#define IOSTATS_SET_DISABLE(disable) (disable)


#include "rocksdb/perf_level.h"
#include "rocksdb/system_clock.h"
// #include "rocksdb/env.h"
#define FDP_DEBUG 0
#define FDP_DEBUG_R (FDP_DEBUG && 1)  /* Read */
#define FDP_DEBUG_W (FDP_DEBUG && 1)  /* Write and Sync */
#define FDP_DEBUG_AF (FDP_DEBUG && 1) /* Append and Flush */
#define FDP_META_DEBUG 0              /* MetaData Flush and Recover */
#define FDP_META_ENABLED 0            /* Enable MetaData 0:Close   1:Open */

#define FDP_PREFETCH 0
#define FDP_PREFETCH_BUF_SZ (1024 * 1024 * 1) /* 1MB */
#define MAX_PID 8
#define MAX_META_SEGMENTS 2
#define NR_DEALLOCATE_THREADS 8
#define FILE_METADATA_BUF_SIZE (1024 * 1024 * 1024)
#define SMALL_DEAL (64 * 1024)

#define FDP_RU_SIZE 13079937024ul
#define META_SEGMENT_CAPACITY (1 * 1024 * 1024 * 1024ul)

// sungjin
#define STREAM_OPTION 0

#define NOSTREAM 0
#define WRITE_LIFE_TIME_HINT 1
#define LEVEL_SEPARATION 2
#define PROPOSAL 3
#define FDP_SSD 4

namespace rocksdb {
class FDPWritableFile;
class TorFS;

struct Segment {
  uint64_t offset;
  uint64_t size;
  uint32_t padding;

  Segment() {
    offset = 0;
    size = 0;
    padding = 0;
  }

  friend bool operator<(Segment s1, Segment s2) {
    return s1.offset > s2.offset;
  }
};

class FDPFile {
 public:
  std::string name_;
  size_t size_;
  uint64_t uuididx_;
  int level_;
  bool is_sst_=false;
  int pid_=0;
  std::vector<Segment> file_segments_;
  uint32_t cur_seg_idx_;
  std::shared_ptr<Logger> logger_;
  char* wcache_;
  char* cache_offset_;

  std::mutex file_mutex_;
  std::mutex writer_mtx_;
  std::atomic<int> readers_{0};
  std::shared_ptr<IOInterface> io_interface_;
  FDPFile(const std::string& fname, int lvl, int placementId,
          std::shared_ptr<IOInterface> IOInterface,
          std::shared_ptr<Logger> logger, bool enable_buf);
  ~FDPFile();
  uint32_t GetFileMetaLen();
  uint32_t WriteMetaToBuf(unsigned char* buf, bool append = false);
};

class TorFS : public FileSystemWrapper {
 public:
  uint64_t torfs_options_[50];
  port::Mutex file_mutex_;
  std::unordered_map<std::string, FDPFile*> files_;
  std::shared_ptr<Logger> logger_;
  uint64_t sequence_;
  std::string fs_path_;
  port::Mutex multi_instances_mutex_;
  bool env_enabled;
  uint64_t uuididx_;
  unsigned char* meta_buf_;
  port::Mutex seg_queue_mutex_[MAX_PID];
  std::queue<Segment> seg_queue_[MAX_PID];
  uint64_t seg_queue_used_cap_[MAX_PID];
  port::Mutex off_mutex_;
  uint64_t dev_offset_;
  std::priority_queue<Segment> all_seg_queue_;
  uint64_t avail_capacity_;
  uint64_t max_capacity_;
  uint64_t block_size_;
  MetaSegment meta_segments_[MAX_META_SEGMENTS];
  uint8_t cur_segment_;

  std::vector<uint64_t> pid_info_;

  port::Mutex meta_mutex_;
  std::unique_ptr<std::thread> deallocate_worker_[NR_DEALLOCATE_THREADS] = {
      nullptr};
  std::priority_queue<Segment> deallocate_queue_;
  port::Mutex deallocate_mutex_;
  std::shared_ptr<IOInterface> io_interface_;
  std::atomic<uint64_t> cache_read_hit_size_{0};
  std::atomic<uint64_t> cache_read_size_{0};
  
  explicit TorFS(std::shared_ptr<FileSystem> aux_fs, const std::string& option,
                 std::shared_ptr<Logger> logger);
  virtual ~TorFS();
  std::string ToAuxPath(std::string path);
  IOStatus FlushTotalMetaData();
  IOStatus FlushAppendMetaData(FDPFile* pfile);
  IOStatus FlushDelMetaData(const std::string& fileName);
  IOStatus FlushRenameMetaData(const std::string& srcName,
                               const std::string& destName);

  std::string FormatPathLexically(std::filesystem::path filepath);
  void RecoverFileFromBuf(unsigned char* buf, uint32_t& praseLen, bool replace);
  void SwitchMetaSegment(uint64_t start);
  IOStatus LoadMetaData();
  void SetString(std::string aux) { fs_path_ = aux; }
  void ClearMetaData();
  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;
  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;
  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;
  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* size, IODebugContext* dbg) override;
  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options, uint64_t* mtime,
                                   IODebugContext* dbg) override;
  IOStatus RenameFile(const std::string& src, const std::string& dst,
                      const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("Truncate is not implemented in TorFS");
  }

  IOStatus LinkFile(const std::string& /* file*/, const std::string& /*link*/,
                    const IOOptions& /* options*/,
                    IODebugContext* /* dbg*/) override {
    return IOStatus::NotSupported("LinkFile is not implemented in TorFS");
  }

  static uint64_t gettid();
  IOStatus NewDirectory(const std::string& fname, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;
  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus GetChildren(const std::string& path, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;
  IOStatus CreateDir(const std::string& fname, const IOOptions& options,
                     IODebugContext* dbg) override;
  IOStatus CreateDirIfMissing(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  IOStatus DeleteDir(const std::string& fname, const IOOptions& options,
                     IODebugContext* dbg) override;
  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;
  IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override;
  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;
  const char* Name() const override;
  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override;
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override;
  void ReclaimSeg(const Segment& seg);
  IOStatus AllocCap(uint64_t size, std::vector<Segment>& segs);
  void SubmitDeallocate(const Segment& seg);
  void UpdateSegments();
  void SetTorFSOption(std::vector<uint64_t> torfsoption) override{
    torfs_options_[STREAM_OPTION]=torfsoption[STREAM_OPTION];
  }
  uint64_t GetTorFSOption(int option_index){
    return torfs_options_[option_index];
  }

struct BreakDown{
  std::atomic<uint64_t> count_{0};
  std::atomic<uint64_t> us_{0};

  BreakDown(uint64_t count,uint64_t us){
    count_=count;
    us_=us;
  }
};
void PrintCumulativeBreakDown(){

  if(cache_read_size_.load()){
    printf("CACHE HIT %lu / %lu = %lu",cache_read_hit_size_.load(),cache_read_size_.load(),
     (cache_read_hit_size_.load()*10000)/cache_read_size_.load() );
  }
  for(auto it : breakdown_map){
    std::string name = it.first;
    uint64_t count=it.second->count_;
    // uint64_t ms = it.second->us_/1000;
    // printf("%s : %lu/%lu = %lu\n",name.c_str(),ms,count,ms/count);
    uint64_t us = it.second->us_;
    printf("%s : %lu/%lu = %lu\n",name.c_str(),us,count,us/count);
    
  }
}

  std::map<std::string, BreakDown*> breakdown_map;

void AddBreakDown(std::string name, uint64_t us){
// std::map<std::string, std::pair<std::atomic<uint64_t>,std::atomic<uint64_t>>> breakdown_map;
  if(breakdown_map.find(name)==breakdown_map.end()){
    // breakdown_map[name].first=1;
    // breakdown_map[name].second=us/1000;

    // BreakDown* new_breakdown= new BreakDown();
    // new_breakdown
    breakdown_map[name]=(new BreakDown(1,us));


  }else{
    breakdown_map[name]->count_++;
    breakdown_map[name]->us_+=us;
    //  breakdown_map[name].first++;
    //  breakdown_map[name].second+=us/1000;
  }
}


struct TorFSStopWatch{
  std::string name;
  struct timespec start_timespec, end_timespec;
  TorFS* fs_=nullptr;
  TorFSStopWatch(const char* _name, TorFS* fs){
    name=_name;
    fs_=fs;
    clock_gettime(CLOCK_MONOTONIC, &start_timespec);
  }

  // uint64_t RecordTickNS();
  // double RecordTickMS();
  ~TorFSStopWatch(){
      clock_gettime(CLOCK_MONOTONIC, &end_timespec);
       long elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
    if(fs_){
       fs_->AddBreakDown(name,elapsed_ns_timespec/1000);
    }
  }
  // {
  //   clock_gettime(CLOCK_MONOTONIC, &end_timespec);
  //   long elapsed_ns_timespec = (end_timespec.tv_sec - start_timespec.tv_sec) * 1000000000 + (end_timespec.tv_nsec - start_timespec.tv_nsec);
  //   printf("\t\t\t\t\t%s breakdown %lu (ms)\n",name.c_str(),(elapsed_ns_timespec/1000)/1000);
  // }
};

 private:
  bool run_deallocate_worker_ = false;
  void DeallocateWorker();
};

class FDPSequentialFile : public FSSequentialFile {
 private:
  std::string filename_;
  bool use_direct_io_;
  size_t logical_sector_size_;
  FDPFile* fdp_file_;
  TorFS* fs_tor_;
  uint64_t read_off_;

 public:
  FDPSequentialFile(const std::string& fname, TorFS* torFS,
                    const FileOptions& options);
  ~FDPSequentialFile();
  /* ### Implemented at env_fdp_io.cc ### */
  IOStatus ReadOffset(uint64_t offset, size_t n, Slice* result, char* scratch,
                      size_t* read_len) const;

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;

  IOStatus Skip(uint64_t n) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  bool use_direct_io() const override;
  size_t GetRequiredBufferAlignment() const override;
};

class FDPRandomAccessFile : public FSRandomAccessFile {
 private:
  std::string filename_;
  bool use_direct_io_;
  size_t logical_sector_size_;
  uint64_t uuididx_;

  TorFS* fs_tor_;
  FDPFile* fdp_file_;
#if FDP_PREFETCH
  char* prefetch_;
  size_t prefetch_sz_;
  uint64_t prefetch_off_;
  std::atomic_flag prefetch_lock_ = ATOMIC_FLAG_INIT;
#endif

 public:
  FDPRandomAccessFile(const std::string& fname, TorFS* torFS,
                      const FileOptions& options)
      : filename_(fname),
        use_direct_io_(options.use_direct_reads),
        logical_sector_size_(torFS->block_size_),
        uuididx_(0),
        fs_tor_(torFS) {
#if FDP_PREFETCH
    prefetch_off_ = 0;
#endif
    fs_tor_->file_mutex_.Lock();
    fdp_file_ = fs_tor_->files_[filename_];
    fs_tor_->file_mutex_.Unlock();

#if FDP_PREFETCH
    prefetch_ = reinterpret_cast<char*>(zrocks_alloc(FDP_PREFETCH_BUF_SZ));
    if (!prefetch_) {
      std::cout << " FDP (alloc prefetch_) error." << std::endl;
      prefetch_ = nullptr;
    }
    prefetch_sz_ = 0;
#endif
  }

  virtual ~FDPRandomAccessFile() {}
  /* ### Implemented at env_fdp_io.cc ### */
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override;

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override;

  size_t GetUniqueId(char* id, size_t max_size) const override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  virtual IOStatus ReadOffset(uint64_t offset, size_t n, Slice* result,
                              char* scratch) const;
  bool use_direct_io() const override;
  size_t GetRequiredBufferAlignment() const override;
};

class FDPWritableFile : public FSWritableFile {
 private:
  const std::string filename_;
  const bool use_direct_io_;
  uint64_t filesize_;

  size_t logical_sector_size_;
  TorFS* fs_tor_;

 public:
  FDPWritableFile(const std::string& fname, TorFS* torFS,
                           const FileOptions& options);

  ~FDPWritableFile();
  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override;

  IOStatus Append(const Slice& data, const IOOptions& opts,
                  const DataVerificationInfo& /* verification_info */,
                  IODebugContext* dbg) override;

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& opts,
                            const DataVerificationInfo& /* verification_info */,
                            IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus DataSync();
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;

  uint64_t GetFileSize(const IOOptions& , IODebugContext*) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;
  size_t GetUniqueId(char* id, size_t max_size) const override;
  bool IsSyncThreadSafe() const override;
  bool use_direct_io() const override;
  size_t GetRequiredBufferAlignment() const override;
  void SungjinSetLevel(int level) override{
    // printf("SungjinSetLevel %d\n",level);
    fdp_file_->level_=level;
    fdp_file_->is_sst_=true;
  }
  FDPFile* fdp_file_;
};

int ParseTorfsOption(std::string option, TorfsConfig* config);

IOStatus NewFDPFS(FileSystem** fs, const std::string& dev_name);


class TorFSLogger : public Logger {
 public:
  TorFSLogger(TorFS* torfs, std::unique_ptr<FSWritableFile> writable_file,
            const std::string& fname, const EnvOptions& options, Env* env,
            InfoLogLevel log_level = InfoLogLevel::ERROR_LEVEL)
        
       : Logger(log_level),
        torfs_(torfs),
        env_(env),
        clock_(env_->GetSystemClock().get()),
        // file_( std::move(writable_file)) ,
        file_(std::unique_ptr<FDPWritableFile>(static_cast<FDPWritableFile*>(writable_file.release()))),

        fname_(fname),
        envoptions_(options),
        last_flush_micros_(0)
        {
            flush_pending_=false;
        }

  ~TorFSLogger() {
    if (!closed_) {
      closed_ = true;
      CloseHelper().PermitUncheckedError();
    }
  }

 private:
  // A guard to prepare file operations, such as mutex and skip
  // I/O context.
  class FileOpGuard {
   public:
    explicit FileOpGuard(TorFSLogger& logger)
        : logger_(logger), prev_perf_level_(GetPerfLevel()) {
      // Preserve iostats not to pollute writes from user writes. We might
      // need a better solution than this.
      SetPerfLevel(PerfLevel::kDisable);
      // IOSTATS_SET_DISABLE(true);
      logger.mutex_.Lock();
    }
    ~FileOpGuard() {
      logger_.mutex_.Unlock();
      // IOSTATS_SET_DISABLE(false);
      SetPerfLevel(prev_perf_level_);
    }

   private:
    TorFSLogger& logger_;
    PerfLevel prev_perf_level_;
  };

  void FlushLocked() {
    mutex_.AssertHeld();
    if (flush_pending_) {
      flush_pending_ = false;
      // file_->Flush();
      // file_->reset_seen_error();
    }
    last_flush_micros_ = clock_->NowMicros();
  }

  void Flush() override {
    // TEST_SYNC_POINT("TorFSLogger::Flush:Begin1");
    // TEST_SYNC_POINT("TorFSLogger::Flush:Begin2");

    FileOpGuard guard(*this);
    FlushLocked();
  }

  Status CloseImpl() override { return CloseHelper(); }

  Status CloseHelper() {
    FileOpGuard guard(*this);
    file_->Close(IOOptions(),nullptr);
    return IOStatus::OK();
    // if (close_status.ok()) {
    //   return close_status;
    // }
    // return Status::IOError("Close of log file failed with error:" +
                          //  (close_status.getState()
                          //       ? std::string(close_status.getState())
                          //       : std::string()));
  }

  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    // IOSTATS_TIMER_GUARD(logger_nanos);

    const uint64_t thread_id = env_->GetThreadID();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 65536;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      // port::TimeVal now_tv;
      // port::GetTimeOfDay(&now_tv, nullptr);
      struct timespec now_tv;
      // get_timespec()
      // timespec_get(&)
      // clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
      clock_gettime(CLOCK_MONOTONIC, &now_tv);
      // const time_t seconds = now_tv.tv_sec;
      struct tm t;
      t.tm_mon=3;
      t.tm_year=2025;
      // port::LocalTimeR(&seconds, &t);
      p += snprintf(p, limit - p, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llu ",
                    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                    t.tm_min, t.tm_sec, static_cast<int>((now_tv.tv_nsec/1000)),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;  // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      {
        FileOpGuard guard(*this);
        // We will ignore any error returned by Append().
        // file_->Append(IOOptions(), Slice(base, p - base)).PermitUncheckedError();
        // PositionedAppend
        file_->Append(Slice(base, p - base),IOOptions(),nullptr);
        // file_->reset_seen_error();
        flush_pending_ = true;
        const uint64_t now_micros = clock_->NowMicros();
        if (now_micros - last_flush_micros_ >= flush_every_seconds_ * 1000000) {
          FlushLocked();
        }
      }
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }

  size_t GetLogFileSize() const override {
    // MutexLock l(&mutex_);
    // return file_->GetFileSize();
    // size_t ret;
    // torfs_->GetFileSize(fname_,IOOptions(),&ret);
    if(file_->fdp_file_)
      return file_->fdp_file_->size_;
    return 0;
  }

 private:
 TorFS* torfs_;
  Env* env_;
  SystemClock* clock_;
  // WritableFileWriter file_;
  std::unique_ptr<FDPWritableFile> file_;
  std::string fname_;
  EnvOptions envoptions_;
  mutable port::Mutex mutex_;  // Mutex to protect the shared variables below.
  const static uint64_t flush_every_seconds_ = 5;
  std::atomic_uint_fast64_t last_flush_micros_;
  std::atomic<bool> flush_pending_;

  // WritableFileWriter file_;
  // Env* env_;
};

}  // namespace rocksdb
