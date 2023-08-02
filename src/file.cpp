#include "file.hpp"
#include "extern/sqlite3.h"

#include <cstdlib>

File::File(const std::string &name) : numReads_(0), numWrites_(0) {
  sqlite3_initialize();
  sqlite3_vfs *vfs = sqlite3_vfs_find(nullptr);
  file_ = (sqlite3_file *)std::malloc(vfs->szOsFile);
  int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
  int rc = vfs->xOpen(vfs, name.c_str(), file_, flags, &flags);
  if (rc != SQLITE_OK) {
    throw std::runtime_error(sqlite3_errstr(rc));
  }
}

void File::read(void *dst, int pageIndex, int numPages) {
  int byteIndex = pageIndex * PAGE_SIZE;
  int numBytes = numPages * PAGE_SIZE;
  int rc = file_->pMethods->xRead(file_, dst, numBytes, byteIndex);
  if (rc != SQLITE_OK) {
    throw std::runtime_error(sqlite3_errstr(rc));
  }

  numReads_ += numPages;
}

void File::write(void *src, int pageIndex, int numPages) {
  int rc;
  for (int pageNumber = 0; pageNumber < numPages; ++pageNumber) {
    rc = file_->pMethods->xWrite(file_, src, PAGE_SIZE, pageIndex * PAGE_SIZE);
    if (rc != SQLITE_OK) {
      throw std::runtime_error(sqlite3_errstr(rc));
    }

    ++pageIndex;
    src = (char *)src + PAGE_SIZE;
  }

  // Turned off sync to avoid blocking to report the flushing to disk... we just keep going
  // turned this off for optimization of algorithm, such that we can find matches quicker
  // of course this can result in the risk of data corruption/loss if machine happens to shut off
  // during the running of the algorithm... use at your own risk

  // rc = file_->pMethods->xSync(file_, SQLITE_SYNC_FULL);
  if (rc != SQLITE_OK) {
    throw std::runtime_error(sqlite3_errstr(rc));
  }

  numWrites_ += numPages;
}

int File::getNumReads() const { return numReads_; }

int File::getNumWrites() const { return numWrites_; }
