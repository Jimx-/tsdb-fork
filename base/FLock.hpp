#ifndef BASE_FLOCK_H
#define BASE_FLOCK_H

#include <cstdio>
#include <sys/file.h>

#include "base/Error.hpp"
#include <cstdio>
#include <unistd.h>

namespace tsdb {
namespace base {

// Currently support Unix.
class FLock {
private:
    int fd;
    bool released;
    error::Error err_;

public:
    FLock() : fd(-1), released(false) {}

    FLock(const std::string& filename) : fd(-1), released(true)
    {
        if ((fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0644)) == -1) {
            err_.set("cannot open lock file " + filename);
            return;
        }
        if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
            err_.set("cannot hold lock");
        }
        released = false;
    }

    void release()
    {
        if (!released && fd != -1) {
            flock(fd, LOCK_UN | LOCK_NB);
            ::close(fd);
            fd = -1;
            released = true;
        }
    }

    error::Error error() { return err_; }

    ~FLock() { release(); }
};

} // namespace base
} // namespace tsdb

#endif
