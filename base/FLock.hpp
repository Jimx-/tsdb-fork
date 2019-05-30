#ifndef BASE_FLOCK_H
#define BASE_FLOCK_H

#include <cstdio>
#include <sys/file.h>

#include "base/Error.hpp"

namespace tsdb{
namespace base{

// Currently support Unix.
class FLock{
    private:
        FILE * f;
        bool released;
        error::Error err_;

    public:
        FLock(): f(NULL), released(false){}

        FLock(const std::string & filename): f(NULL), released(false){
            if((f = fopen(filename.c_str(), "wb")) == NULL){
                err_.set("cannot open lock file " + filename);
                return;
            }
            if(flock(fileno(f), LOCK_EX | LOCK_NB) != 0){
                err_.set("cannot hold lock");
            }
        }

        void release(){
            if(!released && f != NULL){
                flock(fileno(f), LOCK_UN | LOCK_NB);
                fclose(f);
                released = true;
            }
        }

        error::Error error(){
            return err_;
        }

        ~FLock(){
            release();
        }
};

}
}

#endif