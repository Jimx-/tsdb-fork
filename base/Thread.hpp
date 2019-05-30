#ifndef THREAD_H
#define THREAD_H
#include "Atomic.hpp"
#include "CountDownLatch.hpp"

#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <pthread.h>
#include "Logging.hpp"
#include <string>

namespace tsdb{
namespace base{

class Thread : boost::noncopyable
{
    public:
        typedef boost::function<void ()> ThreadFunc;

        explicit Thread(const ThreadFunc& func, const std::string& name = std::string());
        
        ~Thread();

        void start();

        // return pthread_join()
        int join();

        bool started() const { return started_; }
        pthread_t pthreadId() const { return pthreadId_; }
        // pid_t tid() const { return tid_; }
        const std::string& name() const { return name_; }

        static int numCreated() { return numCreated_.get(); }

    private:
        void setDefaultName();

        bool       started_;
        bool       joined_;
        pthread_t  pthreadId_;
        // pid_t      tid_;
        ThreadFunc func_;
        std::string     name_;
        CountDownLatch latch_;

        // shared by all threads
        static AtomicInt numCreated_;
};

}}
#endif