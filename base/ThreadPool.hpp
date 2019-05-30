#ifndef THREADPOOL_H
#define THREADPOOL_H
#include "Mutex.hpp"
#include "Condition.hpp"
#include "Thread.hpp"
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <deque>

namespace tsdb{
namespace base{

class ThreadPool : boost::noncopyable{
    public:
        typedef boost::function<void ()> Task;

        explicit ThreadPool(const std::string& nameArg = std::string("ThreadPool"));
        ~ThreadPool();

        // Must be called before start().
        void setMaxQueueSize(int maxSize) { maxQueueSize_ = maxSize; }
        void setThreadInitCallback(const Task& cb){ threadInitCallback_ = cb; }

        void start(int numThreads);
        void stop();

        const std::string& name() const{ return name_; }

        size_t queueSize() const{
            MutexLockGuard lock(mutex_);
            return queue_.size();
        }

        // void getQueueSize(){
        //     MutexLockGuard lock(mutex_);
        //     qSize = queue_.size();
        // }
        // volatile size_t qSize;
        // Could block if maxQueueSize > 0
        void run(const Task& f);
        // #ifdef __GXX_EXPERIMENTAL_CXX0X__
        //   void run(Task&& f);
        // #endif

    private:
        // bool isFull() const;
        void runInThread();
        Task take();

        mutable MutexLock mutex_;
        Condition notEmpty_;
        Condition notFull_;
        std::string name_;
        Task threadInitCallback_;
        boost::ptr_vector<Thread> threads_;
        std::deque<Task> queue_;
        size_t maxQueueSize_;
        bool running_;
};

}}

#endif