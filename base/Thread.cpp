#include "Thread.hpp"

namespace tsdb{
namespace base{

AtomicInt Thread::numCreated_;

struct ThreadData{
    typedef Thread::ThreadFunc ThreadFunc;
    ThreadFunc func_;
    std::string name_;
    // pid_t* tid_;
    CountDownLatch* latch_;

    ThreadData(const ThreadFunc& func,
         const std::string& name,
         // pid_t* tid,
         CountDownLatch* latch)
    : func_(func),
    name_(name),
    // tid_(tid),
    latch_(latch)
    { }

    void runInThread(){
        // *tid_ = alec::CurrentThread::tid();
        // tid_ = NULL;
        latch_->countDown();
        // latch_ = NULL;

        // alec::CurrentThread::t_threadName = name_.empty() ? "muduoThread" : name_.c_str();
        // ::prctl(PR_SET_NAME, alec::CurrentThread::t_threadName);
        try{
            func_();
            // alec::CurrentThread::t_threadName = "finished";
        }
        // catch (const Exception& ex){
        //     alec::CurrentThread::t_threadName = "crashed";
        //     fprintf(stderr, "exception caught in Thread %s\n", name_.c_str());
        //     fprintf(stderr, "reason: %s\n", ex.what());
        //     fprintf(stderr, "stack trace: %s\n", ex.stackTrace());
        //     abort();
        // }
        catch (const std::exception& ex){
            // alec::CurrentThread::t_threadName = "crashed";
            fprintf(stderr, "exception caught in Thread %s\n", name_.c_str());
            fprintf(stderr, "reason: %s\n", ex.what());
            abort();
        }
        catch (...){
            // alec::CurrentThread::t_threadName = "crashed";
            fprintf(stderr, "unknown exception caught in Thread %s\n", name_.c_str());
            throw; // rethrow
        }
    }
};
    
    void* startThread(void* obj){
        ThreadData* data = static_cast<ThreadData*>(obj);
        data->runInThread();
        delete data;
        return NULL;
    }

    Thread::Thread(const ThreadFunc& func, const std::string& name)
    :   started_(false),
        joined_(false),
        pthreadId_(0),
        func_(func),
        name_(name),
        latch_(1)
    {
        setDefaultName();
    }

    Thread::~Thread(){
        if(started_ && !joined_){
            pthread_detach(pthreadId_);
        }
    }

    void Thread::start(){
        assert(!started_);
        started_ = true;
        ThreadData* data = new ThreadData(func_, name_, &latch_);
        if (pthread_create(&pthreadId_, NULL, &startThread, data)){ //失败返回-1
            started_ = false;
            delete data; // or no delete?
            LOG_SYSFATAL << "Failed in pthread_create";
        }
        else{ //成功返回0
            // 不会死锁因为假如先在pthred_create中countDown，count_会变为0，wait不会等待
            // threadFunc开始执行后start才返回
            latch_.wait();
            // assert(tid_ > 0);
        }

    }

    int Thread::join(){
        assert(started_);
        assert(!joined_);
        joined_ = true;
        return pthread_join(pthreadId_, NULL);
    }

    void Thread::setDefaultName(){
        int num = numCreated_.incrementAndGet();
        if(name_.empty()){
            char temp[80];
            sprintf(temp, "Thread%d", num);
            name_ = temp;
        }
    }

}}