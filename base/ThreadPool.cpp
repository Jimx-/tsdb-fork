#include "ThreadPool.hpp"
#include <boost/bind.hpp>
#include <exception>
// #include <iostream>

namespace tsdb{
namespace base{

ThreadPool::ThreadPool(const std::string& nameArg)
  : mutex_(),
    notEmpty_(mutex_),
    notFull_(mutex_),
    name_(nameArg),
    maxQueueSize_(8),
    running_(false)
{}

ThreadPool::~ThreadPool(){
    // LOG_DEBUG << "~ThreadPool() starts";
    if(running_){
        stop();
    }
    // LOG_DEBUG << "~ThreadPool() quits";
}

void ThreadPool::start(int numThreads){
    assert(threads_.empty());
    running_ = true;
    threads_.reserve(numThreads);
    for (int i = 0; i < numThreads; ++i){
        char id[32];
        snprintf(id, sizeof id, "%d", i+1);
        threads_.push_back(new Thread(boost::bind(&ThreadPool::runInThread, this), name_+id));
        threads_[i].start();
    }
    if (numThreads == 0 && threadInitCallback_){
        threadInitCallback_();
    }
}

void ThreadPool::stop(){
    {
        MutexLockGuard lock(mutex_);
        running_ = false;
        notEmpty_.notifyAll();
    }
    for(boost::ptr_vector<Thread>::iterator i = threads_.begin(); i != threads_.end(); ++i){
        (*i).join();
    }
}

void ThreadPool::run(const Task& f){
    // LOG_DEBUG << "add task";
    if(running_ && maxQueueSize_ > 0){    
        if(threads_.empty()){
            f();
        }
        else{
            MutexLockGuard lock(mutex_);
            while(queue_.size() >= maxQueueSize_){
                notFull_.wait();
            }
            queue_.push_back(f);
            // std::cout << "push\n";
            notEmpty_.notify();
        }
    }
}

ThreadPool::Task ThreadPool::take(){
    Task task;
    if(running_ && maxQueueSize_ > 0){
        MutexLockGuard lock(mutex_);
        while(queue_.empty() && running_){ //一定要有running_条件，否则若调用stop会死循环
            notEmpty_.wait();
        }
        if(!queue_.empty()){
            task = queue_.front();
            queue_.pop_front();
            // std::cout << "pop\n";
            notFull_.notify();
        }
    }
    return task;
}

void ThreadPool::runInThread(){
    try{
        if(threadInitCallback_){
            threadInitCallback_();
        }
        while(running_){
            Task task = take();
            if(task){
                // LOG_DEBUG << "take task";
                task();
            }
            // std::cout << queueSize() << std::endl;
        }
    }
    catch(const std::exception& ex){
        fprintf(stderr, "exception caught in ThreadPool %s\n", name_.c_str());
        fprintf(stderr, "reason: %s\n", ex.what());
        abort();
    }
    catch(...){
        fprintf(stderr, "unknown exception caught in ThreadPool %s\n", name_.c_str());
        throw; // rethrow
    }
}

}}