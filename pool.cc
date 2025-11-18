#include "pool.h"
#include <mutex>
#include <iostream>

Task::Task() = default;
Task::~Task() = default;

ThreadPool::ThreadPool(int num_threads) {
    if (num_threads <= 0) num_threads = 1;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(new std::thread(&ThreadPool::run_thread, this));
    }
}

ThreadPool::~ThreadPool() {
    for (std::thread *t: threads) {
        delete t;
    }
    threads.clear();

    for (Task *q: queue) {
        delete q;
    }
    queue.clear();

    for (auto const& [name, cv_ptr] : task_cv) {
        delete cv_ptr;
    }
    task_cv.clear();
}

void ThreadPool::SubmitTask(const std::string &name, Task *task) {
    {
        std::lock_guard<std::mutex> lg(mtx);

        if (done) {
            std::cout << "Cannot added task to queue" << std::endl;
            return;
        }

        queue.push_back(task);

        task->name = name;
        task_done[name] = false;
        task_cv[name] = new std::condition_variable();

        std::cout << "Added task" << std::endl;
    }
    cv.notify_one();
}

void ThreadPool::run_thread() {
    while (true) {
        Task* task = nullptr;
        std::string task_name;

        {
            std::unique_lock<std::mutex> ul(mtx);

            cv.wait(ul, [this]{ return done || !queue.empty(); });

            if (done && queue.empty()) {
                std::cout << "Stopping thread" << std::endl;
                return; 
            }

            task = queue.front();
            queue.erase(queue.begin());
            task_name = task->name;
            tasks_running++;
            std::cout << "Started task" << std::endl;
        }

        try {
            task->Run();
            std::cout << "Finished task" << std::endl;
        } catch (...) {}

        {
            std::lock_guard<std::mutex> lg(mtx);
            tasks_running--;
            task_done[task_name] = true;
            task_cv[task_name]->notify_all();

            cv.notify_all();
        }

        delete task;
    } 
}

// Remove Task t from queue if it's there
void ThreadPool::remove_task(Task *t) {
    /*
    mtx.lock();
    for (auto it = queue.begin(); it != queue.end();) {
        if (*it == t) {
            queue.erase(it);
            mtx.unlock();
            return;
        }
        ++it;
    }
    mtx.unlock();
    */
}

void ThreadPool::Stop() {
    std::cout << "Called Stop()" << std::endl;
    {
        std::lock_guard<std::mutex> lg(mtx);
        done = true;
    }

    cv.notify_all();
    
    for (auto* t : threads) {
        if (t && t->joinable()) t->join();
    }
}

void ThreadPool::WaitForTask(const std::string &name) {
    std::unique_lock<std::mutex> ul(mtx);

    task_cv[name]->wait(ul, [&]{ return task_done[name]; });

    delete task_cv[name];
    task_cv.erase(name);
    task_done.erase(name);
}

