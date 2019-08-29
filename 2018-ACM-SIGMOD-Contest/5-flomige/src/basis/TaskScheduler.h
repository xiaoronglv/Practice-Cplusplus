// SIGMOD Programming Contest 2018 Submission
// Copyright (C) 2018  Florian Wolf, Michael Brendle, Georgios Psaropoulos
//
// This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with this program; if not, see
// <http://www.gnu.org/licenses/>.

#ifndef TASK_SCHEDULER_H
#define TASK_SCHEDULER_H

#include "Numa.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <array>

namespace basis {

    enum TaskState : char {
        TASK_WAITING_FOR_EXECUTION,
        TASK_IN_EXECUTION,
        TASK_EXECUTED,
        TASK_FAILED,
        TASK_ABORTED
    };

    enum TaskGroupState {
        TASK_GROUP_WAITING_FOR_EXECUTION,
        TASK_GROUP_IN_EXECUTION,
    };

    enum TaskPriority {
        HIGH_TASK_PRIO=0,
        MEDIUM_TASK_PRIO=1,
        OLAP_TASK_PRIO=2,
    };

    // forward declaration of classes that will be friend classes of 'TaskBase' class
    class TaskGroup;
    class TaskQueues;
    class Worker;
    class TaskSchedulerTests;

    class TaskBase : public NumaAllocated {

        // classes that can call 'setInExecutionIfWaiting' and 'setExecuted'
        friend class TaskGroup;
        friend class AsyncTaskGroup;
        friend class TaskQueues;
        friend class Worker;
        friend class TaskSchedulerTests; //TODO

        private:

            //std::mutex _stateMutex;  // wrap it in shared pointer in case want to move Task objects, e.g. in vector
            std::atomic<TaskState> _state = TASK_WAITING_FOR_EXECUTION;
            std::string _failureMessage;

            TaskPriority _priority;
            uint32_t _priorityPartition;
            uint32_t _numaNode;

            // just checks if task is in execution, called by friend task group wait
            bool isWaitingForExecution() const;

            // under lock sets state to 'TASK_IN_EXECUTION' if class is waiting, called by friends task group wait or task queues get task
            bool setInExecutionIfWaiting();

            // just checks if task is in execution, called by friend task group wait
            bool isInExecution() const;

            // just sets this task as executed under a lock, called by called by friends task group wait or worker execute
            void setExecuted();

            // invoked when exception is thrown during task execution, just sets failed state, called by friends task group wait or worker execute
            void setFailed(std::exception& exception);

            // sets this task as aborted when it is currently waiting, called by friend task group wait
            void setAborted();

        public:
            TaskBase(TaskPriority priority, uint32_t numaNode);

            virtual ~TaskBase();

            virtual void execute() = 0;

            // supposed to be called in a wait loop to check if this task is already finished which means that the task is either in
            // 'TASK_EXECUTED', 'TASK_FAILED', 'TASK_ABORTED' state, and not in 'TASK_WAITING_FOR_EXECUTION', or 'TASK_IN_EXECUTION' state
            bool isAlreadyFinished() const;

            // supposed to be called after 'isAlreadyFinished()' to see if the task finished successfully which means that task in 'TASK_EXECUTED' state
            // calling this in a wait could result in an infinite loop when the task failed or was aborted
            bool isExecuted() const;

            // just checks if task is failed
            bool isFailed() const;

            // just checks if task is failed
            bool isAborted() const;

            const std::string& getFailureMessage() const;

            TaskPriority getPriority() const;

            uint32_t getPriorityPartition() const;

            uint32_t getNumaNode() const;

    };


    class AsyncTaskGroup : public std::enable_shared_from_this<AsyncTaskGroup> {

        private:
            AsyncTaskGroup();

            // apart from proccessing group tasks, the observer task aborts waiting group tasks when we it detects a failed task;
            // furthermore it invokes 'startExecution()' of the next task group when all group tasks have finished
            class ObserverTask : public TaskBase {
                private:
                    AsyncTaskGroup& _taskGroup;

                public:
                    ObserverTask(AsyncTaskGroup& taskGroup);

                    void execute();
            };

            //std::mutex _stateMutex;  // wrap it in shared pointer in case want to move task group objects, e.g. in vector
            std::atomic<TaskGroupState> _state = TASK_GROUP_WAITING_FOR_EXECUTION;

            std::shared_ptr<AsyncTaskGroup> _prevTaskGroup = nullptr; // TODO should be a raw pointer ;)
            std::shared_ptr<AsyncTaskGroup> _nextTaskGroup = nullptr;

            std::vector<std::shared_ptr<TaskBase>> _groupTasks;

            std::shared_ptr<ObserverTask> _observerTask;

            void setPrevTaskGroup(std::shared_ptr<AsyncTaskGroup> prevTaskGroup);

        public:
            // since we are using 'shared_from_this()' to chain  async task groups, it is essential to only heap allocate async task groups
            static std::shared_ptr<AsyncTaskGroup> createInstance(){
                return std::shared_ptr<AsyncTaskGroup>(new AsyncTaskGroup());
            }

            // invoked to add a task to this task group
            void addTask(std::shared_ptr<TaskBase> task);

            // start the execution for all tasks in this async task group, when all tasks are finished 'startExecution' for the optional '_nextTaskGroup' is invoked
            // keep in mind that this has to be only invoked for the first async task group in an chain of async task groups
            void startExecution();

            // after the execution is started, this is invoked to wait for all tasks in this group to finish, if there is a '_prevTaskGroup' 'wait' recursively invoked there
            // keep in mind that this has to be only invoked for the last async task group in an chain of async task groups
            void wait();

            // set the task group that is automatically started when this task group is finished, setting a next async task group is optional
            void setNextTaskGroup(std::shared_ptr<AsyncTaskGroup> nextTaskGroup);

    };


    // just a wrapper for 'AsyncTaskGroup'
    // TODO could it be removed?
    class TaskGroup {

        private:
            std::shared_ptr<AsyncTaskGroup> _asyncTaskGroup;

        public:
            TaskGroup();

            // invoked to add a task to this task group
            void addTask(std::shared_ptr<TaskBase> task);

            // starts the execution of all task in this group and returns when all tasks are executed
            void execute();

    };


    class TaskQueues {

        public:
            static const uint32_t PARTITIONS_PER_NUMA = 16;

        private:
            std::array<std::array<std::mutex, PARTITIONS_PER_NUMA>, NUMA_NODE_COUNT> _queueMutexes;
            std::array<std::array<std::queue<std::shared_ptr<TaskBase>>, PARTITIONS_PER_NUMA>, NUMA_NODE_COUNT> _queues;

        public:

            TaskQueues();

            void push(std::shared_ptr<TaskBase> task);

            std::shared_ptr<TaskBase> getTask(uint32_t numaNode);

    };


    class Worker {

        public:
            struct WorkerThreadParameters {
                uint32_t _numaNode;
                uint32_t _workerId;
                TaskQueues& _queues;
                WorkerThreadParameters(uint32_t numaNode, uint32_t workerId, TaskQueues& queues);
            };

            static bool _run;

        private:
            // thread local storage variables
            static thread_local uint32_t _numaNode;
            static thread_local uint32_t _workerId;
            static thread_local bool _isWorkerContext;

        public:
            static void* run(void* param);

            static bool isWorkerContext();

            static uint32_t getNumaNode();

            static uint32_t getId();
    };


    class TaskScheduler {

        private:
            static TaskQueues _taskQueues;
            static std::vector<pthread_t> _threads;
            static std::vector<uint32_t> _workersNumaNode;  // _workersNumaNode[workerId]
            static std::mutex _initializationMutex;
            static bool _initialized;

            static void initialize();

        public:
            static uint32_t getWorkerCount();

            static uint32_t getWorkersNumaNode(uint32_t workerId);

            static void submitTask(std::shared_ptr<TaskBase> task);
    };

}

#endif
