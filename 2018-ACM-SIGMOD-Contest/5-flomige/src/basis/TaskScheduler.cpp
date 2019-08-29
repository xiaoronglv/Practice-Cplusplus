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

#include "TaskScheduler.h"

#include <functional>  // reference_wrapper
#include <chrono>

#include <iostream> // TODO remove

// Defining this results in not catching thrown exceptions during the execution of tasks
#define DEBUG

namespace basis {

    // number generator and uniform distribution to randomly set a task partition (necessary to avoid locks for pushing taks into only one queue)
    static thread_local std::mt19937 TASK_RANDOM_GENERATOR(5465465+std::hash<std::thread::id>{}(std::this_thread::get_id()));
    static std::uniform_int_distribution<uint32_t> TASK_UNIFORM_DISTRIBUTION(0, TaskQueues::PARTITIONS_PER_NUMA-1);

    //
    // TaskBase
    //

    TaskBase::TaskBase(TaskPriority priority, uint32_t numaNode)
    : _priority(priority), _priorityPartition(TASK_UNIFORM_DISTRIBUTION(TASK_RANDOM_GENERATOR)), _numaNode(numaNode){
    }


    TaskBase::~TaskBase(){
    }


    bool TaskBase::isWaitingForExecution() const {
        return _state.load()==TASK_WAITING_FOR_EXECUTION;
    }


    bool TaskBase::setInExecutionIfWaiting(){
        auto expected_val = TASK_WAITING_FOR_EXECUTION;
        return _state.compare_exchange_strong(expected_val, TASK_IN_EXECUTION);
    }


    bool TaskBase::isInExecution() const {
        return _state.load()==TASK_IN_EXECUTION;
    }


    void TaskBase::setExecuted(){
        if(_state.exchange(TASK_EXECUTED)== TASK_EXECUTED)
            throw std::runtime_error("Executed an already executed task again");
    }


    void TaskBase::setFailed(std::exception& exception){
        // set the failure mesage using the exception, and set the state under a lock
        _failureMessage = std::string(exception.what());
        _state.store(TASK_FAILED);
    }


    void TaskBase::setAborted(){
        // set the task as aborted but only when it is waiting, all under a lock
        auto expected_val = TASK_WAITING_FOR_EXECUTION;
        _state.compare_exchange_strong(expected_val, TASK_ABORTED);
    }


    bool TaskBase::isAlreadyFinished() const {
        auto state = _state.load();
        return state!=TASK_WAITING_FOR_EXECUTION && state!=TASK_IN_EXECUTION;
    }


    bool TaskBase::isExecuted() const {
        return _state.load()==TASK_EXECUTED;
    }


    bool TaskBase::isFailed() const {
        return _state.load()==TASK_FAILED;
    }


    bool TaskBase::isAborted() const {
        return _state.load()==TASK_ABORTED;
    }


    const std::string& TaskBase::getFailureMessage() const {
        return _failureMessage;
    }


    TaskPriority TaskBase::getPriority() const {
        return _priority;
    }


    uint32_t TaskBase::getPriorityPartition() const {
        return _priorityPartition;
    }


    uint32_t TaskBase::getNumaNode() const {
        return _numaNode;
    }


    //
    // TaskGroup
    //

    TaskGroup::TaskGroup() : _asyncTaskGroup(AsyncTaskGroup::createInstance()){
    }

    void TaskGroup::addTask(std::shared_ptr<TaskBase> task){
        _asyncTaskGroup->addTask(task);
    }


    void TaskGroup::execute(){
        _asyncTaskGroup->startExecution();
        _asyncTaskGroup->wait();
    }


    //
    // AsyncTaskGroup::ObserverTask
    //

    AsyncTaskGroup::ObserverTask::ObserverTask(AsyncTaskGroup& taskGroup)
    : TaskBase(HIGH_TASK_PRIO, Worker::getNumaNode()), _taskGroup(taskGroup){
    }


    void AsyncTaskGroup::ObserverTask::execute(){
        // surveil the tasks' state and execute some, while not all tasks of this group finished
        bool allTasksFinished;
        bool allLocalNodeTasksScheduled;
        do{
            allTasksFinished=true;
            allLocalNodeTasksScheduled=true;
            // run over all tasks
            for(std::shared_ptr<TaskBase>& task : _taskGroup._groupTasks){
                // if we find a task that is waiting for execution
                if(task->isWaitingForExecution()){
                    allTasksFinished=false;
                    if(task->setInExecutionIfWaiting()){
                        allLocalNodeTasksScheduled=false;
                        // execute that task, catch exception on execute and set different failed task state
                        #ifdef DEBUG
                            task->execute();
                            task->setExecuted();
                        #else
                            try{
                                task->execute();
                                task->setExecuted();
                            }
                            catch(std::exception& e){
                                task->setFailed(e);
                            }
                        #endif
                    }
                }
                else if(task->isInExecution()){
                    allTasksFinished=false;
                }
                // if we find a task that failed
                else if(task->isFailed()){
                    // abort all tasks of this group to avoid that they are scheduled, this could have already done by the task group's wait call
                    for(std::shared_ptr<TaskBase>& taskToAbort : _taskGroup._groupTasks){
                        taskToAbort->setAborted();
                    }
                    // throw an execption to cause the oberver task's state become failed
                    throw std::runtime_error(std::string("Task failed: "+task->getFailureMessage()));
                }
            }
            if(allLocalNodeTasksScheduled){
                // sleep a bit, we could also process some non local tasks at this point
				std::this_thread::sleep_for(std::chrono::microseconds(500));
            }
        }while(!allTasksFinished);

        // check if there is a next task group that we can execute
        if(_taskGroup._nextTaskGroup != nullptr){
            _taskGroup._nextTaskGroup->startExecution();
        }

    }


    //
    // AsyncTaskGroup
    //

    void AsyncTaskGroup::setPrevTaskGroup(std::shared_ptr<AsyncTaskGroup> prevTaskGroup){
        // ensure that the previous task is not yet set
        if(_prevTaskGroup != nullptr){
            throw std::runtime_error("previous task group already set");
        }
        _prevTaskGroup = prevTaskGroup;
    }


    AsyncTaskGroup::AsyncTaskGroup()
    : _observerTask(new(Worker::getNumaNode()) ObserverTask(*this)){
    }


    void AsyncTaskGroup::addTask(std::shared_ptr<TaskBase> task){
        // just push the tast to the task vector member
        _groupTasks.push_back(task);
    }


    void AsyncTaskGroup::startExecution(){
        // ensure that we are in a worker context
        if(!Worker::isWorkerContext()){
            throw std::runtime_error("Executed task group outside of a task context");
        }
        // set task group state -> in execution
        auto expected_val = TASK_GROUP_WAITING_FOR_EXECUTION;
        if(!_state.compare_exchange_strong(expected_val, TASK_GROUP_IN_EXECUTION)){
            throw std::runtime_error("Tried to execute a task group that is already in execution or executed");
        }
        // submit the observer task to the scheduler
        TaskScheduler::submitTask(std::dynamic_pointer_cast<TaskBase>(_observerTask));
        // add all local tasks to the task scheduler
        for(std::shared_ptr<TaskBase>& task : _groupTasks){
            TaskScheduler::submitTask(task);
        }
    }


    void AsyncTaskGroup::wait(){
        // ensure that we are in a worker context
        if(!Worker::isWorkerContext()){
            throw std::runtime_error("Executed task group outside of a task context");
        }
        // wait for previous task to finish
        if(_prevTaskGroup != nullptr){
            // wait for the previous task group
            _prevTaskGroup->wait();
            // set the pointer to the previous task group to nullptr since we do not need it anymore
            // very important to avoid cyclic references of shared pointers, i.e., memory leaks
            _prevTaskGroup = nullptr;
        }
        // ensure that execution of task group was already started
        if(_state.load()==TASK_GROUP_WAITING_FOR_EXECUTION){
            throw std::runtime_error("Tried to wait for a task group that was not yet started");
        }
        // try to execute observer task in this context in case it is still waiting for execution
        if(_observerTask->setInExecutionIfWaiting()){
            #ifdef DEBUG
                _observerTask->execute();
                _observerTask->setExecuted();
            #else
                try{
                    _observerTask->execute();
                    _observerTask->setExecuted();
                }
                catch(std::exception& e){
                    _observerTask->setFailed(e);
                }
            #endif
        }
        // wait for observer task to finish if it's in execution, process group task in the meanwhile
        else if(_observerTask->isInExecution()){
            bool allLocalNodeTasksScheduled=true;
            while(_observerTask->isInExecution()){
                // run over all tasks
                allLocalNodeTasksScheduled=true;
                for(std::shared_ptr<TaskBase>& task : _groupTasks){
                    // if we find a task that is waiting for execution
                    if(task->isWaitingForExecution()){
                        // try to steel it
                        if(task->setInExecutionIfWaiting()){
                            allLocalNodeTasksScheduled=false;
                            // execute that task, catch exception on execute and set different failed task state
                            #ifdef DEBUG
                                task->execute();
                                task->setExecuted();
                            #else
                                try{
                                    task->execute();
                                    task->setExecuted();
                                }
                                catch(std::exception& e){
                                    task->setFailed(e);
                                }
                            #endif
                        }
                    }
                    // if we find a task that failed
                    else if(task->isFailed()){
                        // abort all tasks of this group to avoid that they are scheduled, this could have already done by the observer task
                        for(std::shared_ptr<TaskBase>& task2 : _groupTasks){
                            task2->setAborted();
                        }
                        // let this wait call fail with an exception that contains the tasks failure message
                        throw std::runtime_error(std::string("Task failed: "+task->getFailureMessage()));
                    }
                }
                if(allLocalNodeTasksScheduled){
                    // sleep a bit, we could also process some non local tasks at this point
					std::this_thread::sleep_for(std::chrono::microseconds(20));
                }
            }
        }
        // now that '_observerTask' is finished, check its state
        // no else if !!!
        if(_observerTask->isExecuted()){
            return;
        }
        else if(_observerTask->isFailed()){
            throw std::runtime_error(std::string("Group oberserver task failed: "+_observerTask->getFailureMessage()));
        }
        else if(_observerTask->isAborted()){
            throw std::runtime_error("Group observer task aborted, this should never happen");
        }
        else{
            throw std::runtime_error("Detected unexpected group observer task state, this should never happen");
        }
    }


    void AsyncTaskGroup::setNextTaskGroup(std::shared_ptr<AsyncTaskGroup> nextTaskGroup){
        // ensure that the next task group is not yet set
        if(_nextTaskGroup == nullptr){
            // set this task group as previous task group in the 'nextTaskGroup'
            nextTaskGroup->setPrevTaskGroup(shared_from_this());
            _nextTaskGroup = nextTaskGroup;
        }
        else {
            throw std::runtime_error("Next task group already set");
        }
    }


    //
    // TaskQueues
    //

    TaskQueues::TaskQueues() {
    }


    void TaskQueues::push(std::shared_ptr<TaskBase> task){
        // note that the priority is currently not considered for optimization reasons
        std::unique_lock<std::mutex> uLock(_queueMutexes[task->getNumaNode()][task->getPriorityPartition()]);
        _queues[task->getNumaNode()][task->getPriorityPartition()].push(task);
    }


    std::shared_ptr<TaskBase> TaskQueues::getTask(uint32_t numaNode){
        // variables to track success of pops, and avoid spinning on empty queues
        uint32_t emptyQueueCounter = 0;
        // run until we find a task that we can return
        while(true){
            // start for looking a task on the current numa node
            uint32_t currentNuma = numaNode;
            // iterate over all numa nodes
            for (uint32_t i = 0; i < NUMA_NODE_COUNT; ++i) {
                // start for looking on a random partition
                uint32_t startPartition = TASK_UNIFORM_DISTRIBUTION(TASK_RANDOM_GENERATOR);
                // iterate over all partitions
                for (uint32_t j = 0; j < TaskQueues::PARTITIONS_PER_NUMA; ++j) {
                    // try to lock the queue
                    std::unique_lock<std::mutex> uLock(_queueMutexes[currentNuma][startPartition], std::defer_lock);
                    if(uLock.try_lock()){
                        // while the queue is not empty
                        while(!_queues[currentNuma][startPartition].empty()){
                            // pop a task
                            std::shared_ptr<TaskBase> task = _queues[currentNuma][startPartition].front();
                            _queues[currentNuma][startPartition].pop();
                            // check if it is waiting so that we can return it, maybe it was already stolen e.g. by the master thread of a task group
                            if(task->setInExecutionIfWaiting()){
                                return task;
                            }
                        }
                        // if queue is empty: release uLock
                        uLock.unlock();
                    }
                    // queue is locked or empty, update the empty queue counter
                    emptyQueueCounter++;
                    // increment partition counter to the next partition
                    startPartition = (startPartition + 1) % TaskQueues::PARTITIONS_PER_NUMA;
                }
                // increment numa counter to the next numa node
                currentNuma = (currentNuma + 1) % NUMA_NODE_COUNT;
            }
            // check if we may have to sleep a bit, because the queues are empty
            if(emptyQueueCounter>PARTITIONS_PER_NUMA * NUMA_NODE_COUNT){
                // sleep for some microseconds and reset counter
                // usleep(20);
                std::this_thread::sleep_for(std::chrono::microseconds(20));
                emptyQueueCounter=0;
            }
        }
    }


    //
    // WorkerThreadParameters
    //

    Worker::WorkerThreadParameters::WorkerThreadParameters(uint32_t numaNode, uint32_t workerId, TaskQueues& queues)
    : _numaNode(numaNode), _workerId(workerId), _queues(queues){
    }


    //
    // Worker
    //

    bool Worker::_run = true;

    thread_local uint32_t Worker::_numaNode;

    thread_local uint32_t Worker::_workerId;

    thread_local bool Worker::_isWorkerContext=false;


    void* Worker::run(void* param){
        //cast thread parameters
        WorkerThreadParameters* parameters = (WorkerThreadParameters*)param;

        // set TLS variables for this thread
        _isWorkerContext = true;
        _numaNode = parameters->_numaNode;
        _workerId = parameters->_workerId;

        // bind execution to cpu (core) or to node (socket)?
        //  - bind to socket -> more robust in case we have concurrent processes
        //  - binding to cpus would avoid potential unnecessary context switches, (numa_node_to_cpus,  sched_setaffinity/pthread_setaffinity_np
        // bind memory allocation to the same node?
        //  - yes, bind to node
        //  - only disadvantage if process is forced to execute on node 0 and allocate mem only from node 1
        // see manual: numa_bind()
        //
        // binding memory allocation might conflict with 'numa_alloc_onnode()' using a different node, but the documentation says
        // that it will only fail when the node is 'externally denied'
        basis::NumaInterface::bindCurrentContextToNode(parameters->_numaNode);

        // run forever, get tasks and execute them
        while(_run){
            // 'getTask' blocks if queues are empty
            std::shared_ptr<TaskBase> task = parameters->_queues.getTask(parameters->_numaNode);
            // catch exception on execute and set different failed task state
            #ifdef DEBUG
                task->execute();
                task->setExecuted();
            #else
                try{
                    task->execute();
                    task->setExecuted();
                }
                catch(std::exception& e){
                    task->setFailed(e);
                }
            #endif
        }
        return NULL;
    }


    bool Worker::isWorkerContext(){
        return _isWorkerContext;
    }


    uint32_t Worker::getNumaNode(){
        if(!_isWorkerContext){
            throw std::runtime_error("Called getter for numa node number out of task context");
        }
        return _numaNode;
    }


    uint32_t Worker::getId(){
        if(!_isWorkerContext){
            throw std::runtime_error("Called getter for worker id out of task context");
        }
        return _workerId;
    }


    //
    // TaskScheduler
    //

    TaskQueues TaskScheduler::_taskQueues;

    std::vector<pthread_t> TaskScheduler::_threads;

    std::vector<uint32_t> TaskScheduler::_workersNumaNode;

    std::mutex TaskScheduler::_initializationMutex;

    bool TaskScheduler::_initialized=false;


    void TaskScheduler::initialize(){
        // under the assumption that the numa configuration does not change during the process runtime, e.g. we run in a VM

        // ensure that we initialize only once
        std::unique_lock<std::mutex> uLock(_initializationMutex);
        if(!_initialized){
            // basically create as much threads on a node as we have configured on that node. bind them to the node, not to a specific cpu which
            // allows them to be reschduled inside the node

            // run over all numa nodes
            for(uint32_t numaNode=0; numaNode < basis::NUMA_NODE_COUNT; numaNode++){

                // for each cpu on a socket
                for(uint32_t cpu=0; cpu<basis::CORES_PER_NUMA; cpu++){
                    // get new a worker id
                    uint32_t workerId = _threads.size();
                    // emplace a thread handle
                    _workersNumaNode.push_back(numaNode);
                    // create the worker thread parameters
                    Worker::WorkerThreadParameters* param = new Worker::WorkerThreadParameters(numaNode, workerId, _taskQueues);
                    // finally create the worker thread
                    _threads.emplace_back();
                    pthread_create(&_threads[cpu], NULL, Worker::run, param);
                    //_threads.emplace_back(Worker::run, param);
                }

            }
            _initialized=true;
        }
    }


    uint32_t TaskScheduler::getWorkerCount(){
        if(!_initialized){
            initialize();
        }
        return _threads.size();
    }


    uint32_t TaskScheduler::getWorkersNumaNode(uint32_t workerId){
        if(!_initialized){
            initialize();
        }
        return _workersNumaNode[workerId];
    }


    void TaskScheduler::submitTask(std::shared_ptr<TaskBase> task){
        if(!_initialized){
            initialize();
        }
        _taskQueues.push(task);
    }

}
