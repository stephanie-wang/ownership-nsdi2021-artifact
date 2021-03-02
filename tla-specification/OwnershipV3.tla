---------------------------- MODULE OwnershipV3 ----------------------------
EXTENDS FiniteSets, Integers, Sequences, TLC

CONSTANTS MAX_CHILDREN, MAX_CALL_DEPTH, MAX_WORKERS

VARIABLES nextWorkerId, workerIds, taskTable, objectStore, schedulerInbox, taskInbox

vars == <<nextWorkerId, workerIds, taskTable, objectStore, schedulerInbox, taskInbox>>

----------------------------------------------------------------------------

RECURSIVE SeqFromSet(_)
SeqFromSet(S) ==
    IF S = {} THEN <<>> ELSE
    LET x == CHOOSE x \in S : TRUE IN <<x>> \o SeqFromSet(S \ {x})

TASK_ID_SEP == 100
WORKER_ID_SEP == 1000
Task == 0..(TASK_ID_SEP ^ MAX_CALL_DEPTH)
Globalize(scope, x) == scope * TASK_ID_SEP + x

\* Program Return Values

WorkerID == Nat
PENDING == -1
COLLECTED == -2
INLINE_VALUE == -WORKER_ID_SEP
FutureValue == WorkerID \union {PENDING, COLLECTED, INLINE_VALUE}
IsValueReady(x) == x \notin {PENDING, COLLECTED}
IsValueWorkerID(x) == x \in WorkerID

\* Program Steps

Operation == {"CALL", "GET", "DELETE", "RETURN", "TERMINATED"}
CALL(x, args) == <<"CALL", x, args>>
GET(x) == <<"GET", x>>
DELETE(x) == <<"DELETE", x>>
RETURN == <<"RETURN">>
TERMINATED == <<"TERMINATED">>

Instruction == Operation \X Seq(Task)

TaskSpec == [
    owner: Task,
    future: Task,
    args: SUBSET Task
]

FutureState == [
    inScope: BOOLEAN,
    valueGotten: BOOLEAN,
    value: FutureValue,
    workerId: WorkerID,
    taskSpec: TaskSpec
]

TaskState == [
    owner: Task,
    workerId: WorkerID,
    pendingArgs: SUBSET Task,
    executedSteps: Seq(Instruction),
    nextSteps: Instruction,
    nextChildId: Task,
    children: [SUBSET Task -> FutureState]
]

IsReady(taskState) == Cardinality(taskState.pendingArgs) = 0

IsIdle(taskState) == Len(taskState.nextSteps) = 0

IsTerminated(taskState) ==
    /\ Len(taskState.nextSteps) > 0
    /\ taskState.nextSteps[1][1] = "TERMINATED"

\* Messages that a Task Inbox can receive

WorkerFailedMessages == {"FAILED"} \X WorkerID
TaskReturnedMessages == {"RETURNED"} \X Task \X WorkerID
TaskScheduledMessages == {"SCHEDULED"} \X TaskSpec \X WorkerID
Messages == UNION { WorkerFailedMessages, TaskReturnedMessages, TaskScheduledMessages }

\* Type Invariants

\* Ownership Table: [Task -> TaskState]
TaskTableTypeOK ==
    \A t \in DOMAIN taskTable :
    /\ t \in Task
\*    /\ taskTable[t] \in TaskState

ObjectStoreTypeOK ==
    \A w \in DOMAIN objectStore :
    /\ w \in workerIds
    /\ objectStore[w] \in Task

SchedulerInboxTypeOK ==
    \* Scheduler's Inbox is a sequence of TaskSpec.
    schedulerInbox \in Seq(TaskSpec)

\* Each owner's inbox is a sequence of Messages.
TaskInboxTypeOK ==
    \A t \in DOMAIN taskInbox :
    /\ t \in Task
    /\ taskInbox[t] \in Seq(Messages)
 
TypeOK ==
    /\ TaskTableTypeOK
    /\ ObjectStoreTypeOK
    /\ SchedulerInboxTypeOK
    /\ TaskInboxTypeOK


(***************************************************************************
  The initial state contains one function: "main".
 ***************************************************************************)

NewTaskState(owner, workerId, args) ==
    [owner |-> owner, workerId |-> workerId, pendingArgs |-> args, executedSteps |-> <<>>, nextSteps |-> <<>>, nextChildId |-> 1, children |-> <<>>]

Init ==
    /\ nextWorkerId = WORKER_ID_SEP
    /\ workerIds = {0}
    /\ taskTable = [x \in {0} |-> NewTaskState(0, 0, {})]
    /\ objectStore = <<>>
    /\ schedulerInbox = <<>>
    /\ taskInbox = [x \in {0} |-> <<>>]

SendMessage(inbox, receiver, msg) ==
    IF receiver \notin DOMAIN inbox THEN inbox ELSE
    [taskInbox EXCEPT ![receiver] = Append(inbox[receiver], msg)]

(***************************************************************************
    Executing Program Steps
 ***************************************************************************)

\* Pop the current instruction from nextSteps and append it to executedSteps.
FinishExecuting(taskState) ==
    LET nextSteps == taskState.nextSteps IN
    LET inst == Head(nextSteps) IN
    LET executedSteps_== Append(taskState.executedSteps, inst) IN
    [[taskState EXCEPT !.executedSteps = executedSteps_] EXCEPT !.nextSteps = Tail(nextSteps)]

\* Call a function, creating a new future and adding it to the ownership table.
\* Then send the task to the scheduler's queue for scheduling.
\* (Task Creation Step 1)
CallAndSchedule(scope, taskState, inst) ==
    LET x == Globalize(scope, inst[2]) IN
    LET args == inst[3] IN
    LET task == [owner |-> scope, future |-> x, args |-> args] IN
    /\ x \notin DOMAIN taskTable
    /\ x \notin DOMAIN taskTable[scope].children
    /\ schedulerInbox' = Append(schedulerInbox, task)
    /\ taskTable' =
       LET child == [inScope |-> TRUE, valueGotten |-> FALSE, value |-> PENDING, workerId |-> PENDING, taskSpec |-> task] IN
       LET children_ == [y \in {x} |-> child] @@ taskState.children IN
       LET taskState_ == [FinishExecuting(taskState) EXCEPT !.children = children_] IN
       [taskTable EXCEPT ![scope] = taskState_]
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, taskInbox>>

\* Wait on a future for its task to finish, and get its value.
Get(scope, taskState, inst) ==
    LET x == inst[2] IN
    LET entry == taskState.children[x] IN
    /\ entry.inScope
    /\ ~entry.valueGotten
    /\ IsValueReady(entry.value)
    /\ taskTable' =
        LET futureState_ == [entry EXCEPT !.valueGotten = TRUE] IN
        LET taskState_ == [FinishExecuting(taskState) EXCEPT !.children[x] = futureState_] IN
        [taskTable EXCEPT ![scope] = taskState_]
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox, taskInbox>>

\* Remove a future from current scope. This does not remove its task spec.
Delete(scope, taskState, inst) ==
    LET x == inst[2] IN
    /\ taskTable' =
        LET futureState_ == [taskTable[scope].children[x] EXCEPT !.inScope = FALSE] IN
        LET taskState_ == [FinishExecuting(taskState) EXCEPT !.children[x] = futureState_] IN
        [taskTable EXCEPT ![scope] = taskState_]
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox, taskInbox>>

\* Finish execution. Return the value to its owner.
\* Set all of its children's "inScope" flags to FALSE.
Return(scope, taskState) ==
    \* The return value could either be a literal, or stored in the worker's object store.
    \E retVal \in {INLINE_VALUE, taskState.workerId} :
    LET owner == taskTable[scope].owner IN
    /\ taskTable' = 
        LET children == taskState.children IN
        LET children_ == [c \in DOMAIN children |-> [children[c] EXCEPT !.inScope = FALSE]] IN
        [y \in {scope} |->
            [[FinishExecuting(taskState)
                EXCEPT !.nextSteps = <<TERMINATED>>]
                EXCEPT !.children = children_]
        ]
        @@ taskTable
    /\ objectStore' =
        IF retVal = INLINE_VALUE THEN objectStore ELSE
        [y \in {retVal} |-> scope] @@ objectStore
    /\ taskInbox' =
        \* (owner = scope) is a special case for the main function (0).
        IF owner \notin DOMAIN taskTable \/ owner = scope THEN taskInbox ELSE
        LET msg == <<"RETURNED", scope, taskState.workerId>> IN
        SendMessage(taskInbox, owner, msg)
    /\ UNCHANGED <<nextWorkerId, workerIds, schedulerInbox>>

ExecuteProgramStep(scope, taskState) ==
    LET inst == taskState.nextSteps[1] IN
    LET op == Head(inst) IN
    CASE op = "CALL" -> CallAndSchedule(scope, taskState, inst)
      [] op = "GET" -> Get(scope, taskState, inst)
      [] op = "DELETE" -> Delete(scope, taskState, inst)
      [] op = "RETURN" -> Return(scope, taskState)
      [] OTHER -> FALSE

ExecuteSomeProgramStep ==
    \E scope \in DOMAIN taskTable :
    LET taskState == taskTable[scope] IN
    /\ ~IsTerminated(taskState)
    /\ ~IsIdle(taskState)
    /\ ExecuteProgramStep(scope, taskState)

(***************************************************************************
    Submitting Program Steps
 ***************************************************************************)

\* Generate all possible instructions given a task state.
PossibleNextSteps(scope, taskState) ==
    LET children == DOMAIN taskState.children IN
    LET inScopes == {x \in children : taskState.children[x].inScope} IN
    UNION {
        IF
        \/ taskState.nextChildId >= MAX_CHILDREN
        \/ scope >= TASK_ID_SEP ^ (MAX_CALL_DEPTH - 1)
        THEN {} ELSE
        LET x == taskState.nextChildId IN
        \* CALL with any set of arguments.
        { CALL(x, args) : args \in SUBSET inScopes },
        \* GET anything that has not been gotten yet.
        { GET(x) : x \in {x \in inScopes : ~taskState.children[x].valueGotten} },
        \* DELETE anything that is in scope.
        { DELETE(x) : x \in inScopes },
        \* RETURN.
        { RETURN }
    }

\* Submit a program step according to the current task state.
\* Prerequisite: no arguments are pending for this task.
SubmitProgramStep(scope, taskState) ==
    \E inst \in PossibleNextSteps(scope, taskState) :
    /\ taskTable' = 
       LET taskState_ == IF inst[1] = "CALL"
         THEN [taskState EXCEPT !.nextChildId = taskState.nextChildId + 1]
         ELSE taskState IN
       LET nextSteps_ == Append(taskState.nextSteps, inst) IN
       [y \in {scope} |-> [taskState_ EXCEPT !.nextSteps = nextSteps_]]
       @@ taskTable
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox, taskInbox>>

SubmitSomeProgramStep ==
    \E scope \in DOMAIN taskTable :
    LET taskState == taskTable[scope] IN
    /\ IsReady(taskState)
    /\ IsIdle(taskState)
    /\ SubmitProgramStep(scope, taskState)

\* Resolve a pending argument for a task by looking it up from the object store.
ResolvePendingArg(scope, arg) ==
    LET taskState == taskTable[scope] IN
    LET owner == taskState.owner IN
    /\ /\ owner \in DOMAIN taskTable
       /\ arg \in DOMAIN taskTable[owner].children
       /\ IsValueReady(taskTable[owner].children[arg].value)
    /\ taskTable' =
        [y \in {scope} |-> [taskState EXCEPT !.pendingArgs = taskState.pendingArgs \ {arg}]]
        @@ taskTable
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox, taskInbox>>

ResolveSomePendingArg ==
    \E scope \in DOMAIN taskTable :
    \E arg \in taskTable[scope].pendingArgs :
    ResolvePendingArg(scope, arg)

ProgramStep ==
    \/ ExecuteSomeProgramStep
    \/ SubmitSomeProgramStep
    \/ ResolveSomePendingArg

(***************************************************************************
    System Steps: Scheduler Actions
 ***************************************************************************)

\* Task Creation Step 2:
\* Scheduler takes a task from its queue and schedule it.
\* In the real system this involves allocating a worker location to the task.
\* In this spec it is simply sending the owner a message "SCHEDULED".
ScheduleTask ==
    /\ Len(schedulerInbox) > 0
    /\ schedulerInbox' = Tail(schedulerInbox)
    /\ LET task == Head(schedulerInbox) IN
       LET owner == task.owner IN
       LET msg == <<"SCHEDULED", task, nextWorkerId>> IN
       taskInbox' = SendMessage(taskInbox, owner, msg)
    /\ workerIds' = workerIds \union {nextWorkerId}
    /\ nextWorkerId' = nextWorkerId + WORKER_ID_SEP
    /\ UNCHANGED <<taskTable, objectStore>>

(***************************************************************************
    System Steps: Garbage Collection
 ***************************************************************************)

\* Collect the task state when it is terminated and when its children is empty.
CollectTaskState(scope) ==
    LET taskState == taskTable[scope] IN
    /\ IsTerminated(taskState)
    /\ Cardinality(DOMAIN taskState.children) = 0
    /\ taskTable' =
       [y \in DOMAIN taskTable \ {scope} |-> taskTable[y]]
    /\ taskInbox' =
       [y \in DOMAIN taskInbox \ {scope} |-> taskInbox[y]]
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox>>

CollectSomeTaskState ==
    \E x \in DOMAIN taskTable : CollectTaskState(x)

\* Decides if no one depends on x's value or x's lineage.
OutOfLineageScope(scope, x) ==
    LET taskState == taskTable[scope] IN
    LET children == taskState.children IN
    ~(
        \* Do not collect lineage of a task that hasn't returned yet.
        \/ children[x].value = PENDING
        \/ children[x].inScope
        \/ \E y \in DOMAIN children :
           LET futureState == children[y] IN
           /\ x \in futureState.taskSpec.args
           /\ futureState.value \in ({PENDING, COLLECTED} \union WorkerID)
    )

\* Remove x's entry from its owner's task table entry.
CollectLineage(scope, x) ==
    /\ OutOfLineageScope(scope, x)
    /\ taskTable' =
       LET taskState == taskTable[scope] IN
       LET children == taskState.children IN
       LET children_ == [y \in DOMAIN children \ {x} |-> children[y]] IN
       LET taskState_ == [taskState EXCEPT !.children = children_] IN
       [y \in {scope} |-> taskState_] @@ taskTable
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox, taskInbox>>

CollectSomeLineage ==
    \E scope \in DOMAIN taskTable :
    \E x \in DOMAIN taskTable[scope].children :
    CollectLineage(scope, x)

\* Checks if no one depends on x's value.
OutOfScope(scope, x) == 
    LET taskState == taskTable[scope] IN
    LET children == taskState.children IN
    ~(
        \/ children[x].inScope
        \/ \E y \in DOMAIN children :
           LET futureState == children[y] IN
           /\ futureState.value = PENDING
           /\ x \in futureState.taskSpec.args
    )

\* Garbage-collect x's value from the ownership table if no other future depends on it.
CollectValue(scope, x) ==
    LET taskState == taskTable[scope] IN
    LET children == taskState.children IN
    LET workerId == children[x].value IN
    /\ OutOfScope(scope, x)
    /\ IsValueWorkerID(workerId)
    /\ taskTable' =
       LET children_ == [y \in {x} |-> [children[x] EXCEPT !.value = COLLECTED]] @@ children IN
       LET taskState_ == [taskState EXCEPT !.children = children_] IN
       [y \in {scope} |-> taskState_] @@ taskTable
    /\ objectStore' = [y \in DOMAIN objectStore \ {workerId} |-> objectStore[y]]
    /\ UNCHANGED <<nextWorkerId, workerIds, schedulerInbox, taskInbox>>

CollectSomeValue ==
    \E scope \in DOMAIN taskTable :
    \E x \in DOMAIN taskTable[scope].children :
    CollectValue(scope, x)

(***************************************************************************
    System Steps: Message Handling
 ***************************************************************************)

MarkMessageAsRead(inbox, scope) == [inbox EXCEPT ![scope] = Tail(inbox[scope])]

ScheduleTasks(inbox, tasks) == inbox \o SeqFromSet(tasks)

\* Task Creation Step 3:
\* Owner takes a task from its "scheduled" queue and launch it,
\* provided that all of its arguments are ready.
OnTaskScheduled(scope, msg) ==
    LET task == msg[2] IN
    LET workerId == msg[3] IN
    IF workerId \notin workerIds THEN
    \* The worker has failed and we need to resubmit this task.
    /\ schedulerInbox' = ScheduleTasks(schedulerInbox, {task})
    /\ taskInbox' = MarkMessageAsRead(taskInbox, scope)
    /\ UNCHANGED <<nextWorkerId, workerIds, taskTable, objectStore>>
    ELSE IF task.future \notin DOMAIN taskTable[scope].children THEN
    \* This future has gone out of scope so we do not need to launch it.
    /\ taskInbox' = MarkMessageAsRead(taskInbox, scope)
    /\ UNCHANGED <<nextWorkerId, workerIds, taskTable, objectStore, schedulerInbox>>
    ELSE
    \* Launch the task.
    /\ taskTable' =
       [y \in {task.future} |-> NewTaskState(task.owner, workerId, task.args)]
       @@ [taskTable EXCEPT ![scope].children[task.future].workerId = workerId]
    /\ taskInbox' =
       [y \in {task.future} |-> <<>>] @@ MarkMessageAsRead(taskInbox, scope)
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox>>

\* The owner checks if any of its children task is on the failed worker.
\* If so, resubmit the task.
OnWorkerFailed(scope, msg) ==
    LET id == msg[2] IN
    LET children == IF scope \in DOMAIN taskTable THEN taskTable[scope].children ELSE <<>> IN
    LET failedFutures == {x \in DOMAIN children : children[x].workerId = id} IN
    LET failedTasks == {children[x].taskSpec : x \in failedFutures} IN
    /\ schedulerInbox' = ScheduleTasks(schedulerInbox, failedTasks)
    /\ taskInbox' = MarkMessageAsRead(taskInbox, scope)
    /\ UNCHANGED <<nextWorkerId, workerIds, taskTable, objectStore>>

OnTaskReturned(owner, msg) ==
    LET scope == msg[2] IN
    LET retVal == msg[3] IN
    IF scope \notin DOMAIN taskTable[owner].children THEN
    \* This future has gone out of scope. Discard its return value.
    /\ taskInbox' = MarkMessageAsRead(taskInbox, owner)
    /\ UNCHANGED <<nextWorkerId, workerIds, taskTable, objectStore, schedulerInbox>>
    ELSE
    /\ taskTable' =
        [y \in {owner} |-> [taskTable[owner] EXCEPT !.children[scope].value = retVal]]
        @@ taskTable
    /\ taskInbox' = MarkMessageAsRead(taskInbox, owner)
    /\ UNCHANGED <<nextWorkerId, workerIds, objectStore, schedulerInbox>>

ProcessMessage(scope) ==
    LET inbox == taskInbox[scope] IN
    LET msg == Head(inbox) IN
    LET msgTag == msg[1] IN
    CASE msgTag = "SCHEDULED" -> OnTaskScheduled(scope, msg)
      [] msgTag = "RETURNED" -> OnTaskReturned(scope, msg)
      [] msgTag = "FAILED" -> OnWorkerFailed(scope, msg)
      [] OTHER -> FALSE

ProcessSomeMessage ==
    \E scope \in DOMAIN taskInbox :
    /\ Len(taskInbox[scope]) > 0
    /\ ProcessMessage(scope)

(***************************************************************************
    System Steps: Failures
 ***************************************************************************)

\* A set of workers fail; all states and stored objects are lost.
FailWorkers(ids) ==
    LET tasks_ == {task \in DOMAIN taskTable : taskTable[task].workerId \notin ids} IN
    \* 1) Delete the task states that live on this worker.
    /\ taskTable' = [task \in tasks_ |-> taskTable[task]]
    \* 2) Delete any objects stored on this worker.
    /\ objectStore' = [w \in DOMAIN objectStore \ ids |-> objectStore[w]]
    \* 3) Send a message to every task, and remove the failed task's inbox.
    /\ taskInbox' =
        LET msgs == {<<"FAILED", id>> : id \in ids} IN
        [task \in tasks_ |-> taskInbox[task] \o SeqFromSet(msgs)]
    \* 4) Remove the worker ID from the set.
    /\ workerIds' = workerIds \ ids
    /\ UNCHANGED <<nextWorkerId, schedulerInbox>>

RECURSIVE AllDescendantWorkerIds(_)
AllDescendantWorkerIds(scope) ==
    LET children == {} IN
    {taskTable[scope].workerId} \union
    UNION {AllDescendantWorkerIds(x) : x \in children}

\* Pick an owner, fail its worker, and all of its children's workers.
FailSomeWorker ==
    /\ nextWorkerId < MAX_WORKERS * WORKER_ID_SEP
    /\ \E scope \in DOMAIN taskTable \ {0} :
       FailWorkers(AllDescendantWorkerIds(scope))

SystemStep ==
    \/ ScheduleTask
    \/ CollectSomeTaskState
    \/ CollectSomeLineage
    \/ CollectSomeValue
    \/ ProcessSomeMessage

FailureStep ==
    \/ FailSomeWorker


(***************************************************************************
    Spec
 ***************************************************************************)

Next ==
    \/ ProgramStep
    \/ SystemStep
    \/ FailureStep

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(ProgramStep)
    /\ WF_vars(SystemStep)


(***************************************************************************
    Invariants
 ***************************************************************************)

RECURSIVE LineageInScope(_, _)
LineageInScope(scope, x) ==
    LET table == taskTable[scope].children IN
    /\ x \in DOMAIN table
    /\ \/ table[x].value = INLINE_VALUE
       \/ \A arg \in table[x].taskSpec.args : LineageInScope(scope, arg)

LineageInScopeInvariant ==
    \A scope \in DOMAIN taskTable :
    \A x \in DOMAIN taskTable[scope].children :
    LineageInScope(scope, x)

SafetyInvariant ==
    /\ LineageInScopeInvariant


(***************************************************************************
    Temporal Properties
 ***************************************************************************)
 
AllTasksFulfilled ==
    \A scope \in DOMAIN taskTable :
    \A x \in DOMAIN taskTable[scope].children :
    taskTable[scope].children[x].value /= PENDING

SchedulerInboxEmpty == Len(schedulerInbox) = 0

TaskInboxEmpty ==
    \A scope \in DOMAIN taskInbox :
    Len(taskInbox[scope]) = 0

TaskTableEmpty ==
    Cardinality(DOMAIN taskTable) = 0

LivenessProperty ==
    <>[](
        /\ AllTasksFulfilled
        /\ TaskTableEmpty
        /\ SchedulerInboxEmpty
        /\ TaskInboxEmpty
    )


=============================================================================
\* Modification History
\* Last modified Mon Mar 01 22:06:22 PST 2021 by lsf
\* Created Mon Aug 10 17:23:49 PDT 2020 by lsf
