---------------------------- MODULE HAHandoff ----------------------------
EXTENDS Naturals
CONSTANTS Scenario, BrokenActiveGuard, BrokenTerminal, BrokenEpoch,
          BrokenPreservePending, BrokenBusyPending, BrokenFinishPending,
          BrokenConnectedGuard
VARIABLES connected, stopped, active, worker, pending, notification, handoff,
          wantConnection, stage, owner, startedNew, loggedOn, dropped,
          terminal, terminalEvents, lateCallback, reconnectEvents, staleDone,
          staleTouched, competingWorker, replacedSession, workerSucceeded,
          notificationObsolete
vars == <<connected, stopped, active, worker, pending, notification, handoff,
          wantConnection, stage, owner, startedNew, loggedOn, dropped,
          terminal, terminalEvents, lateCallback, reconnectEvents, staleDone,
          staleTouched, competingWorker, replacedSession, workerSucceeded,
          notificationObsolete>>
Init ==
  /\ connected = (Scenario \in {"initial", "stale"})
  /\ stopped = (Scenario = "handoff") /\ active = (Scenario = "initial")
  /\ worker = CASE Scenario = "handoff" -> "old"
                  [] Scenario = "listener" -> "reserved" [] OTHER -> "none"
  /\ pending = FALSE /\ notification = FALSE /\ handoff = FALSE
  /\ wantConnection = (Scenario # "handoff")
  /\ stage = IF Scenario = "initial" THEN "return" ELSE "idle"
  /\ owner = IF Scenario = "initial" THEN "initial" ELSE "none"
  /\ startedNew = (Scenario \in {"initial", "stale"})
  /\ loggedOn = (Scenario \in {"initial", "stale"})
  /\ dropped = FALSE /\ terminal = FALSE /\ terminalEvents = 0
  /\ lateCallback = FALSE /\ reconnectEvents = IF Scenario = "initial" THEN 0 ELSE 1
  /\ staleDone = FALSE /\ staleTouched = FALSE /\ competingWorker = FALSE
  /\ replacedSession = FALSE /\ workerSucceeded = FALSE /\ notificationObsolete = FALSE
StartNew ==
  /\ ~startedNew /\ ~active
  /\ active' = TRUE /\ stopped' = FALSE /\ stage' = "dial"
  /\ owner' = "explicit" /\ startedNew' = TRUE /\ wantConnection' = TRUE
  /\ pending' = FALSE /\ terminal' = FALSE
  /\ UNCHANGED <<connected, worker, notification, handoff, loggedOn, dropped,
                 terminalEvents, lateCallback, reconnectEvents, staleDone,
                 staleTouched, competingWorker, replacedSession, workerSucceeded, notificationObsolete>>
LogonSuccess ==
  /\ active /\ stage = "dial" /\ stage' = "return"
  /\ connected' = TRUE /\ loggedOn' = TRUE
  /\ UNCHANGED <<stopped, active, worker, pending, notification, handoff,
                 wantConnection, owner, startedNew, dropped, terminal, terminalEvents,
                 lateCallback, reconnectEvents, staleDone, staleTouched, competingWorker,
                 replacedSession, workerSucceeded, notificationObsolete>>
LoseSocket ==
  /\ connected /\ ~dropped /\ connected' = FALSE /\ dropped' = TRUE
  /\ notification' = TRUE
  /\ UNCHANGED <<stopped, active, worker, pending, handoff, wantConnection, stage,
                 owner, startedNew, loggedOn, terminal, terminalEvents, lateCallback,
                 reconnectEvents, staleDone, staleTouched, competingWorker,
                 replacedSession, workerSucceeded, notificationObsolete>>
HandleNotification ==
  /\ notification /\ notification' = FALSE
  /\ LET ignore == stopped \/ connected
         competing == active /\ owner = "initial" /\ BrokenActiveGuard
         reserve == ~ignore /\ (competing \/ (~active /\ worker = "none"))
         remember == ~ignore /\ ~reserve /\ (active \/ (worker # "none" /\ ~BrokenBusyPending))
     IN /\ worker' = IF reserve THEN "reserved" ELSE worker
        /\ pending' = (pending \/ remember)
        /\ reconnectEvents' = reconnectEvents + IF reserve THEN 1 ELSE 0
        /\ competingWorker' = (competingWorker \/ (~ignore /\ competing))
  /\ UNCHANGED <<connected, stopped, active, handoff, wantConnection, stage,
                 owner, startedNew, loggedOn, dropped, terminal, terminalEvents,
                 lateCallback, staleDone, staleTouched, replacedSession, workerSucceeded, notificationObsolete>>
CompleteSuccess ==
  /\ active /\ stage = "return" /\ active' = FALSE /\ stage' = "done"
  /\ LET needs == pending /\ ~stopped /\ ~connected
     IN /\ handoff' = (handoff \/ (needs /\ worker = "none"))
        /\ pending' = (needs /\ worker # "none" /\ ~BrokenPreservePending)
  /\ workerSucceeded' = (workerSucceeded \/ owner = "worker")
  /\ UNCHANGED <<connected, stopped, worker, notification, wantConnection,
                 owner, startedNew, loggedOn, dropped, terminal, terminalEvents,
                 lateCallback, reconnectEvents, staleDone, staleTouched,
                 competingWorker, replacedSession, notificationObsolete>>
TerminalFailure ==
  /\ active /\ active' = FALSE /\ connected' = FALSE
  /\ stopped' = ~BrokenTerminal /\ terminal' = TRUE
  /\ terminalEvents' = reconnectEvents /\ wantConnection' = FALSE
  /\ stage' = "done" /\ pending' = FALSE
  /\ UNCHANGED <<worker, notification, handoff, owner, startedNew, loggedOn,
                 dropped, lateCallback, reconnectEvents, staleDone, staleTouched,
                 competingWorker, replacedSession, workerSucceeded, notificationObsolete>>
LateCallback ==
  /\ terminal /\ ~lateCallback /\ lateCallback' = TRUE /\ notification' = TRUE
  /\ UNCHANGED <<connected, stopped, active, worker, pending, handoff,
                 wantConnection, stage, owner, startedNew, loggedOn, dropped,
                 terminal, terminalEvents, reconnectEvents, staleDone, staleTouched,
                 competingWorker, replacedSession, workerSucceeded, notificationObsolete>>
CheckNotification ==
  /\ worker = "reserved"
  /\ worker' = "checked" /\ notificationObsolete' = (stopped \/ connected)
  /\ UNCHANGED <<connected, stopped, active, pending, notification, handoff,
                 wantConnection, stage, owner, startedNew, loggedOn, dropped,
                 terminal, terminalEvents, lateCallback, reconnectEvents, staleDone,
                 staleTouched, competingWorker, replacedSession, workerSucceeded>>
ScheduleWorker ==
  /\ worker = "checked" /\ ~notificationObsolete /\ worker' = "running"
  /\ UNCHANGED <<connected, stopped, active, pending, notification, handoff,
                 wantConnection, stage, owner, startedNew, loggedOn, dropped,
                 terminal, terminalEvents, lateCallback, reconnectEvents, staleDone,
                 staleTouched, competingWorker, replacedSession, workerSucceeded, notificationObsolete>>
DropObsoleteWorker ==
  /\ worker = "checked" /\ notificationObsolete /\ worker' = "none"
  /\ handoff' = (handoff \/ (pending /\ ~stopped /\ ~connected))
  /\ pending' = FALSE
  /\ UNCHANGED <<connected, stopped, active, notification,
                 wantConnection, stage, owner, startedNew, loggedOn, dropped,
                 terminal, terminalEvents, lateCallback, reconnectEvents, staleDone,
                 staleTouched, competingWorker, replacedSession, workerSucceeded, notificationObsolete>>
RunWorker ==
  /\ worker = "running" /\ ~active
  /\ IF stopped \/ connected THEN
       /\ worker' = "old"
       /\ replacedSession' = (replacedSession \/ (connected /\ ~stopped /\ BrokenConnectedGuard))
       /\ stopped' = (stopped \/ (connected /\ BrokenConnectedGuard))
       /\ UNCHANGED <<active, stage, owner>>
     ELSE /\ active' = TRUE /\ stage' = "dial" /\ owner' = "worker"
          /\ worker' = "working" /\ UNCHANGED <<stopped, replacedSession>>
  /\ UNCHANGED <<connected, pending, notification, handoff, wantConnection,
                 startedNew, loggedOn, dropped, terminal, terminalEvents,
                 lateCallback, reconnectEvents, staleDone, staleTouched,
                 competingWorker, workerSucceeded, notificationObsolete>>
FinishWorker ==
  /\ worker = "old" \/ (worker = "working" /\ ~active /\ stage = "done")
  /\ worker' = "none"
  /\ handoff' = (handoff \/ (((pending /\ ~BrokenFinishPending) \/ workerSucceeded)
       /\ ~stopped /\ ~connected))
  /\ pending' = FALSE /\ workerSucceeded' = FALSE
  /\ UNCHANGED <<connected, stopped, active, notification, wantConnection,
                 stage, owner, startedNew, loggedOn, dropped, terminal, terminalEvents,
                 lateCallback, reconnectEvents, staleDone, staleTouched,
                 competingWorker, replacedSession, notificationObsolete>>
DispatchHandoff ==
  /\ handoff /\ handoff' = FALSE /\ notification' = TRUE
  /\ UNCHANGED <<connected, stopped, active, worker, pending, wantConnection,
                 stage, owner, startedNew, loggedOn, dropped, terminal, terminalEvents,
                 lateCallback, reconnectEvents, staleDone, staleTouched,
                 competingWorker, replacedSession, workerSucceeded, notificationObsolete>>
RunStaleAttempt ==
  /\ Scenario = "stale" /\ ~staleDone /\ staleDone' = TRUE
  /\ staleTouched' = BrokenEpoch
  /\ UNCHANGED <<connected, stopped, active, worker, pending, notification,
                 handoff, wantConnection, stage, owner, startedNew, loggedOn, dropped,
                 terminal, terminalEvents, lateCallback, reconnectEvents,
                 competingWorker, replacedSession, workerSucceeded, notificationObsolete>>
Done == /\ ~active /\ worker = "none" /\ ~notification /\ ~handoff /\ UNCHANGED vars
Next == StartNew \/ LogonSuccess \/ LoseSocket \/ HandleNotification \/ CompleteSuccess
        \/ TerminalFailure \/ LateCallback \/ CheckNotification \/ ScheduleWorker
        \/ DropObsoleteWorker \/ RunWorker
        \/ FinishWorker \/ DispatchHandoff \/ RunStaleAttempt \/ Done
Spec == Init /\ [][Next]_vars
NoCompetingInitialWorker == ~competingWorker
TerminalIsQuiet == terminal => reconnectEvents <= terminalEvents
StaleAttemptIsRejected == ~staleTouched
RecoveredSessionIsPreserved == ~replacedSession
NoDroppedReconnect == (wantConnection /\ loggedOn /\ ~stopped /\ ~connected
                       /\ ~active /\ worker = "none") => (notification \/ handoff)
=============================================================================
