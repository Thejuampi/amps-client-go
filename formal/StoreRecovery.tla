-------------------------- MODULE StoreRecovery --------------------------
EXTENDS Naturals, Sequences
CONSTANTS BrokenTailRepair, BrokenRejection, BrokenFreeze
VARIABLES wal, checkpoint, memory, accepted, opened, phase, result, halted,
          uncertainEvidence, maintenanceAttempted
vars == <<wal, checkpoint, memory, accepted, opened, phase, result, halted,
          uncertainEvidence, maintenanceAttempted>>
RECURSIVE Recoverable(_)
Recoverable(records) == IF records = <<>> THEN {} ELSE
  IF Head(records) = 0 THEN {} ELSE {Head(records)} \cup Recoverable(Tail(records))
Init == /\ wal = <<1, 0>> /\ checkpoint = {} /\ memory = {1}
        /\ accepted = {1} /\ opened = FALSE /\ phase = "idle" /\ result = "none"
        /\ halted = FALSE /\ uncertainEvidence = <<>> /\ maintenanceAttempted = FALSE
Open ==
  /\ ~opened /\ opened' = TRUE
  /\ wal' = IF BrokenTailRepair THEN wal ELSE <<1>>
  /\ memory' = checkpoint \cup Recoverable(wal)
  /\ UNCHANGED <<checkpoint, accepted, phase, result, halted,
                 uncertainEvidence, maintenanceAttempted>>
StartAppend ==
  /\ opened /\ phase = "idle" /\ ~halted
  /\ wal' = Append(wal, 2) /\ phase' = "written"
  /\ UNCHANGED <<checkpoint, memory, accepted, opened, result, halted,
                 uncertainEvidence, maintenanceAttempted>>
CommitAppend ==
  /\ phase = "written" /\ memory' = memory \cup {2}
  /\ accepted' = accepted \cup {2} /\ phase' = "checkpoint" /\ result' = "accepted"
  /\ UNCHANGED <<wal, checkpoint, opened, halted, uncertainEvidence, maintenanceAttempted>>
RollbackAppend ==
  /\ phase = "written" /\ wal' = SubSeq(wal, 1, Len(wal) - 1)
  /\ phase' = "done" /\ result' = "rejected"
  /\ UNCHANGED <<checkpoint, memory, accepted, opened, halted,
                 uncertainEvidence, maintenanceAttempted>>
UncertainAppend ==
  /\ phase = "written" /\ halted' = TRUE /\ result' = "uncertain"
  /\ phase' = "done" /\ uncertainEvidence' = wal
  /\ UNCHANGED <<wal, checkpoint, memory, accepted, opened, maintenanceAttempted>>
CheckpointSuccess ==
  /\ phase = "checkpoint" /\ checkpoint' = memory /\ wal' = <<>>
  /\ phase' = "done"
  /\ UNCHANGED <<memory, accepted, opened, result, halted,
                 uncertainEvidence, maintenanceAttempted>>
CheckpointFailure ==
  /\ phase = "checkpoint" /\ phase' = "done"
  /\ result' = IF BrokenRejection THEN "rejected" ELSE "accepted-maintenance-error"
  /\ UNCHANGED <<wal, checkpoint, memory, accepted, opened, halted,
                 uncertainEvidence, maintenanceAttempted>>
AttemptMaintenance ==
  /\ result = "uncertain" /\ ~maintenanceAttempted
  /\ maintenanceAttempted' = TRUE
  /\ wal' = IF BrokenFreeze THEN <<>> ELSE wal
  /\ checkpoint' = IF BrokenFreeze THEN memory ELSE checkpoint
  /\ UNCHANGED <<memory, accepted, opened, phase, result, halted, uncertainEvidence>>
Reopen ==
  /\ phase = "done" /\ phase' = "reopened"
  /\ memory' = checkpoint \cup Recoverable(wal) /\ result' = "reopened"
  /\ UNCHANGED <<wal, checkpoint, accepted, opened, halted,
                 uncertainEvidence, maintenanceAttempted>>
Done == /\ phase = "reopened" /\ UNCHANGED vars
Next == Open \/ StartAppend \/ CommitAppend \/ RollbackAppend \/ UncertainAppend
        \/ CheckpointSuccess \/ CheckpointFailure \/ AttemptMaintenance \/ Reopen \/ Done
Spec == Init /\ [][Next]_vars
NoAcceptedLoss == accepted \subseteq (checkpoint \cup Recoverable(wal))
RejectedCannotReplay == result = "rejected" => 2 \notin (memory \cup checkpoint \cup Recoverable(wal))
UncertainEvidencePreserved == result = "uncertain" => (halted /\ wal = uncertainEvidence)
=============================================================================
