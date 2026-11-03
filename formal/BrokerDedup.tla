---------------------------- MODULE BrokerDedup --------------------------
EXTENDS Naturals
CONSTANTS BrokenDeleteRestore, BrokenPublishRestore
VARIABLES phase, watermarkA, watermarkB, journalDelete, journalPublish,
          row, deleteApplications, publishApplications, deleteReplays, publishReplays
vars == <<phase, watermarkA, watermarkB, journalDelete, journalPublish,
          row, deleteApplications, publishApplications, deleteReplays, publishReplays>>
Init == /\ phase = 0 /\ watermarkA = 0 /\ watermarkB = 0
        /\ journalDelete = FALSE /\ journalPublish = FALSE /\ row = "old"
        /\ deleteApplications = 0 /\ publishApplications = 0
        /\ deleteReplays = 0 /\ publishReplays = 0
DeleteFirst ==
  /\ phase = 0 /\ phase' = 1 /\ watermarkA' = 1 /\ journalDelete' = TRUE
  /\ row' = "none" /\ deleteApplications' = 1
  /\ UNCHANGED <<watermarkB, journalPublish, publishApplications, deleteReplays, publishReplays>>
PublishNewRow ==
  /\ phase = 1 /\ phase' = 2 /\ watermarkB' = 1 /\ journalPublish' = TRUE
  /\ row' = "new" /\ publishApplications' = 1
  /\ UNCHANGED <<watermarkA, journalDelete, deleteApplications, deleteReplays, publishReplays>>
Restart ==
  /\ phase = 2 /\ phase' = 3
  /\ watermarkA' = IF journalDelete /\ ~BrokenDeleteRestore THEN 1 ELSE 0
  /\ watermarkB' = IF journalPublish /\ ~BrokenPublishRestore THEN 1 ELSE 0
  /\ UNCHANGED <<journalDelete, journalPublish, row, deleteApplications,
                 publishApplications, deleteReplays, publishReplays>>
ReplayDelete ==
  /\ phase >= 1 /\ deleteReplays < 2 /\ deleteReplays' = deleteReplays + 1
  /\ row' = IF watermarkA = 0 THEN "none" ELSE row
  /\ deleteApplications' = deleteApplications + IF watermarkA = 0 THEN 1 ELSE 0
  /\ watermarkA' = 1
  /\ UNCHANGED <<phase, watermarkB, journalDelete, journalPublish,
                 publishApplications, publishReplays>>
ReplayPublish ==
  /\ phase >= 2 /\ publishReplays < 2 /\ publishReplays' = publishReplays + 1
  /\ row' = IF watermarkB = 0 THEN "new" ELSE row
  /\ publishApplications' = publishApplications + IF watermarkB = 0 THEN 1 ELSE 0
  /\ watermarkB' = 1
  /\ UNCHANGED <<phase, watermarkA, journalDelete, journalPublish,
                 deleteApplications, deleteReplays>>
Done == /\ phase = 3 /\ deleteReplays = 2 /\ publishReplays = 2 /\ UNCHANGED vars
Next == DeleteFirst \/ PublishNewRow \/ Restart \/ ReplayDelete \/ ReplayPublish \/ Done
Spec == Init /\ [][Next]_vars
DeleteReplayIsIdempotent == deleteApplications <= 1
PublishReplayIsIdempotent == publishApplications <= 1
NewRowSurvivesOldDelete == phase >= 2 => row = "new"
=============================================================================
