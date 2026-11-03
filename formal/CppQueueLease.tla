---------------------------- MODULE CppQueueLease --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Eligibility, SameSubscriptionID, BrokenNoFallback, BrokenSameIDExclusion,
          BrokenDropPending, BrokenDuplicateRecord
VARIABLES phase, eligible, target, sent, pending, leases
vars == <<phase, eligible, target, sent, pending, leases>>

EligibleSubscribers ==
  CASE Eligibility = "none" -> {}
    [] Eligibility = "owner-only" -> {"owner"}
    [] Eligibility = "other-only" -> {"other"}
    [] Eligibility = "both" -> {"owner", "other"}

Init ==
  /\ phase = "expired" /\ eligible = EligibleSubscribers
  /\ target = "none" /\ sent = FALSE /\ pending = FALSE /\ leases = {"owner"}

RemoveExpiredLease ==
  /\ phase = "expired" /\ leases' = {}
  /\ phase' = "select"
  /\ UNCHANGED <<eligible, target, sent, pending>>

SelectRedeliveryTarget ==
  /\ phase = "select"
  /\ target' =
       IF "other" \in eligible /\ ~(BrokenSameIDExclusion /\ SameSubscriptionID)
       THEN "other"
       ELSE IF "owner" \in eligible /\ ~BrokenNoFallback
            THEN "owner"
            ELSE "none"
  /\ phase' = IF target' = "none" THEN "enqueue" ELSE "send"
  /\ UNCHANGED <<eligible, sent, pending, leases>>

EnqueuePending ==
  /\ phase = "enqueue" /\ pending' = ~BrokenDropPending /\ phase' = "done"
  /\ UNCHANGED <<eligible, target, sent, leases>>

SendRedelivery ==
  /\ phase = "send" /\ sent' = TRUE /\ phase' = "record"
  /\ UNCHANGED <<eligible, target, pending, leases>>

RecordLease ==
  /\ phase = "record"
  /\ leases' = IF BrokenDuplicateRecord THEN {"owner", target} ELSE {target}
  /\ phase' = "done"
  /\ UNCHANGED <<eligible, target, sent, pending>>

Done == /\ phase = "done" /\ UNCHANGED vars
Next == RemoveExpiredLease \/ SelectRedeliveryTarget \/ EnqueuePending \/
        SendRedelivery \/ RecordLease \/ Done
Spec == Init /\ [][Next]_vars

NoEligibleMessageStrandedAfterDispatch ==
  (phase = "done" /\ eligible # {}) => leases # {}
PreferOtherWhenEligible ==
  (phase = "done" /\ "other" \in eligible) => target = "other"
UniqueLease == Cardinality(leases) <= 1
RetainedAfterDispatch == phase = "done" => pending \/ leases # {}
=============================================================================
