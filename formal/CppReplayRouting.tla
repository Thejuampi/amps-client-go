-------------------------- MODULE CppReplayRouting -------------------------
EXTENDS Naturals, Sequences

CONSTANTS Scenario, BrokenNoSids, BrokenIgnoreBookmark, BrokenNowAsEpoch
VARIABLES phase, wireBookmark, recognizedBookmark, pending, frame, sids, delivered

vars == <<phase, wireBookmark, recognizedBookmark, pending, frame, sids, delivered>>

Expected ==
  CASE Scenario = "plain" -> <<3>>
    [] Scenario = "epoch" -> <<1, 2, 3>>
    [] Scenario = "now" -> <<3>>
    [] Scenario = "after" -> <<2, 3>>

Init ==
  /\ phase = "subscribe"
  /\ wireBookmark = "unset" /\ recognizedBookmark = "unset"
  /\ pending = <<>> /\ frame = 0 /\ sids = "" /\ delivered = <<>>

SendSubscribe ==
  /\ phase = "subscribe"
  /\ wireBookmark' = Scenario
  /\ phase' = "recognize"
  /\ UNCHANGED <<recognizedBookmark, pending, frame, sids, delivered>>

RecognizeBookmark ==
  /\ phase = "recognize"
  /\ recognizedBookmark' = IF BrokenIgnoreBookmark THEN "plain" ELSE wireBookmark
  /\ phase' = "select"
  /\ UNCHANGED <<wireBookmark, pending, frame, sids, delivered>>

SelectHistory ==
  /\ phase = "select"
  /\ pending' = CASE recognizedBookmark = "epoch" -> <<1, 2>>
                  [] recognizedBookmark = "after" -> <<2>>
                  [] recognizedBookmark = "now" /\ BrokenNowAsEpoch -> <<1, 2>>
                  [] OTHER -> <<>>
  /\ phase' = "history"
  /\ UNCHANGED <<wireBookmark, recognizedBookmark, frame, sids, delivered>>

SendHistory ==
  /\ phase = "history" /\ Len(pending) > 0
  /\ frame' = Head(pending) /\ pending' = Tail(pending)
  /\ sids' = IF BrokenNoSids THEN "" ELSE "sub-1"
  /\ phase' = "dispatch-history"
  /\ UNCHANGED <<wireBookmark, recognizedBookmark, delivered>>

DispatchHistory ==
  /\ phase = "dispatch-history"
  /\ delivered' = IF sids = "sub-1" THEN Append(delivered, frame) ELSE delivered
  /\ phase' = "history"
  /\ UNCHANGED <<wireBookmark, recognizedBookmark, pending, frame, sids>>

FinishHistory ==
  /\ phase = "history" /\ Len(pending) = 0
  /\ phase' = "live"
  /\ UNCHANGED <<wireBookmark, recognizedBookmark, pending, frame, sids, delivered>>

SendLive ==
  /\ phase = "live"
  /\ frame' = 3 /\ sids' = IF BrokenNoSids THEN "" ELSE "sub-1"
  /\ phase' = "dispatch-live"
  /\ UNCHANGED <<wireBookmark, recognizedBookmark, pending, delivered>>

DispatchLive ==
  /\ phase = "dispatch-live"
  /\ delivered' = IF sids = "sub-1" THEN Append(delivered, frame) ELSE delivered
  /\ phase' = "done"
  /\ UNCHANGED <<wireBookmark, recognizedBookmark, pending, frame, sids>>

Done == /\ phase = "done" /\ UNCHANGED vars
Next == SendSubscribe \/ RecognizeBookmark \/ SelectHistory \/ SendHistory \/
        DispatchHistory \/ FinishHistory \/ SendLive \/ DispatchLive \/ Done
Spec == Init /\ [][Next]_vars

AllExpectedDelivered == phase = "done" => delivered = Expected
NoUnexpectedDelivery ==
  /\ Len(delivered) <= Len(Expected)
  /\ \A i \in 1..Len(delivered): delivered[i] = Expected[i]
=============================================================================
