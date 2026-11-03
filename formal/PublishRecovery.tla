------------------------- MODULE PublishRecovery -------------------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS MaxMessages, MaxGeneration, BrokenGate, BrokenAck, BrokenGeneration
VARIABLES retained, accepted, applied, nextSequence, generation, connected,
          awaitingReplay, phase, snapshot, position, replayGeneration,
          network, acknowledgements, staleSend
vars == <<retained, accepted, applied, nextSequence, generation, connected,
          awaitingReplay, phase, snapshot, position, replayGeneration,
          network, acknowledgements, staleSend>>
RECURSIVE Ordered(_)
Ordered(s) == IF s = {} THEN <<>> ELSE
  LET first == CHOOSE n \in s : \A other \in s : n <= other
  IN <<first>> \o Ordered(s \ {first})
Elements(s) == {s[i] : i \in 1..Len(s)}
Init == /\ retained = {1} /\ accepted = {1} /\ applied = {}
        /\ nextSequence = 2 /\ generation = 0 /\ connected = FALSE
        /\ awaitingReplay = TRUE /\ phase = "idle" /\ snapshot = <<>>
        /\ position = 1 /\ replayGeneration = 0 /\ network = <<>>
        /\ acknowledgements = <<>> /\ staleSend = FALSE
Publish ==
  /\ nextSequence <= MaxMessages /\ (phase = "idle" \/ BrokenGate)
  /\ retained' = retained \cup {nextSequence}
  /\ accepted' = accepted \cup {nextSequence}
  /\ network' = IF connected /\ ~awaitingReplay
       THEN Append(network, [seq |-> nextSequence, persisted |-> TRUE]) ELSE network
  /\ nextSequence' = nextSequence + 1
  /\ UNCHANGED <<applied, generation, connected, awaitingReplay, phase,
                 snapshot, position, replayGeneration, acknowledgements, staleSend>>
Connect ==
  /\ ~connected /\ generation < MaxGeneration
  /\ connected' = TRUE /\ generation' = generation + 1 /\ awaitingReplay' = TRUE
  /\ UNCHANGED <<retained, accepted, applied, nextSequence, phase, snapshot,
                 position, replayGeneration, network, acknowledgements, staleSend>>
Disconnect ==
  /\ connected /\ generation < MaxGeneration
  /\ connected' = FALSE /\ awaitingReplay' = TRUE
  /\ network' = <<>> /\ acknowledgements' = <<>>
  /\ UNCHANGED <<retained, accepted, applied, nextSequence, generation, phase,
                 snapshot, position, replayGeneration, staleSend>>
StartReplay ==
  /\ connected /\ awaitingReplay /\ phase = "idle"
  /\ phase' = "replay" /\ snapshot' = Ordered(retained)
  /\ position' = 1 /\ replayGeneration' = generation
  /\ UNCHANGED <<retained, accepted, applied, nextSequence, generation, connected,
                 awaitingReplay, network, acknowledgements, staleSend>>
SendReplay ==
  /\ phase = "replay" /\ position <= Len(snapshot) /\ connected
  /\ (replayGeneration = generation \/ BrokenGeneration)
  /\ network' = Append(network,
       [seq |-> snapshot[position], persisted |-> (~BrokenAck \/ snapshot[position] # 1)])
  /\ position' = position + 1
  /\ staleSend' = (staleSend \/ replayGeneration # generation)
  /\ UNCHANGED <<retained, accepted, applied, nextSequence, generation, connected,
                 awaitingReplay, phase, snapshot, replayGeneration, acknowledgements>>
AbortReplay ==
  /\ phase = "replay" /\ (~connected \/ replayGeneration # generation)
  /\ phase' = "idle"
  /\ UNCHANGED <<retained, accepted, applied, nextSequence, generation, connected,
                 awaitingReplay, snapshot, position, replayGeneration, network,
                 acknowledgements, staleSend>>
FinishReplay ==
  /\ phase = "replay" /\ position > Len(snapshot)
  /\ phase' = "idle"
  /\ awaitingReplay' = IF connected /\ replayGeneration = generation
       THEN FALSE ELSE awaitingReplay
  /\ UNCHANGED <<retained, accepted, applied, nextSequence, generation, connected,
                 snapshot, position, replayGeneration, network, acknowledgements, staleSend>>
Deliver ==
  /\ connected /\ Len(network) > 0
  /\ applied' = applied \cup {Head(network).seq}
  /\ acknowledgements' = IF Head(network).persisted
       THEN Append(acknowledgements, Head(network).seq) ELSE acknowledgements
  /\ network' = Tail(network)
  /\ UNCHANGED <<retained, accepted, nextSequence, generation, connected,
                 awaitingReplay, phase, snapshot, position, replayGeneration, staleSend>>
PersistedAck ==
  /\ connected /\ Len(acknowledgements) > 0
  /\ retained' = {n \in retained : n > Head(acknowledgements)}
  /\ acknowledgements' = Tail(acknowledgements)
  /\ UNCHANGED <<accepted, applied, nextSequence, generation, connected,
                 awaitingReplay, phase, snapshot, position, replayGeneration, network, staleSend>>
Done == /\ nextSequence > MaxMessages /\ retained = {} /\ phase = "idle"
        /\ network = <<>> /\ acknowledgements = <<>> /\ UNCHANGED vars
Next == Publish \/ Connect \/ Disconnect \/ StartReplay \/ SendReplay \/ AbortReplay
        \/ FinishReplay \/ Deliver \/ PersistedAck \/ Done
Spec == Init /\ [][Next]_vars
TypeOK == /\ retained \subseteq 1..MaxMessages /\ accepted \subseteq 1..MaxMessages
          /\ applied \subseteq 1..MaxMessages /\ generation \in 0..MaxGeneration
          /\ nextSequence \in 2..(MaxMessages + 1)
NoLoss == accepted \subseteq (retained \cup applied)
PersistedRequested == \A item \in Elements(network) : item.persisted
CurrentGenerationOnly == ~staleSend
=============================================================================
