---------------------------- MODULE Replication ----------------------------
(***************************************************************************)
(* Models the replicated-disk backend of durable_buffer, one partition.    *)
(*                                                                         *)
(*   DurableBuffer.Backend.Replica  commit_async/4, handle_message/2,      *)
(*                                  truncate/1, advance/3, durable_count/2 *)
(*   DurableBuffer.Replica.Sender   handle_cast/2, reconcile/2,            *)
(*                                  :resync_step, drop_acked/3,            *)
(*                                  force_reattach/1, next_needed/1        *)
(*   DurableBuffer.Replica.Writer   replicate_group/2                      *)
(*                                                                         *)
(* One batch is one byte, so a WAL byte offset is a sequence index and a    *)
(* batch id also identifies its content. Channels are ordered, matching     *)
(* Erlang's per-pair message ordering.                                      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    MaxId,          \* how many batches the primary may commit
    MaxChan,        \* bound on in-flight messages, for the state constraint
    Replicas,
    NeededAcks,     \* the ack: policy, primary included
    TailCheck,      \* the writer appends only at its own tail
    EpochStamp,     \* the epoch fences a replica that missed a truncate
    Fsync,          \* the primary datasyncs before it counts its own ack
    AllowCrash,     \* the primary may lose its unsynced WAL tail
    AllowLoss,      \* the channel to a writer may drop a batch
    AllowTruncate,
    HealOnOpen      \* open/2 pulls back bytes a replica holds and the
                    \* primary lost, before the partition serves

VARIABLES
    pEpoch,     \* primary epoch, from DurableBuffer.Epoch
    pWal,       \* primary WAL, a sequence of batch ids
    pSynced,    \* length of the primary WAL prefix that survives a crash
    nextId,
    acked,      \* batch ids the caller was told :ok
    pending,    \* commits awaiting their ack target
    rEpoch,
    rWal,
    chan,       \* [r -> Seq] primary -> writer, the writer's mailbox
    ackq,       \* [r -> Seq] writer -> primary watermarks
    queue,      \* [r -> Seq] the sender's unacked queue
    mode,       \* [r -> "live" | "resync" | "detached"]
    wm,         \* [r -> watermark] highest watermark the primary saw
    cursor,     \* [r -> Nat] resync cursor
    crashed,
    truncated

vars == <<pEpoch, pWal, pSynced, nextId, acked, pending, rEpoch, rWal,
          chan, ackq, queue, mode, wm, cursor, crashed, truncated>>

Min(a, b) == IF a < b THEN a ELSE b

Perms == Permutations(Replicas)

(***************************************************************************)
(* A watermark is <<epoch, offset>>: everything up to offset in epoch is on *)
(* that member's disk. The epoch dominates the offset.                      *)
(***************************************************************************)
WmLeq(a, b) == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] <= b[2])
WmMax(a, b) == IF WmLeq(a, b) THEN b ELSE a

(***************************************************************************)
(* Each attach mints a reference that stamps the batches it sends, and the  *)
(* writer echoes it on every ack. Rather than count generations, a message  *)
(* or an ack carries `stale`, set when an attach happens after it was sent. *)
(* The sender drops a stale ack. The writer still appends a stale batch: it *)
(* cannot tell, and only the ack matters.                                   *)
(***************************************************************************)
Msg(e, o, i) == [epoch |-> e, off |-> o, id |-> i, stale |-> FALSE]

Ack(w, stale) == [wm |-> w, stale |-> stale]

RECURSIVE MarkStale(_)
MarkStale(q) ==
    IF q = <<>>
      THEN <<>>
      ELSE <<[q[1] EXCEPT !.stale = TRUE]>> \o MarkStale(Tail(q))

(***************************************************************************)
(* Sender.next_needed/1: the first unacked offset, or the primary tail when *)
(* the queue is empty.                                                      *)
(***************************************************************************)
NextNeeded(r) == IF queue[r] # <<>> THEN queue[r][1].off ELSE Len(pWal)

DurableCount(t) == 1 + Cardinality({r \in Replicas : WmLeq(t, wm[r])})

RECURSIVE DropAcked(_, _)
DropAcked(q, w) ==
    IF q = <<>>
      THEN q
      ELSE IF WmLeq(<<q[1].epoch, q[1].off + 1>>, w)
             THEN DropAcked(Tail(q), w)
             ELSE q

Init ==
    /\ pEpoch = 0
    /\ pWal = <<>>
    /\ pSynced = 0
    /\ nextId = 1
    /\ acked = {}
    /\ pending = <<>>
    /\ rEpoch = [r \in Replicas |-> 0]
    /\ rWal = [r \in Replicas |-> <<>>]
    /\ chan = [r \in Replicas |-> <<>>]
    /\ ackq = [r \in Replicas |-> <<>>]
    /\ queue = [r \in Replicas |-> <<>>]
    /\ mode = [r \in Replicas |-> "live"]
    /\ wm = [r \in Replicas |-> <<0, 0>>]
    /\ cursor = [r \in Replicas |-> 0]
    /\ crashed = FALSE
    /\ truncated = FALSE

(***************************************************************************)
(* Backend.Replica.commit_async/4: write the local WAL, hand the batch to   *)
(* every sender, then wait for the ack target.                              *)
(***************************************************************************)
Commit ==
    /\ nextId <= MaxId
    /\ LET off == Len(pWal)
           m == Msg(pEpoch, off, nextId)
       IN /\ pWal' = Append(pWal, nextId)
          /\ pSynced' = IF Fsync THEN off + 1 ELSE pSynced
          /\ queue' = [r \in Replicas |-> Append(queue[r], m)]
          /\ chan' = [r \in Replicas |->
                       IF mode[r] = "live" THEN Append(chan[r], m) ELSE chan[r]]
          /\ pending' = Append(pending, [id |-> nextId, t |-> <<pEpoch, off + 1>>])
          /\ nextId' = nextId + 1
    /\ UNCHANGED <<pEpoch, acked, rEpoch, rWal, ackq, mode, wm, cursor,
                   crashed, truncated>>

(***************************************************************************)
(* Backend.Replica.handle_message/2: pending targets are ordered, so the    *)
(* head completes first.                                                    *)
(***************************************************************************)
CompletePending ==
    /\ pending # <<>>
    /\ DurableCount(pending[1].t) >= NeededAcks
    /\ acked' = acked \cup {pending[1].id}
    /\ pending' = Tail(pending)
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, rEpoch, rWal, chan, ackq,
                   queue, mode, wm, cursor, crashed, truncated>>

(***************************************************************************)
(* Writer.replicate_group/2: append at the tail, re-ack a duplicate, nack   *)
(* anything else. A nack force_reattaches the sender.                       *)
(***************************************************************************)
DeliverBatch(r) ==
    /\ chan[r] # <<>>
    /\ LET m == chan[r][1]
           tail == Len(rWal[r])
           epochOk == (~EpochStamp) \/ m.epoch = rEpoch[r]
           dup == m.epoch = rEpoch[r] /\ m.off + 1 <= tail
       IN IF dup
            THEN /\ ackq' = [ackq EXCEPT ![r] = Append(@, Ack(<<rEpoch[r], tail>>, m.stale))]
                 /\ UNCHANGED <<rWal, mode>>
            ELSE IF epochOk /\ (m.off = tail \/ ~TailCheck)
              THEN /\ rWal' = [rWal EXCEPT ![r] = Append(@, m.id)]
                   /\ ackq' =
                        [ackq EXCEPT ![r] = Append(@, Ack(<<rEpoch[r], tail + 1>>, m.stale))]
                   /\ UNCHANGED mode
              ELSE /\ mode' = [mode EXCEPT ![r] = "detached"]
                   /\ UNCHANGED <<rWal, ackq>>
    /\ chan' = [chan EXCEPT ![r] = Tail(@)]
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, acked, pending, rEpoch,
                   queue, wm, cursor, crashed, truncated>>

LoseBatch(r) ==
    /\ AllowLoss
    /\ chan[r] # <<>>
    /\ chan' = [chan EXCEPT ![r] = Tail(@)]
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, acked, pending, rEpoch,
                   rWal, ackq, queue, mode, wm, cursor, crashed, truncated>>

(***************************************************************************)
(* Sender.handle_info({:replica_ack, watermark}, _): advance the member's   *)
(* watermark and drop the batches it covers.                                *)
(***************************************************************************)
DeliverAck(r) ==
    /\ ackq[r] # <<>>
    /\ IF ackq[r][1].stale
         THEN UNCHANGED <<wm, queue>>
         ELSE /\ wm' = [wm EXCEPT ![r] = WmMax(@, ackq[r][1].wm)]
              /\ queue' = [queue EXCEPT ![r] = DropAcked(@, ackq[r][1].wm)]
    /\ ackq' = [ackq EXCEPT ![r] = Tail(@)]
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, acked, pending, rEpoch,
                   rWal, chan, mode, cursor, crashed, truncated>>

(***************************************************************************)
(* Sender.force_reattach/1: writer death, a nack, an ack stall.             *)
(***************************************************************************)
Detach(r) ==
    /\ mode[r] # "detached"
    /\ mode' = [mode EXCEPT ![r] = "detached"]
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, acked, pending, rEpoch,
                   rWal, chan, ackq, queue, wm, cursor, crashed, truncated>>

(***************************************************************************)
(* Sender.handle_cast/2, the :max_sender_bytes branch: drop the queue and   *)
(* re-attach. next_needed/1 then falls back to the primary tail.            *)
(***************************************************************************)
Overflow(r) ==
    /\ queue[r] # <<>>
    /\ queue' = [queue EXCEPT ![r] = <<>>]
    /\ mode' = [mode EXCEPT ![r] = "detached"]
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, acked, pending, rEpoch,
                   rWal, chan, ackq, wm, cursor, crashed, truncated>>

(***************************************************************************)
(* Sender.reconcile/2 on attach. The remote tail is compared against the    *)
(* primary's own WAL, not against the sender's unacked queue: a replica     *)
(* whose ack is merely still in flight holds a valid prefix and must not be *)
(* wiped. What it already has is dropped from the queue and adopted as its  *)
(* watermark. EpochStamp gates the comparison as well as the writer's       *)
(* accept rule: without it the sender ignores the epoch entirely.           *)
(***************************************************************************)
Attach(r) ==
    /\ mode[r] = "detached"
    /\ LET epochOk == IF EpochStamp THEN rEpoch[r] = pEpoch ELSE TRUE
           remote == Len(rWal[r])
           tail == <<pEpoch, remote>>
       IN IF ~epochOk \/ remote > Len(pWal)
            THEN /\ rWal' = [rWal EXCEPT ![r] = <<>>]
                 /\ rEpoch' = [rEpoch EXCEPT ![r] = pEpoch]
                 /\ mode' = [mode EXCEPT ![r] = "resync"]
                 /\ cursor' = [cursor EXCEPT ![r] = 0]
                 /\ chan' = [chan EXCEPT ![r] = MarkStale(@)]
                 /\ UNCHANGED <<queue, wm>>
            ELSE IF remote < NextNeeded(r)
              THEN /\ mode' = [mode EXCEPT ![r] = "resync"]
                   /\ cursor' = [cursor EXCEPT ![r] = remote]
                   /\ chan' = [chan EXCEPT ![r] = MarkStale(@)]
                   /\ UNCHANGED <<rWal, rEpoch, queue, wm>>
              ELSE /\ mode' = [mode EXCEPT ![r] = "live"]
                   /\ queue' = [queue EXCEPT ![r] = DropAcked(@, tail)]
                   /\ wm' = [wm EXCEPT ![r] = WmMax(@, tail)]
                   /\ chan' =
                        [chan EXCEPT ![r] = MarkStale(@) \o DropAcked(queue[r], tail)]
                   /\ UNCHANGED <<rWal, rEpoch, cursor>>
    /\ ackq' = [ackq EXCEPT ![r] = MarkStale(@)]
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, acked, pending,
                   crashed, truncated>>

(***************************************************************************)
(* Sender.handle_info(:resync_step, _): stream the WAL suffix, then resume  *)
(* live traffic by re-sending the unacked queue.                            *)
(***************************************************************************)
ResyncStep(r) ==
    /\ mode[r] = "resync"
    /\ IF cursor[r] >= NextNeeded(r)
         THEN /\ mode' = [mode EXCEPT ![r] = "live"]
              /\ chan' = [chan EXCEPT ![r] = @ \o queue[r]]
              /\ UNCHANGED cursor
         ELSE IF cursor[r] < Len(pWal)
           THEN /\ chan' = [chan EXCEPT ![r] =
                             Append(@, Msg(pEpoch, cursor[r], pWal[cursor[r] + 1]))]
                /\ cursor' = [cursor EXCEPT ![r] = cursor[r] + 1]
                /\ UNCHANGED mode
           ELSE /\ mode' = [mode EXCEPT ![r] = "detached"]
                /\ UNCHANGED <<chan, cursor>>
    /\ UNCHANGED <<pEpoch, pWal, pSynced, nextId, acked, pending, rEpoch,
                   rWal, ackq, queue, wm, crashed, truncated>>

(***************************************************************************)
(* The primary loses the WAL tail it never datasynced, then restarts. Every *)
(* sender restarts with an empty queue and a zero watermark.                *)
(*                                                                          *)
(* With HealOnOpen, Backend.Replica.open/2 asks every replica for its tail   *)
(* and pulls back what it is missing from the furthest one that is on its    *)
(* own epoch. A replica WAL is always a prefix-extension of the primary's,   *)
(* so the healed WAL is that replica's WAL. The pull is datasynced, hence    *)
(* pSynced follows it.                                                       *)
(***************************************************************************)
Crash ==
    /\ AllowCrash
    /\ ~crashed
    /\ LET lost == SubSeq(pWal, 1, pSynced)
           best == CHOOSE r \in Replicas :
                     \A other \in Replicas : Len(rWal[other]) <= Len(rWal[r])
           healed == IF /\ HealOnOpen
                        /\ rEpoch[best] = pEpoch
                        /\ Len(rWal[best]) > Len(lost)
                       THEN rWal[best]
                       ELSE lost
       IN /\ pWal' = healed
          /\ pSynced' = Len(healed)
    /\ crashed' = TRUE
    /\ pending' = <<>>
    /\ queue' = [r \in Replicas |-> <<>>]
    /\ chan' = [r \in Replicas |-> <<>>]
    /\ ackq' = [r \in Replicas |-> <<>>]
    /\ mode' = [r \in Replicas |-> "detached"]
    /\ wm' = [r \in Replicas |-> <<0, 0>>]
    /\ cursor' = [r \in Replicas |-> 0]
    /\ UNCHANGED <<pEpoch, nextId, acked, rEpoch, rWal, truncated>>

(***************************************************************************)
(* Backend.Replica.truncate/1: bump and persist the epoch, wipe the local   *)
(* WAL, reset every sender, then erpc a truncate to every replica. That     *)
(* erpc is wrapped in try/catch, so it may fail for any subset.             *)
(***************************************************************************)
Truncate ==
    /\ AllowTruncate
    /\ ~truncated
    /\ pEpoch' = pEpoch + 1
    /\ pWal' = <<>>
    /\ pSynced' = 0
    /\ acked' = {}
    /\ pending' = <<>>
    /\ queue' = [r \in Replicas |-> <<>>]
    /\ truncated' = TRUE
    /\ \E reached \in SUBSET Replicas :
         /\ rWal' = [r \in Replicas |-> IF r \in reached THEN <<>> ELSE rWal[r]]
         /\ rEpoch' = [r \in Replicas |->
                        IF r \in reached THEN pEpoch + 1 ELSE rEpoch[r]]
    /\ UNCHANGED <<nextId, chan, ackq, mode, wm, cursor, crashed>>

Next ==
    \/ Commit
    \/ CompletePending
    \/ Crash
    \/ Truncate
    \/ \E r \in Replicas :
         \/ DeliverBatch(r)
         \/ LoseBatch(r)
         \/ DeliverAck(r)
         \/ Attach(r)
         \/ ResyncStep(r)
         \/ Detach(r)
         \/ Overflow(r)

Spec == Init /\ [][Next]_vars

Constraint ==
    \A r \in Replicas : Len(chan[r]) <= MaxChan /\ Len(ackq[r]) <= MaxChan

(***************************************************************************)
(* Safety                                                                   *)
(***************************************************************************)

(* A replica never diverges: it agrees with the primary wherever both hold  *)
(* a byte. This is what the tail check and the epoch fence protect.         *)
NoConflict ==
    \A r \in Replicas :
      \A i \in 1..Min(Len(pWal), Len(rWal[r])) : pWal[i] = rWal[r][i]

(* Reads are served from the local WAL, so an acked batch must still be     *)
(* readable from the primary.                                              *)
AckedInPrimary ==
    \A id \in acked : \E i \in 1..Len(pWal) : pWal[i] = id

(* stream/2 reads the local WAL through the static backend config, so it    *)
(* never consults a watermark. Every readable byte would have to have met    *)
(* the ack policy for a read to be ack-durable.                             *)
ReadsAreAckDurable ==
    \A i \in 1..Len(pWal) : DurableCount(<<pEpoch, i>>) >= NeededAcks

(* The weaker claim: an acked batch still exists on some member.            *)
AckedSomewhere ==
    \A id \in acked :
      \/ \E i \in 1..Len(pWal) : pWal[i] = id
      \/ \E r \in Replicas : \E i \in 1..Len(rWal[r]) : rWal[r][i] = id

=============================================================================
