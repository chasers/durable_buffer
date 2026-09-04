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
    HealOnOpen,     \* open/2 pulls back bytes a replica holds and the
                    \* primary lost, before the partition serves
    GateReads,      \* stream/3 stops at the durable offset
    AllowTrim,      \* trim/3 drops entries below the ack floor
    TrimRebases     \* a replica the trim reaches advances its own base

VARIABLES
    pEpoch,     \* primary epoch, from DurableBuffer.Epoch
    pWal,       \* primary WAL, the bytes the primary still retains
    pDropped,   \* bytes trimmed from the head, still readable through an
                \* fd a resync opened before the trim unlinked the file
    pBase,      \* logical offset the retained bytes start at
    pSynced,    \* logical offset through which the primary WAL survives a crash
    nextId,
    acked,      \* batch ids the caller was told :ok
    pending,    \* commits awaiting their ack target
    rEpoch,
    rWal,
    rBase,      \* [r -> Nat] logical offset each replica's bytes start at
    chan,       \* [r -> Seq] primary -> writer, the writer's mailbox
    ackq,       \* [r -> Seq] writer -> primary watermarks
    queue,      \* [r -> Seq] the sender's unacked queue
    mode,       \* [r -> "live" | "resync" | "detached"]
    wm,         \* [r -> watermark] highest watermark the primary saw
    cursor,     \* [r -> Nat] resync cursor
    resyncBase, \* [r -> Nat] base the running resync opened its fd at
    adopted,    \* [r -> Nat] newest epoch that replica has confirmed
    crashed,
    truncated,
    trimmed

vars == <<pEpoch, pWal, pDropped, pBase, pSynced, nextId, acked, pending, rEpoch, rWal,
          rBase, chan, ackq, queue, mode, wm, cursor, resyncBase, adopted, crashed,
          truncated, trimmed>>

Min(a, b) == IF a < b THEN a ELSE b
Max2(a, b) == IF a < b THEN b ELSE a

(***************************************************************************)
(* Byte offsets are logical: they count from the first byte ever written,   *)
(* not from the start of the file. Trimming the head advances a base and    *)
(* leaves every offset already stamped on the wire alone. The byte at       *)
(* logical offset L lives at pWal[L - pBase + 1] while pBase <= L < PTail.  *)
(***************************************************************************)
PTail == pBase + Len(pWal)
RTail(r) == rBase[r] + Len(rWal[r])

(***************************************************************************)
(* Everything the primary has written this epoch, retained or not. A resync *)
(* opens the WAL once and keeps that fd, so a trim that unlinks the file    *)
(* does not stop it mid-stream: it reads on through the old inode. Reading  *)
(* Full models exactly that.                                               *)
(***************************************************************************)
Full == pDropped \o pWal

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
NextNeeded(r) == IF queue[r] # <<>> THEN queue[r][1].off ELSE PTail

DurableCount(t) == 1 + Cardinality({r \in Replicas : WmLeq(t, wm[r])})

(***************************************************************************)
(* Backend.Replica.durable_offset/1: the furthest offset that NeededAcks     *)
(* member watermarks agree on.                                              *)
(***************************************************************************)
DurableThrough ==
    LET reached == {i \in pBase..PTail : DurableCount(<<pEpoch, i>>) >= NeededAcks}
    IN IF reached = {}
         THEN pBase
         ELSE CHOOSE i \in reached : \A j \in reached : j <= i

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
    /\ pDropped = <<>>
    /\ pBase = 0
    /\ pSynced = 0
    /\ nextId = 1
    /\ acked = {}
    /\ pending = <<>>
    /\ rEpoch = [r \in Replicas |-> 0]
    /\ rWal = [r \in Replicas |-> <<>>]
    /\ rBase = [r \in Replicas |-> 0]
    /\ chan = [r \in Replicas |-> <<>>]
    /\ ackq = [r \in Replicas |-> <<>>]
    /\ queue = [r \in Replicas |-> <<>>]
    /\ mode = [r \in Replicas |-> "live"]
    /\ wm = [r \in Replicas |-> <<0, 0>>]
    /\ cursor = [r \in Replicas |-> 0]
    /\ resyncBase = [r \in Replicas |-> 0]
    /\ adopted = [r \in Replicas |-> 0]
    /\ crashed = FALSE
    /\ truncated = FALSE
    /\ trimmed = FALSE

(***************************************************************************)
(* Backend.Replica.commit_async/4: write the local WAL, hand the batch to   *)
(* every sender, then wait for the ack target.                              *)
(***************************************************************************)
Commit ==
    /\ nextId <= MaxId
    /\ LET off == PTail
           m == Msg(pEpoch, off, nextId)
       IN /\ pWal' = Append(pWal, nextId)
          /\ pSynced' = IF Fsync THEN off + 1 ELSE pSynced
          /\ queue' = [r \in Replicas |-> Append(queue[r], m)]
          /\ chan' = [r \in Replicas |->
                       IF mode[r] = "live" THEN Append(chan[r], m) ELSE chan[r]]
          /\ pending' = Append(pending, [id |-> nextId, t |-> <<pEpoch, off + 1>>])
          /\ nextId' = nextId + 1
    /\ UNCHANGED <<pEpoch, pDropped, pBase, acked, rEpoch, rWal, rBase, ackq, mode, wm,
                   cursor, adopted, crashed, truncated, trimmed, resyncBase>>

(***************************************************************************)
(* Backend.Replica.handle_message/2: pending targets are ordered, so the    *)
(* head completes first.                                                    *)
(***************************************************************************)
CompletePending ==
    /\ pending # <<>>
    /\ DurableCount(pending[1].t) >= NeededAcks
    /\ acked' = acked \cup {pending[1].id}
    /\ pending' = Tail(pending)
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, rEpoch, rWal, rBase, chan, ackq,
                   queue, mode, wm, cursor, resyncBase, adopted, crashed, truncated, trimmed>>

(***************************************************************************)
(* Writer.replicate_group/2: append at the tail, re-ack a duplicate, nack   *)
(* anything else. A nack force_reattaches the sender.                       *)
(***************************************************************************)
DeliverBatch(r) ==
    /\ chan[r] # <<>>
    /\ LET m == chan[r][1]
           tail == RTail(r)
           epochOk == (~EpochStamp) \/ m.epoch = rEpoch[r]
           dup == m.epoch = rEpoch[r] /\ m.off + 1 <= tail
       IN IF dup
            THEN /\ ackq' = [ackq EXCEPT ![r] = Append(@, Ack(<<rEpoch[r], tail>>, m.stale))]
                 /\ UNCHANGED <<rWal, mode, resyncBase>>
            ELSE IF epochOk /\ (m.off = tail \/ ~TailCheck)
              THEN /\ rWal' = [rWal EXCEPT ![r] = Append(@, m.id)]
                   /\ ackq' =
                        [ackq EXCEPT ![r] = Append(@, Ack(<<rEpoch[r], tail + 1>>, m.stale))]
                   /\ UNCHANGED mode
              ELSE /\ mode' = [mode EXCEPT ![r] = "detached"]
                   /\ UNCHANGED <<rWal, ackq, resyncBase>>
    /\ chan' = [chan EXCEPT ![r] = Tail(@)]
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, acked, pending, rEpoch, rBase,
                   queue, wm, cursor, adopted, crashed, truncated, trimmed, resyncBase>>

LoseBatch(r) ==
    /\ AllowLoss
    /\ chan[r] # <<>>
    /\ chan' = [chan EXCEPT ![r] = Tail(@)]
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, acked, pending, rEpoch,
                   rWal, rBase, ackq, queue, mode, wm, cursor, resyncBase, adopted, crashed, truncated, trimmed>>

(***************************************************************************)
(* Sender.handle_info({:replica_ack, watermark}, _): advance the member's   *)
(* watermark and drop the batches it covers.                                *)
(***************************************************************************)
DeliverAck(r) ==
    /\ ackq[r] # <<>>
    /\ IF ackq[r][1].stale
         THEN UNCHANGED <<wm, queue, resyncBase>>
         ELSE /\ wm' = [wm EXCEPT ![r] = WmMax(@, ackq[r][1].wm)]
              /\ queue' = [queue EXCEPT ![r] = DropAcked(@, ackq[r][1].wm)]
    /\ ackq' = [ackq EXCEPT ![r] = Tail(@)]
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, acked, pending, rEpoch,
                   rWal, rBase, chan, mode, cursor, adopted, crashed, truncated, trimmed, resyncBase>>

(***************************************************************************)
(* Sender.force_reattach/1: writer death, a nack, an ack stall.             *)
(***************************************************************************)
Detach(r) ==
    /\ mode[r] # "detached"
    /\ mode' = [mode EXCEPT ![r] = "detached"]
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, acked, pending, rEpoch,
                   rWal, rBase, chan, ackq, queue, wm, cursor, resyncBase, adopted, crashed, truncated, trimmed>>

(***************************************************************************)
(* Sender.handle_cast/2, the :max_sender_bytes branch: drop the queue and   *)
(* re-attach. next_needed/1 then falls back to the primary tail.            *)
(***************************************************************************)
Overflow(r) ==
    /\ queue[r] # <<>>
    /\ queue' = [queue EXCEPT ![r] = <<>>]
    /\ mode' = [mode EXCEPT ![r] = "detached"]
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, acked, pending, rEpoch,
                   rWal, rBase, chan, ackq, wm, cursor, adopted, crashed, truncated, trimmed, resyncBase>>

(***************************************************************************)
(* Sender.reconcile/2 on attach. The remote tail is compared against the    *)
(* primary's own WAL, not against the sender's unacked queue: a replica     *)
(* whose ack is merely still in flight holds a valid prefix and must not be *)
(* wiped. What it already has is dropped from the queue and adopted as its  *)
(* watermark. A replica *below* the primary's trimmed base needs bytes      *)
(* nobody has, so it is discarded and rebased on the primary's base.        *)
(* EpochStamp gates the comparison as well as the writer's accept rule:     *)
(* without it the sender ignores the epoch entirely.                        *)
(***************************************************************************)
Attach(r) ==
    /\ mode[r] = "detached"
    /\ LET epochOk == IF EpochStamp THEN rEpoch[r] = pEpoch ELSE TRUE
           remote == RTail(r)
           tail == <<pEpoch, remote>>
       IN IF ~epochOk \/ remote > PTail \/ remote < pBase
            THEN /\ rWal' = [rWal EXCEPT ![r] = <<>>]
                 /\ rBase' = [rBase EXCEPT ![r] = pBase]
                 /\ rEpoch' = [rEpoch EXCEPT ![r] = pEpoch]
                 /\ mode' = [mode EXCEPT ![r] = "resync"]
                 /\ cursor' = [cursor EXCEPT ![r] = pBase]
                 /\ resyncBase' = [resyncBase EXCEPT ![r] = pBase]
                 /\ chan' = [chan EXCEPT ![r] = MarkStale(@)]
                 /\ UNCHANGED <<queue, wm, resyncBase>>
            ELSE IF remote < NextNeeded(r)
              THEN /\ mode' = [mode EXCEPT ![r] = "resync"]
                   /\ cursor' = [cursor EXCEPT ![r] = Max2(remote, pBase)]
                   /\ resyncBase' = [resyncBase EXCEPT ![r] = pBase]
                   /\ chan' = [chan EXCEPT ![r] = MarkStale(@)]
                   /\ UNCHANGED <<rWal, rBase, rEpoch, queue, wm, resyncBase>>
              ELSE /\ mode' = [mode EXCEPT ![r] = "live"]
                   /\ queue' = [queue EXCEPT ![r] = DropAcked(@, tail)]
                   /\ wm' = [wm EXCEPT ![r] = WmMax(@, tail)]
                   /\ chan' =
                        [chan EXCEPT ![r] = MarkStale(@) \o DropAcked(queue[r], tail)]
                   /\ UNCHANGED <<rWal, rBase, rEpoch, cursor, resyncBase>>
    /\ ackq' = [ackq EXCEPT ![r] = MarkStale(@)]
    /\ adopted' = [adopted EXCEPT ![r] = pEpoch]
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, acked, pending,
                   crashed, truncated, trimmed, resyncBase>>

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
         ELSE IF cursor[r] >= resyncBase[r] /\ cursor[r] < PTail
           THEN /\ chan' =
                     [chan EXCEPT ![r] =
                        Append(@, Msg(pEpoch, cursor[r], Full[cursor[r] + 1]))]
                /\ cursor' = [cursor EXCEPT ![r] = cursor[r] + 1]
                /\ UNCHANGED mode
           ELSE /\ mode' = [mode EXCEPT ![r] = "detached"]
                /\ UNCHANGED <<chan, cursor, resyncBase>>
    /\ UNCHANGED <<pEpoch, pDropped, pWal, pBase, pSynced, nextId, acked, pending, rEpoch,
                   rWal, rBase, ackq, queue, wm, resyncBase, adopted, crashed, truncated, trimmed>>

(***************************************************************************)
(* The primary loses the WAL tail it never datasynced, then restarts. Every *)
(* sender restarts with an empty queue and a zero watermark.                *)
(*                                                                          *)
(* With HealOnOpen, Backend.Replica.open/2 asks every replica for its tail   *)
(* and pulls back what it is missing from the furthest one on its own epoch  *)
(* that starts at or below the primary's base. Members agree at every shared *)
(* logical offset, so the healed WAL is that replica's bytes from the        *)
(* primary's base onward. The pull is datasynced, hence pSynced follows it.  *)
(***************************************************************************)
Crash ==
    /\ AllowCrash
    /\ ~crashed
    /\ LET lost == SubSeq(pWal, 1, pSynced - pBase)
           best == CHOOSE r \in Replicas :
                     \A other \in Replicas : RTail(other) <= RTail(r)
           healed == IF /\ HealOnOpen
                        /\ rEpoch[best] = pEpoch
                        /\ rBase[best] <= pBase
                        /\ RTail(best) > pBase + Len(lost)
                       THEN SubSeq(rWal[best], pBase - rBase[best] + 1,
                                   RTail(best) - rBase[best])
                       ELSE lost
       IN /\ pWal' = healed
          /\ pSynced' = pBase + Len(healed)
    /\ crashed' = TRUE
    /\ pending' = <<>>
    /\ queue' = [r \in Replicas |-> <<>>]
    /\ chan' = [r \in Replicas |-> <<>>]
    /\ ackq' = [r \in Replicas |-> <<>>]
    /\ mode' = [r \in Replicas |-> "detached"]
    /\ wm' = [r \in Replicas |-> <<0, 0>>]
    /\ cursor' = [r \in Replicas |-> pBase]
    /\ resyncBase' = [r \in Replicas |-> pBase]
    /\ UNCHANGED <<pEpoch, pDropped, pBase, nextId, acked, rEpoch, rWal, rBase,
                   adopted, truncated, trimmed, resyncBase>>

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
    /\ pDropped' = <<>>
    /\ pBase' = 0
    /\ pSynced' = 0
    /\ acked' = {}
    /\ pending' = <<>>
    /\ queue' = [r \in Replicas |-> <<>>]
    /\ truncated' = TRUE
    /\ \E reached \in SUBSET Replicas :
         /\ rWal' = [r \in Replicas |-> IF r \in reached THEN <<>> ELSE rWal[r]]
         /\ rBase' = [r \in Replicas |-> IF r \in reached THEN 0 ELSE rBase[r]]
         /\ rEpoch' = [r \in Replicas |->
                        IF r \in reached THEN pEpoch + 1 ELSE rEpoch[r]]
         /\ adopted' = [r \in Replicas |->
                         IF r \in reached THEN pEpoch + 1 ELSE adopted[r]]
    /\ UNCHANGED <<nextId, chan, ackq, mode, wm, cursor, resyncBase, crashed, trimmed>>

(***************************************************************************)
(* Backend.Replica.trim/2: drop entries below an ack-durable point, then    *)
(* pass the resulting logical byte base on to each replica. The :erpc is    *)
(* best-effort, exactly like the truncate one, so an arbitrary subset is    *)
(* reached. A replica whose tail is already below the new base is left      *)
(* alone — Local.trim_bytes/2 refuses it — and gets rebased when its sender *)
(* next attaches.                                                          *)
(*                                                                          *)
(* Logical offsets do not move, so nothing already on the wire changes. The *)
(* dropped ids leave `acked`: the caller asked for them to go.              *)
(*                                                                          *)
(* The committer drains its pipeline before trimming, so no commit is still *)
(* in flight when the cut lands — hence the `pending = <<>>` guard.         *)
(***************************************************************************)
Trim ==
    /\ AllowTrim
    /\ ~trimmed
    /\ pending = <<>>
    /\ DurableThrough > pBase
    /\ \E cut \in 1..Min(Len(pWal), DurableThrough - pBase) :
         LET base == pBase + cut IN
         /\ pBase' = base
         /\ pWal' = SubSeq(pWal, cut + 1, Len(pWal))
         /\ pDropped' = pDropped \o SubSeq(pWal, 1, cut)
         /\ acked' = acked \ {pWal[k] : k \in 1..cut}
         /\ \E reached \in SUBSET Replicas :
              /\ rWal' = [r \in Replicas |->
                           IF r \in reached /\ rBase[r] < base /\ RTail(r) >= base
                             THEN SubSeq(rWal[r], base - rBase[r] + 1, Len(rWal[r]))
                             ELSE rWal[r]]
              /\ rBase' = [r \in Replicas |->
                            IF TrimRebases /\ r \in reached /\ rBase[r] < base
                               /\ RTail(r) >= base
                              THEN base
                              ELSE rBase[r]]
    /\ trimmed' = TRUE
    /\ UNCHANGED <<pEpoch, pSynced, nextId, pending, rEpoch, chan, ackq,
                   queue, mode, wm, cursor, resyncBase, adopted, crashed, truncated>>

Next ==
    \/ Commit
    \/ CompletePending
    \/ Crash
    \/ Truncate
    \/ Trim
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
      \A L \in Max2(pBase, rBase[r])..(Min(PTail, RTail(r)) - 1) :
        pWal[L - pBase + 1] = rWal[r][L - rBase[r] + 1]

(* Reads are served from the local WAL, so an acked batch must still be     *)
(* readable from the primary.                                              *)
AckedInPrimary ==
    \A id \in acked : \E i \in 1..Len(pWal) : pWal[i] = id

(***************************************************************************)
(* Backend.Replica.durable_offset/1: the furthest offset that NeededAcks     *)
(* member watermarks agree on. Ungated, a reader sees the whole local WAL.   *)
(***************************************************************************)
Readable == IF GateReads THEN DurableThrough ELSE PTail

(* Ground truth, not the primary's belief: how many members actually hold    *)
(* byte i in the current epoch. The primary holds every byte it can read.    *)
HoldersOf(L) ==
    1 + Cardinality({r \in Replicas :
                      rEpoch[r] = pEpoch /\ rBase[r] <= L /\ L < RTail(r)})

(* Everything a reader can see is held by an ack policy's worth of members.  *)
(* Checking against the replica WALs rather than the watermarks means a      *)
(* stale watermark fails this too.                                          *)
ReadsAreDurable == \A L \in pBase..(Readable - 1) : HoldersOf(L) >= NeededAcks

(* Backend.Replica tracks the newest epoch each replica confirmed, and       *)
(* replica_status/3 reports it. The primary must never claim an epoch a      *)
(* replica has not persisted.                                               *)
AdoptedIsHonest == \A r \in Replicas : adopted[r] <= rEpoch[r]

(* And a replica reported as promotable must not hold pre-truncate data.     *)
PromotableIsClean ==
    \A r \in Replicas :
      adopted[r] = pEpoch =>
        \A L \in Max2(pBase, rBase[r])..(Min(PTail, RTail(r)) - 1) :
          pWal[L - pBase + 1] = rWal[r][L - rBase[r] + 1]

(* A replica never trims further than the primary: it only ever adopts the  *)
(* primary's base, whether by propagation or by a rebase at attach.         *)
ReplicaBaseNotAhead == \A r \in Replicas : rBase[r] <= pBase

(* A resync never reads outside the file it opened. It may sit below the    *)
(* current base, still reading the inode a trim unlinked, but it can never  *)
(* start below the base its own fd began at — that is what the clamp in     *)
(* start_resync/2 buys.                                                     *)
CursorInRange ==
    \A r \in Replicas :
      mode[r] = "resync" =>
        (cursor[r] >= resyncBase[r] /\ cursor[r] <= PTail)

(* The weaker claim: an acked batch still exists on some member.            *)
AckedSomewhere ==
    \A id \in acked :
      \/ \E i \in 1..Len(pWal) : pWal[i] = id
      \/ \E r \in Replicas : \E i \in 1..Len(rWal[r]) : rWal[r][i] = id

=============================================================================
