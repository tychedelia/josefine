# josefine

> So perhaps we shall not miss so very much after all, while Josephine, for her part, 
> delivered from earthly afflictions, which however to her mind are the privilege of 
> chosen spirits, will happily lose herself in the countless throng of the heroes of 
> our people, and soon, since we pursue no history, be accorded the heightened relief
> of being forgotten along with all her brethren.

\- Franz Kafka, *Josefine, die Sängerin oder Das Volk der Mäuse*

### Project Description

A toy implementation of [Kafka](https://kafka.apache.org/), a distributed, replicated
event stream, using an implementation of [Chained Raft](https://decentralizedthoughts.github.io/2021-07-17-simplifying-raft-with-chaining/)
for managing cluster state.

Josefine speaks the [Kafka wire protocol](https://kafka.apache.org/protocol.html), using
100% Rust implementation of the protocol, [kafka-protocol-rs](https://github.com/tychedelia/kafka-protocol-rs).

Traditionally, Kafka has used ZooKeeper in order to maintain cluster state and 
configuration, while storage and partitioning of the event log itself
was handled by Kafka brokers. Over time, more and more operations in the Kafka API
have come to be routed through the brokers themselves, and there is currently an
[experimental version](https://developer.confluent.io/learn/kraft/) of Kafka 
that removes the ZooKeeper dependency entirely.

Josefine maintains a Raft cluster comprised of all brokers. Leadership elections
in the logical Raft cluster are independent of any kind of replication of the data
at the level of the API.

## Chained Raft

The algorithm described in the [original Raft paper](https://raft.github.io/raft.pdf) is 
a replicated log, where each entry is tagged with an index in the log and a leadership
term at which it was written. A consequence of this is that when nodes fail and elections
are triggered in the Raft cluster, sometimes portions of a node's log must be rewritten
with the latest up-to-date log sent from a new leader. Ensuring this happens correctly
is one of the most difficult parts of implementing Raft correctly.

On the other hand, in Chained Raft, log entries become "blocks" in an acyclic graph, which
only maintain a pointer to the next block. As the paper illustrates, by maintaining a reference
to the current head of the chain, we can always walk backwards pointer by pointer to find the
valid current state of the chain. Rather than having to overwrite existing entries, un-replicated
blocks simply become dead branches of the tree that can be garbage collected later.

This is particularly well suited for something like Kafka, which in many smaller installations
has relatively infrequent cluster state changes. Since the replication of data is handled by 
the brokers themselves and not a state machine in Raft. Cluster metadata is mostly used for things
like ensuring topics and broker assignment to particular partitions.

