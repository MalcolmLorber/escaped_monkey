# escaped_monkey

Implementation of the [zookeper](https://zookeeper.apache.org/doc/r3.4.1/zookeeperInternals.html#sc_atomicBroadcast) atomic broadcast system.

Pair programmed by Adam Freeman and Malcolm Lorber.

To run, run `client.py <number>` on each client. Each client can create, append to, read, or delete files, with the syntax `<command> <filename> [<contents>]`.
