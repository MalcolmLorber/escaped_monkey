# escaped_monkey

Implementation of the [zookeper](https://zookeeper.apache.org/doc/r3.4.1/zookeeperInternals.html#sc_atomicBroadcast) atomic broadcast system. For more information on the atomic broadcast protocol implemented here, read (this paper)[http://www.tcs.hut.fi/Studies/T-79.5001/reports/2012-deSouzaMedeiros.pdf].

Pair programmed by Adam Freeman and Malcolm Lorber.

To run, run `client.py <number>` on each client. Each client can create, append to, read, or delete files, with the syntax `<command> <filename> [<contents>]`.
