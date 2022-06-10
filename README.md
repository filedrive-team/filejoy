## Filejoy

**Induction**

Filejoy is a lightweight ipfs node. It only has a small subset of cmds within ipfs node, because filejoy mainly focus on data transfer among libp2p nodes. It also has some customized cmds to deal with large datasets transferring on the networks.

The filejoy node can join the public ipfs network, also can organize private ipfs networks according to different fingerprints. 

Filejoy supports various storage types, like badgerds, go-ds-cluster, remoteds, erasure-bs. Badgerds storage is for single point usage, while go-ds-cluster and erasure-bs can build large size storage base on multiple storage nodes who work together as a cluster. Remoteds is another experiment that using other node's storage for some node does not have or unwilling to use their own local storage.

Filejoy also has a lightweight gateway to support data fetch from browser.

**tips**

Add a file to network
```shell
$ # same effect as ipfs add /path/to/file, but a little slow
$ ./filejoy add /path/to/file
$ # read more bytes and more parallel works, will be faster
$ ./filejoy add2 /path/to/file
```