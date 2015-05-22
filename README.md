# hyperfs

A content-addressable union file system that replicates and is build on top of fuse, leveldb, and node

```
npm install -g hyperfs
```

Notice: This is HIGHLY experimental

## Usage

```
hyperfs create test # create a new fs volume
hyperfs mount test ./mnt # mount test on ./mnt
```

Now open a the folder ./mnt in your favorite file explorer and start making some changes.

Using the terminal:

```
mkdir mnt/test
echo hello world > mnt/test/hello.txt
```

Now quit your file explorer and go back to the terminal where you ran `hyperfs mount ...`.
Hit CTRL-C to unmount the volume.

Now lets snapshot that volume so we can replicate it. Snapshotting just makes a readonly layer
of the changes you've made. You can use a snapshot as a base fs for a new volume

```
hyperfs snapshot test
```

This will print the snapshot hash when it succeeds.
Now lets replicate the file system to another machine.

```
# assuming hyperfs is installed on example.com
hyperfs replicate ssh://user@example.com
```

Now ssh into example.com and run

```
hyperfs create test --node=<snapshot-hash-from-before>
hyperfs mount test mnt
```

Now if you enter ./mnt you'll see that its the volume from your local machine.

## Help

```
hyperfs # prints help
```

## License

MIT
