# hyperfs

A content-addressable union file system that replicates using [hyperlog](https://github.com/mafintosh/hyperlog) and is build on top of fuse, leveldb, and node

![logo.png](logo.png)

## Usage

First install hyperfs from npm

``` sh
npm install -g hyperfs
```

hyperfs requires fuse. If your installation fails make sure you have the [fuse requirements](https://github.com/mafintosh/fuse-bindings#requirements) installed for your platform.

``` sh
hyperfs create test # create a new fs volume
hyperfs mount test ./mnt # mount test on ./mnt
```

Now open a the folder ./mnt in your favorite file explorer and start making some changes.

Using the terminal:

``` sh
mkdir mnt/test
echo hello world > mnt/test/hello.txt
```

Now quit your file explorer and go back to the terminal where you ran `hyperfs mount ...`.
Hit CTRL-C to unmount the volume.

Now lets snapshot that volume so we can replicate it. Snapshotting just makes a readonly layer
of the changes you've made. You can use a snapshot as a base fs for a new volume

``` sh
hyperfs snapshot test
```

This will print the snapshot hash when it succeeds.
Now lets replicate the file system to another machine.

``` sh
# assuming hyperfs is installed on example.com
hyperfs replicate ssh://user@example.com
```

Now ssh into example.com and run

```
hyperfs create test --node=<snapshot-hash-from-before>
hyperfs mount test mnt
```

Now if you enter ./mnt you'll see that its the volume from your local machine.

## A container file system

You can use hyperfs to build your own docker-like container platform

``` sh
npm i mini-container -g # a minimal container runtime
apt-get install debootstrap # for installing base distros into a folder
# create an ubuntu volume
hyperfs create ubuntu
# execute debootstrap on this volume. installs base ubuntu trusty
# note: this will take a while (> 20 mins)
hyperfs exec ubuntu 'debootstrap --variant=buildd --arch amd64 trusty . http://archive.ubuntu.com/ubuntu/'
# snapshot ubuntu so we can use it later for other containers
hyperfs snapshot ubuntu -m 'ubuntu trusty core installation'
# create a container volume that inherits from our ubuntu base (your hash might be different)
hyperfs create my-container --node=674a896ec3477d921429dd900da0bab9e32b23aa7f8509c82f1d8b39f42678fe
# install git and curl in our container volume using mini-container
hyperfs exec my-container 'mini-container "apt-get update"'
hyperfs exec my-container 'mini-container "apt-get -y install git curl"'
# snapshot the new container so we can share it
hyperfs snapshot my-container
```

Now to run a bash session inside our container locally we just do

``` sh
# <ctrl-d> to exit :)
hyperfs exec my-container 'mini-container "/bin/bash"'
```

Or to replicate our containers we just do

``` sh
hyperfs replicate ssh://user@remote.com
ssh user@remote.com
hyperfs create my-container --node=<my-container-snapshot-hash-from-above>
hyperfs exec my-container 'mini-container "/bin/bash"'
```

The above example only works on Linux but since hyperfs is only a file system it
would work on OSX too assuming you changed debootstrap and the other commands to the OSX equivalents.

## Content addressed

hyperfs is content addressed on the file level. This means that if you install ubuntu
twice on two different volumes most of the data will only be stored once. This also means
that if you install ubuntu and replicate to another peer that installed ubuntu independently
replication will probably be really fast since almost all of the data is shared.

## Help

```
hyperfs # prints help
```

## License

MIT
