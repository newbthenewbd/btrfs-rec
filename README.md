This is basically a mirror of the btrfs-rec tool by Luke T. Shumaker, available under https://git.lukeshu.com/btrfs-progs-ng/, **along with modifications** that I found necessary to make it work. For my particular case of the Btrfs Messup - your mileage may vary. :)

[I also provide a prebuilt version for Linux.](https://github.com/newbthenewbd/btrfs-rec/releases/tag/v0.1.0)

But if you somehow find yourself in deep btrfs troubles on macOS or anything, building it is easy! Just [get the Go toolchain installed](https://go.dev/doc/install), then run:
```
git clone https://github.com/newbthenewbd/btrfs-rec.git
cd btrfs-rec/cmd/btrfs-rec
go build
./btrfs-rec # yay!
```

---

*Original README follows*

---

To: linux-btrfs@vger.kernel.org
From: Luke T. Shumaker <lukeshu@lukeshu.com>
Subject: btrfs-rec: Recover (data from) a broken btrfs filesystem

Inspired by a mis-typed `dd` command, for the last year I've been
working on a tool for recovering corrupt btrfs filesystems; at first
idly here and there, but more actively in the last few months.  I hope
to get it incorporated into btrfs-progs, though perhaps that is
problematic for a few reasons I'll get to.  If the code can't be
incorporated into btrfs-progs, at least the ideas and algorithms
should be.

    https://git.lukeshu.com/btrfs-progs-ng/

Highlights:

 - In general, it's more tolerant of corrupt filesystems than
   `btrfs check --repair`, `btrfs rescue` or `btrfs restore`.

 - `btrfs-rec inspect rebuild-mappings` is a better
   `btrfs rescue chunk-recover`.

 - `btrfs-rec inspect rebuild-trees` can re-attach lost branches to
   broken B+ trees.

 - `btrfs-rec inspect mount` is a read-only FUSE implementation of
   btrfs.  This is conceptually a replacement for `btrfs restore`.

 - It's entirely written in Go.  I'm not saying that's a good thing,
   but it's an interesting thing.

Hopefully some folks will find it useful, or at least neat!

    1.      Motivation
    2.      Overview of use
    3.      Prior art
    4.      Internals/Design
    4.1.      Overview of the source tree layout
    4.2.      Base decisions: CLI structure, Go, JSON
    4.3.      Algorithms
    4.3.1.      The `rebuild-mappings` algorithm
    4.3.2.      The `--rebuild` algorithm
    4.3.2.1.      rebuilt forrest behavior
    4.3.2.2.      rebuilt individual tree behavior
    4.3.3.      The `rebuild-trees` algorithm
    4.3.3.1.      initialization
    4.3.3.2.      the main loop
    4.3.3.3.      graph callbacks
    5.      Future work
    6.      Problems for merging this code into btrfs-progs

# 1. Motivation

Have you ever ended up with a corrupt btrfs filesystem (through no
fault of btrfs itself, but perhaps a failing drive, or a mistaken `dd`
invocation)?  Surely losing less than 100MB of data from a drive
should not render hundreds of GB of perfectly intact data unreadable!
And yet, the existing tools are unable to even attempt to read that
data:

    $ btrfs check --repair --force dump-zero.1.img
    enabling repair mode
    Opening filesystem to check...
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    bad tree block 1048576, bytenr mismatch, want=1048576, have=11553381380038442733
    ERROR: cannot read chunk root
    ERROR: cannot open file system

or

    $ btrfs check --init-extent-tree --force dump-zero.1.img
    Opening filesystem to check...
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    bad tree block 1048576, bytenr mismatch, want=1048576, have=11553381380038442733
    ERROR: cannot read chunk root
    ERROR: cannot open file system

or

    $ btrfs check --init-csum-tree --force dump-zero.1.img
    Creating a new CRC tree
    Opening filesystem to check...
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    bad tree block 1048576, bytenr mismatch, want=1048576, have=11553381380038442733
    ERROR: cannot read chunk root
    ERROR: cannot open file system

or

    $ btrfs rescue chunk-recover dump-zero.1.img
    Scanning: DONE in dev0
    corrupt node: root=1 block=160410271744 slot=0, corrupt node: root=1 block=160410271744, nritems too large, have 39 expect range [1,0]
    Couldn't read tree root
    open with broken chunk error

or

    $ btrfs rescue zero-log dump-zero.1.img
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    ERROR: cannot read chunk root
    ERROR: could not open ctree

or

    $ mkdir out
    $ btrfs restore dump-zero.1.img out
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    bad tree block 1048576, bytenr mismatch, want=1048576, have=11553381380038442733
    ERROR: cannot read chunk root
    Could not open root, trying backup super
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    bad tree block 1048576, bytenr mismatch, want=1048576, have=11553381380038442733
    ERROR: cannot read chunk root
    Could not open root, trying backup super
    ERROR: superblock bytenr 274877906944 is larger than device size 256060514304
    Could not open root, trying backup super

or

    $ btrfs restore --list-roots dump-zero.1.img
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    bad tree block 1048576, bytenr mismatch, want=1048576, have=11553381380038442733
    ERROR: cannot read chunk root
    Could not open root, trying backup super
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    checksum verify failed on 1048576 wanted 0xf81c950a found 0xd66a46e0
    bad tree block 1048576, bytenr mismatch, want=1048576, have=11553381380038442733
    ERROR: cannot read chunk root
    Could not open root, trying backup super
    ERROR: superblock bytenr 274877906944 is larger than device size 256060514304
    Could not open root, trying backup super

or

    $ btrfs-find-root dump-zero.1.img
    WARNING: cannot read chunk root, continue anyway
    Superblock thinks the generation is 6596071
    Superblock thinks the level is 1

Well, have I got a tool for you!

(FWIW, I also tried manipulating the filesystem and patching to tools
to try to get past those errors, only to get a different set of
errors.  Some of these patches I am separately submitting to
btrfs-progs.)

# 2. Overview of use

There are two `btrfs-rec` sub-command groups:
`btrfs-rec inspect SUBCMD` and `btrfs-rec repair SUBCMD`, and you can
find out about various sub-commands with `btrfs-rec help`.  These are
both told about devices or images with the `--pv` flag.

`btrfs-rec inspect SUBCMD` commands open the filesystem read-only, and
(generally speaking) write extracted or rebuilt information to stdout.
`btrfs-rec repair SUBCMD` commands open the filesystem read+write, and
consume information from `btrfs-rec inspect SUBCMD` commands to
actually repair the filesystem (except I haven't actually implemented
any `repair` commands yet...  despite the lack of `repair` commands, I
believe that `btrfs-rec` is already a useful because of the `btrfs-rec
inspect mount` command to get data out of the broken filesystem).
This split allows you to try things without being scared by WARNINGs
about not using these tools unless you're an expert or have been told
to by a developer.

In the broken `dump-zero.1.img` example above (which has a perfectly
intact superblock, but a totally broken `CHUNK_TREE`), to "repair" it
I'd:

 1. Start by using `btrfs-rec inspect rebuild-mappings` to rebuild the
    broken chunk/dev/blockgroup trees:

        $ btrfs-rec inspect rebuild-mappings \
            --pv=dump-zero.1.img \
            > mappings-1.json

 2. If it only mostly succeeds, but on stderr tells us about a few
    regions of the image that it wasn't able to figure out the chunks
    for.  Using some human-level knowledge, you can write those
    yourself, inserting them into the generated `mappings.json`, and
    ask `rebuild-mappings` to normalize what you wrote:

        $ btrfs-rec inspect rebuild-mappings \
            --pv=dump-zero.1.img \
            --mappings=<(sed <mappings-1.json \
                -e '2a{"LAddr":5242880,"PAddr":{"Dev":1,"Addr":5242880},"Size":1},' \
                -e '2a{"LAddr":13631488,"PAddr":{"Dev":1,"Addr":13631488},"Size":1},') \
            > mappings-2.json

 3. Now that it has functioning chunk/dev/blockgroup trees, we can use
    `btrfs-rec inspect rebuild-trees` to rebuild other trees that rely
    on those:

        $ btrfs-rec inspect rebuild-mappings \
            --pv=dump-zero.1.img \
            --mappings=mappings-2.json \
            > trees.json

 4. Now that (hopefully) everything that was damaged has been
    reconstructed, we can use `btrfs-rec inspect mount` to mount the
    filesystem read-only and copy out our data:

        $ mkdir mnt
        $ sudo btrfs-rec inspect mount \
            --pv=dump-zero.1.img \
            --mappings=mappings-2.json \
            --trees=trees.json \
            ./mnt

This example is fleshed out more (and the manual edits to
`mappings.json` explained more) in `./examples/main.sh`.

# 3. Prior art

Comparing `btrfs-rec inspect mount` with the existing
https://github.com/adam900710/btrfs-fuse project:

 - Again, mine has better fault tolerance
 - Mine is read-only
 - Mine supports xattrs ("TODO" in Adam's)
 - Mine supports separate inode address spaces for subvolumes; Adam's
   doesn't due to limitations in FUSE, mine works around this by
   lazily setting up separate mountpoints for each subvolume (though
   this does mean that the process needs to run as root, which is a
   bummer).

# 4. Internals/Design

## 4.1. Overview of the source tree layout

 - `examples/` has example scripts showing how to use `btrfs-rec`.

 - `lib/btrfs/` is the core btrfs implementation.

 - `lib/btrfscheck/` and `lib/btrfsutil/` are libraries for
   "btrfs-progs" type programs, that are userland-y things that I
   thought should be separate from the core implementation; something
   that frustrated me about libbtrfs was having to figure out "is this
   thing here in support of btrfs bits-on-disk, or in support of a
   higher-level 'how btrfs-progs wants to think about things'?"

 - `cmd/btrfs-rec/` is where the command implementations live.  If a
   sub-command fits in a single file, it's
   `cmd/btrfs-rec/inspect_SUBCMD.go`, otherwise, it's in a separate
   `cmd/btrfs-rec/inspect/SUBCMD/` package.

 - `lib/textui/` is reasonably central to how the commands implement a
   text/CLI user-interface.

 - `lib/binstruct/`, `lib/diskio/`, and `lib/streamio/` are
   non-btrfs-specific libraries related to the problem domain.

 - `lib/containers/`, `lib/fmtutil/`, `lib/maps/`, `lib/slices/`, and
   `lib/profile/` are all generic Go libraries that have nothing to do
   with btrfs or the problem domain, but weren't in the Go standard
   library and I didn't find/know-of exiting implementations that I
   liked.  Of these, all but `containers` are pretty simple utility
   libraries.  Also, some of these things have been added to the
   standard library since I started the project.

## 4.2. Base decisions: CLI structure, Go, JSON

I started with trying to enhance btrfs-progs, but ended up writing a
wholy new program in Go, for several reasons:

 - writing a new thing: I was having to learn both the btrfs-progs
   codebase and how btrfs-bits-on-disk work, and it got to the point
   that I decided I should just focus on learning btrfs-bits-on-disk.

 - writing a new thing: It was becoming increasingly apparent to me
   that it was going to be an uphill-fight of having recovery-tools
   share the same code as the main-tools, as the routines used by the
   main-tools rightly have validity checks, where recovery-tools want
   to say "yes, I know it's invalid, can you give it to me anyway?".

 - writing it in not-C: I love me some C, but higher level languages
   are good for productivity.  And I was trying to write a whole lot
   of code at once, I needed a productivity boost.

 - writing it in not-C: This forced me to learn btrfs-bits-on-disk
   better, instead of just cribbing from btrfs-progs.  That knowledge
   is particularly important for having ideas on how to deal with
   corrupt bits-on-disk.

 - writing it in Go: At the time I started, my day job was writing Go,
   so I had Go swapped into my brain.  And Go still feels close to C
   but provides *a lot* of niceness and safety over C.

It turned out that Go was perhaps not the best choice, but we'll come
back to that.

I wanted to separate things into a pipeline.  For instance: Instead of
`btrfs rescue chunk-recover` trying to do everything to rebuild a
broken chunk tree, I wanted to separate I/O from computation from
repairs.  So I have `btrfs-rec inspect rebuild-mappings scan` that
reads all the info necessary to rebuild the chunk tree, then dump that
as a 2GB glob of JSON.  Then I can feed that JSON to `btrfs-rec
inspect rebuild-mappings process` which actually rebuilds the mappings
in the chunk tree, and dumps them as JSON.  And then other commands
can consume that `mappings.json` to use that instead of trying to read
the chunk tree from the actual FS, so that you don't have to make
potentially destructive writes to inspect an FS with a broken chunk
tree, and can inspect it more forensically.  Or then use
`btrfs-rec repair SOME_SUBCMD_I_HAVENT_WRITTEN_YET` to write that
chunk tree in `mappings.json` back to the filesystem.

(But also, the separate steps thing was useful just so I could iterate
on the algorithms of `rebuild-mappings process` separately from having
to scan the entire FS)

So, I made the decision that `btrfs-rec inspect SUBCMD` commands
should all only open the FS read-only, and output their work to a
separate file; that writing that info back to the FS should be
separate in `btrfs-rec repair SUBCMD`.

For connecting those parts of the pipeline, I chose JSON, for a few
reasons:

 - I wanted something reasonably human-readable, so that I could debug
   it easier.

 - I wanted something reasonably human-readable, so that human
   end-users could make manual edits; for example, in
   `examples/main.sh` I have an example of manually editing
   `mappings.json` to resolve a region that the algorithm couldn't
   figure out, but with knowledge of what caused the corruption a
   human can.

 - I didn't want to invent my own DSL and have to handle writing a
   parser.  (This part didn't pay off!  See below.)

 - I wanted something that I thought would have good support in a
   variety of languages, so that if Go is problematic for getting
   things merged upstream it could be rewritten in C (or maybe Rust?)
   piece-meal where each subcommand can be rewritten one at a time.

It turned out that JSON was perhaps not the best choice.

OK, so: Go and/or JSON maybe being mistakes:

 - I spent a lot of time getting the garbage collector to not just
   kill performance.

 - The `btrfs-rec inspect rebuild-mappings SUBCMD` subcommands all
   throw a lot of data through the JSON encoder/decoder, and I learned
   that the Go stdlib `encoding/json` package has memory use that
   grows O(n^2) (-ish? I didn't study the implementation, but that's
   what the curve looks like just observing it) on the size of the
   data being shoved through it, so I had to go take a break and go
   write https://pkg.go.dev/git.lukeshu.com/go/lowmemjson which is a
   mostly-drop-in-replacement that tries to be as close-as possible to
   O(1) memory use.  So I did end up having to write my own parser
   anyway :(

## 4.3. Algorithms

There are 3 algorithms of note in `btrfs-rec`, that I think are worth
getting into mainline btrfs-progs even if the code of `btrfs-rec`
doesn't get in:

 1. The `btrfs-rec inspect rebuild-mappings` algoritithm to rebuild
    information from the `CHUNK_TREE`, `DEV_TREE`, and
    `BLOCK_GROUP_TREE`.

 2. The `btrfs-rec --rebuild` algorithm to cope with reading broken B+
    trees.

 3. The `btrfs-rec inspect rebuild-trees` algorithm to re-attach lost
    branches to broken B+ trees.

### 4.3.1. The `rebuild-mappings` algorithm

(This step-zero scan is `btrfs-rec inspect rebuild-mappings scan`, and
principally lives in `./lib/btrfsutil/scan.go` and
`./cmd/btrfs-rec/inspect/rebuidmappings/scan.go`)

 0. Similar to `btrfs rescue chunk-recover`, scan each device for
    things that look like nodes; keep track of:
     - Checksums of every block on the device
     - Which physical addresses contain nodes that claim to be at a
       given logical addess.
     - Any found Chunk items, BlockGroup items, DevExtent, and CSum
       items.  Keep track of the key for each of these, and for CSum
       items also track the generation.

Create a bucket of the data from Chunks, DevExtents, and BlockGroups;
since these are mostly a Chunk and a DevExtent+BlockGroup store pretty
much the same information; we can use one to reconstruct the other.
How we "merge" these and handle conflicts is in
`./lib/btrfs/btrfsvol/lvm.go:addMapping()`, I don't think this
part is particularly clever, but given that `btrfs rescue
chunk-recover` crashes if it encounters two overlapping chunks, I
suppose I should spell it out:

 - A "mapping" is represented as a group of 4 things:

   + logical address
   + a list of 1 or more physical addresses (device ID and offset)
   + size, and a Boolean indicator of whether the size is "locked"
   + block group flags, and a Boolean presence-indicator

 - Mappings must be merged if their logical or physical regions
   overlap.

 - If a mapping has a "locked" size, then when merging it may subsume
   smaller mappings with unlocked sizes, but its size cannot be
   changed; trying to merge a locked-size mapping with another mapping
   that is not for a subset region should return an error.

 - If a mapping has block group flags present, then those flags may
   not be changed; it may only be merged with another mapping that
   does not have flags present, or has identical flags.

 - When returning an error because of overlapping non-mergeable
   mappings, just log an error on stderr and keep going.  That's an
   important design thing that is different than normal filesystem
   code; if there's an error, yeah, detect and notify about it, **but
   don't bail out of the whole routine**.  Just skip that one item or
   whatever.

Now that we know how to "add a mapping", let's do that:

(The following main-steps are `btrfs-rec inspect rebuild-mappings
process`, and principally live in
`./cmd/btrfs-rec/inspect/rebuidmappings/process.go`)

 1. Add all found Chunks.

 2. Add all found DevExtents.

 3. Add a phyical:logical mapping of length nodesize for each node
    that was found.

 4. Any mappings from steps 2 or 3 that are missing blockgroup flags
    (that is: they weren't able to be merged with a mapping from step
    1), use the found BlockGroups to fill in those flags.

 5. Now we'll merge all found CSum items into a map of the sums of the
    logical address space.  Sort all of the csum items by generation,
    then by address.  Loop over them in that order, inserting their
    sums into the map.  If two csum items overlap, but agree about the
    sums of the overlapping region, that's fine, just take their
    union.  For overlaps that disagree, items with a newer generation
    kick out items with an older generation.  If disagreeing items
    have the same generation... I don't think that can happen except
    by a filesystem bug (i.e. not by a failing drive or other external
    corruption), so I wasn't too concerned about it, so I just log an
    error on stderr and skip the later-processed item.  See
    `./cmd/btrfs-rec/inspect/rebuidmappings/process_sums_logical.go`.

    Look at regions of the logical address space that meet all the 3
    criteria:

     - we have CSum items for them
     - we have a BlockGroup for them
     - we don't have a Chunk/DevExtent mapping them to the pysical
       address space.

    Pair those CSums up with BlockGroups, and for each BlockGroup,
    search the list of checksums of physical blocks to try to find a
    physical region that matches the logical csums (and isn't already
    mapped to a different logical region).  I used a
    Knuth-Morris-Pratt search, modified to handle holes in the logical
    csum list as wildcards.

    Insert any found mappings into our bucket of mappings.

 6. Do the same again, but with a fuzzy search (we can re-use the csum
    map of the logical address space).  My implementation of this is
    comparatively time and space intensive; I just walk over the
    entire unmapped physical address space, noting what % of match
    each BlockGroup has if placed at that location.  I keep track of
    the best 2 matches for each BlockGroup.  If the best match is
    better than a 50% match, and the second best is less than a 50%
    match, then I add the best match.  In my experience, the best
    match is >90% (or at whatever the maximum percent is for how much
    of the BlockGroup has logical sums), and the second best is 0% or
    1%.  The point of tracking both is that if there isn't a clear-cut
    winner, I don't want it to commit to a potentially wrong choice.

### 4.3.2. The `--rebuild` algorithm

The `--rebuild` flag is implied by the `--trees=trees.json` flag, and
triggers an algorithm that allows "safely" reading from a broken B+
tree, rather than the usual B+ tree lookup and search functions.  I
probably should have tried to understand the `btrfs restore`
algorithm, maybe I reinvented the wheel...

This algorithm requires a list of all nodes on the filesystem; we find
these using the same scan as above (`./lib/btrfsutil/scan.go`), the
same procedure as `btrfs rescue chunk-recover`.

We walk all of those nodes, and build a reasonably lightweight
in-memory graph of all nodes (`./lib/btrfsutil/graph.go`), tracking

 - each node's
   + logical address
   + level
   + generation
   + tree
   + each item's key and size
 - each keypointer's
   + source node
   + source slot within the node
   + tree of the source node
   + destination node
   + destination level implied by the level of the source node
   + destination key
   + destination generation
 - logical addresses and error messages for nodes that are pointed to
   by a keypointer or the superblock, but can't be read (because that
   logical address isn't mapped, or it doesn't look like a node,
   or...)
 - an index such that for a given node we can quickly list both
   keypointers both originating at that node and pointing to that
   node.

#### 4.3.2.1. rebuilt forrest behavior (looking up trees)

(see: `./lib/btrfsutil/rebuilt_forrest.go`)

 - The `ROOT_TREE`, `CHUNK_TREE`, `TREE_LOG`, and `BLOCK_GROUP_TREE`
   (the trees pointed to directy by the superblock) work as you'd
   expect.
 - For other trees, we (as you'd expect) look up the root item in the
   rebuilt `ROOT_TREE`, and then (if rootitem.ParentUUID is non-zero)
   eagerly also look up the parent tree (recursing on ourself).  We
   try to use the `UUID_TREE` tree to help with this, but fall back to
   just doing a linear scan over the `ROOT_TREE`.  If we fail to look
   up the parent tree (or its parent, or a more distant ancestor),
   then (depending on a flag) we either make a note of that, or error
   out and fail to look up the child tree.  For `--rebuild` and
   `--trees=trees.json` we are permissive of this error, and just make
   note of it; but we'll re-use this algorithm in the `rebuild-trees`
   algorithm below, and it needs the more strict handling.
 - When creating the rebuilt individual tree, we start by adding the
   root node specified by the superblock/root-item.  But we may also
   add additional root nodes grafted on to the tree by the
   `--trees=trees.json` flag or by the `rebuild-trees` algorithm
   below.  So a tree may have more than 1 root node.

#### 4.3.2.2. rebuilt individual tree behavior

(see: `./lib/btrfsutil/rebuilt_tree.go`)

In order to read from a tree, we first have to build a few indexes.
We store these indexes in an Adaptive Replacement Cache; they are all
re-buildable based on the tree's list of roots and the above graph; if
we have a bunch of trees we don't need to keep all of this in memory
at once.  Note that this is done 100% with the in-memory graph, we
don't need to read anything from the filesystem during these
procedures.

 - The first index we build is the "node index".  This is an index
   that for every node tells us what root(s) the tree would need to
   have in order for the tree to include that node, and also what the
   highest item key would be acceptable in the node if the tree
   includes that root.  We track both a `loMaxItem` and a `hiMaxItem`,
   in case the tree is real broken and there are multiple paths from
   the root to the node; as these different paths may imply different
   max-item constraints.  Put more concretely, the type of the index
   is:

       map[ nodeID → map[ rootNodeID → {loMaxItem, hiMaxItem} ] ]

   We'll do a loop over the graph, using dynamic-programming
   memoization to figure out ordering and avoid processing the same
   node twice; for each node we'll

   + Check whether the owner-tree is this tree or one of this tree's
      ancestors (and if it's an ancestor, that the node's generation
      isn't after the point that the child tree was forked from the
      parent tree).  If not, we are done processing that node (record
      an empty/nil set of roots for it).

   + Create an empty map of `rootID` → {`loMaxItem`, `hiMaxItem`}.

   + Look at each keypointer that that points at the node and:

     * Skip the keypointer if its expectations of the node aren't met:
       if the level, generation, and min-key constraints don't match
       up.  If the keypointer isn't in the last slot in the source
       node, we also go ahead and include checking that the
       destination node's max-key is under the min-key of the
       keypointer in the next slot, since that's cheap to do now.

     * Skip the keypointer if its source node's owner-tree isn't this
       tree or one of this tree's ancestors (and if it's an ancestor,
       that the node's generation isn't after the point that the child
       tree was forked from the parent tree).

     * dynamic-programming recurse and index the keypointer's source
       node.

     * for every root that would result in the keypointer's source
       node being included in the tree:

       . If the keypointer is in the last slot, look at what the what
         the source node's last-item constraints would be if that root
         is included, and can now check the max-item of our
         destination node.  We check against the `hiMaxItem`; as if
         there is any valid path from the root to this node, then we
         want to be permissive and include it.  If that check fails,
         then we're done with this keypointer.  Also, make node of
         those `loMaxItem` and `hiMaxItem` values, we'll use them
         again in just a moment.

       . Otherwise, set both `loMaxItem` and `hiMaxItem` to 1-under
         the min-item of the keypointer in the next slot.

       . Insert that `loMaxItem` and `hiMaxItem` pair into the
         `rootID` → {`loMaxItem`, `hiMaxItem`} map we created above.
         If an entry already exists for this root (since a broken tree
         might have multiple paths from the root to our node), then
         set `loMaxItem` to the min of the existing entry and our
         value, and `hiMaxItem` to the max.

   + If that `rootID` → {`loMaxItem`, `hiMaxItem`} map is still empty,
     then consider this node to be a (potential) root, and insert
     `rootID=thisNode` -> {`loMaxItem=maxKey`, `hiMaxItem=maxKey`}
     (where `maxKey` is the maximum value of the key datatype).

   + Take that `rootID` → {`loMaxItem`, `hiMaxItem`} map and insert it
     into the index as the entry for this node.

 - The next index we build is the "item index".  This is a "sorted
   map" (implemented as a red-black tree, supporting sub-range
   iteration) of `key` → {`nodeID`, `slotNumber`}; a map that for each
   key tells us where to find the item with that key.

   + Loop over the node index, and for each node check if both (a) it
     has `level==0` (is a leaf node containing items), and (b) its set
     of roots that would include it has any overlap with the tree's
     set of roots.

   + Loop over each of those included leaf nodes, and loop over the
     items in each node.  Insert the `key` → {`nodeId`, `slot`} into
     our sorted map.  If there is already an entry for that key,
     decide which one wins by:

     * Use the one from the node with the owner-tree that is closer to
       this tree; node with owner=thisTree wins over a node with
       owner=thisTree.parent, which would win over a node with
       owner.thisTree.parent.parent.  If that's a tie, then...

     * Use the one from the node with the higher generation.  If
       that's a tie, then...

     * I don't know, I have the code `panic`:

           // TODO: This is a panic because I'm not really sure what the
           // best way to handle this is, and so if this happens I want the
           // program to crash and force me to figure out how to handle it.
           panic(fmt.Errorf("dup nodes in tree=%v: old=%v=%v ; new=%v=%v",
               tree.ID,
               oldNode, tree.forrest.graph.Nodes[oldNode],
               newNode, tree.forrest.graph.Nodes[newNode]))

   Note that this algorithm means that for a given node we may use a
   few items from that node, while having other items from that same
   node be overridden by another node.

 - The final index we build is the "error index".  This is an index of
   what errors correspond to which range of keys, so that we can
   report them, and give an idea of "there may be entries missing from
   this directory" and similar.

   For each error, we'll track the min-key and max-key of the range it
   applies to, the node it came from, and what the error string is.
   We'll store these into an interval tree keyed on that
   min-key/max-key range.

   + Create an empty set `nodesToProcess`.  Now populate it:

     * Once again, we'll loop over the node index, but this time we'll
       only check that there's overlap between the set of roots that
       would include the node and the tree's set of roots.  The nodes
       that are included in this tree, insert both that node itself
       and all node IDs that it has keypointers pointing to into the
       `nodesToProcess` set.

     * Also insert all of the tree's roots into `nodesToProcess`; this
       is in case the superblock/root-item points to an invalid node
       that we couldn't read.

   + Now loop over `nodesToProcess`.  For each node, create an empty
     list of errors.  Use the keypointers pointing to and the min
     `loMaxItem` from the node index to construct a set of
     expectations for the node; this should be reasonably
     straight-forward, given:

     * If different keypointers have disagreeing levels, insert an
       error in to the list, and don't bother with checking the node's
       level.

     * If different keypointers have disagreeing generations, insert
       an error in to the list, and don't bother with checking the
       node's generation.

     * If different keypointers have different min-item expectations,
       use the max of them.

     Then:

     * If the node is a "bad node" in the graph, insert the error
       message associated with it.  Otherwise, check those
       expectations against the node in the graph.

     If the list of error messages is non-empty, then insert their
     concatenation into the interval tree, with the range set to the
     min of the min-item expectations from the keypointers through the
     max of the `hiMaxItem`s from the node index.  If the min min-item
     expectation turns out to be higher than the max `hiMaxItem`, then
     set the range to the zero-key through the max-key.

From there, it should be trivial to implement the usual B+ tree
operations using those indexes; exact-lookup using the item index, and
range-lookups and walks using the item index together with the error
index.  Efficiently searching the `CSUM_TREE` requires knowing item
sizes, so that's why we recorded the item sizes into the graph.

### 4.3.3. The `rebuild-trees` algorithm

The `btrfs inspect rebuild-trees` algorithm finds nodes to attach as
extra roots to trees.  I think that conceptually it's the the simplest
of the 3 algorithms, but turned out to be the hardest to get right.
So... maybe more than the others reference the source code too
(`./cmd/btrfs-rec/inspect/rebuildtrees/`) because I might forget some
small but important detail.

The core idea here is that we're just going to walk each tree,
inspecting each item in the tree, and checking for any items that are
implied by other items (e.g.: a dir entry item implies the existence
of inode item for the inode that it points at).  If an implied item is
not in the tree, but is in some other node, then we look at which
potential roots we could add to the tree that would add that other
node.  Then, after we've processed all of the items in the filesystem,
we go add those various roots to the various trees, keeping track of
which items are added or updated.  If any of those added/updated items
have a version with a newer generation on a different node, see what
roots we could add to get that newer version.  Then add those roots,
keeping track of items that are added/updated.  Once we reach
steady-state with the newest version of each item has been added, loop
back and inspect all added/updated items for implied items, keeping
track of roots we could add.  Repeat until a steady-state is reached.

There are lots of little details in that process, some of which are
for correctness, and some of which are for "it should run in hours
instead of weeks."

#### 4.3.3.1. initialization

First up, we're going to build and in-memory graph, same as above.
But this time, while we're reading the nodes to do that, we're also
going to watch for some specific items and record a few things about
them.

(see: `./cmd/btrfs-rec/inspect/rebuildtrees/scan.go`)

For each {`nodeID`, `slotNumber`} pair that matches one of these item
types, we're going to record:

 - flags:
   + `INODE_ITEM`s: whether it has the `INODE_NODATASUM` flag set
 - names:
   + `DIR_INDEX` items: the file's name
 - sizes:
   + `EXTENT_CSUM` items: the number of bytes that this is a sum for
     (i.e. the item size over the checksum size, times the block size)
   + `EXTENT_DATA` items: the number of bytes in this extent
     (i.e. either the item size minus
     `offsetof(btrfs_file_extent_item.disk_bytenr)` if
     `FILE_EXTENT_INLINE`, or else the item's `num_bytes`).
 - data backrefs:
   - `EXTENT_ITEM`s and `METADATA_ITEM`s: a list of the same length as
     the number of refs embedded in the item; for embeded
     ExtentDataRefs, the list entry is the subvolume tree ID that the
     ExtentDataRef points at, otherwise it is zero.
   - `EXTENT_DATA_REF` items: a list of length 1, with the sole member
     being the subvolume tree ID that the ExtentDataRef points at.

#### 4.3.3.2. the main loop

(see: `./cmd/btrfs-rec/inspect/rebuildtrees/rebuild.go`)

Start with that scan data (graph + info about items), and also a
rebuilt forrest from the above algorithm, but with:

 - the flag set so that it refuses to look up a tree if it can't look
   up all of that tree's ancestors

 - an additional "potential-item index" that is similar to the item
   index.  It is generated the same way and can cache/evict the same
   way; the difference is that we invert the check for if the set of
   roots for a node has overlap with the tree's set of roots; we're
   looking for *potential* nodes that we could add to this tree.

 - some callbacks; we'll get to what we do in these callbacks in a
   bit, but for now, what the callbacks are:

   + a callback that is called for each added/updated item when we add
     a root.

   + a callback that is called whenever we add a root

   + a callback that intercepts looking up a root item

   + a callback that intercepts resolving an UUID to an object ID.

  (The callbacks are in
  `./cmd/btrfs-rec/inspect/rebuildtrees/rebuild_treecb.go`)

We have 5 unordered queues ("work lists"?); these are sets that when
it's time to drain them we'll sort the members and process them in
that order.

 1. the tree queue: a list of tree IDs that we need to crawl
 2. the retry-item queue: for each tree ID, a set of items that we
    should re-process if we add a root to that tree
 3. the added-item queue: a set of key/tree pairs identifying items
    that have been added by adding a root to a tree
 4. the settled-item-queue: a set of key/tree pairs that have have not
    just been added by adding a root, but we've also verified that
    they are the newest-generation item with that key that we could
    add to the tree.
 5. the augment queue: for each item that we want to add to a tree,
    the list of roots that we could add to get that item.

The roots all start out empty, except for the tree queue, which we
seed with the `ROOT_TREE`, the `CHUNK_TREE`, and the
`BLOCK_GROUP_TREE` (It is a "TODO" task that it should probably also
be seeded with the `TREE_LOG`, but as I will say below in the "future
work" section, I don't actually understand the `TREE_LOG`, so I
couldn't implement it).

Now we're going to loop until the tree queue, added-item queue,
settled-item queue, and augment queue are all empty (all queues except
for the retry-item queue).  Each loop "pass" has 3 substeps:

 1. Crawl the trees (drain the tree queue, fill the added-item queue).

 2. Either:

    a. if the added-item queue is non-empty: "settle" those items
       (drain the added-item queue, fill the augment queue and the
       settled-item queue).

    b. otherwise: process items (drain the settled-item queue, fill
       the augment queue and the tree queue)

 3. Apply augments (drain the augment queue and maybe the retry-item
    queue, fill the added-item queue).

OK, let's look at those 3 substeps in more detail:

 1. Crawl the trees; drain the tree queue, fill the added-item queue.

    We just look up the tree in the rebuilt forrest, which will (per
    the above `--rebuild` algorithm) will either fail to look up the
    tree, or succeed, and add to that tree the root node from the
    superblock/root-item.  Because we set an item-added callback, when
    adding that root it will loop over the nodes added by that root,
    and call our callback for each item in one of the added nodes.
    Our callback inserts each item into the added-item queue.  The
    forrest also calls our root-added callback, but because of the way
    this algorithm works, that turns out to be a no-op at this step.

    I mentioned that we added callbacks to intercept the forrest's
    looking up of root items and resolving UUIDs; we override the
    forrest's "lookup root item" routine and "resolve UUID" routine to
    instead of doing normal lookups on the `ROOT_TREE` and
    `UUID_TREE`, use the above `WantXXX` routines that we'll define
    below in the "graph callbacks" section.

    It shouldn't matter what order this queue is processed in, but I
    sort tree IDs numerically.

    The crawling is fairly fast because it's just in-memory, the only
    accesses to disk are looking up root items and resolving UUIDs.

 2. Either:

    a. Settle items from the added-item queue to the settled-item queue
       (and fill the augment queue).

       For each item in the queue, we look in the tree's item index to
       get the {node, slot} pair for it, then we do the same in the
       tree's potential-item index.  If the potential-item index
       contains an entry for the item's key, then we check if the
       potential-item's node should "win" over the queue item's node,
       deciding the "winner" using the same routine as when building
       the item index.  If the potential-item's node wins, then we add
       the potential node's set of roots to the augment queue.  If the
       queue-item's node wins, then we add the item to the
       settled-item queue (except, as an optimization, if the item is
       of a type that cannot possibly imply the existence of another
       item, then we just drop it and don't add it to the settled-item
       queue).

       It shouldn't matter what order this queue is processed in, but
       I sort it numerically by treeID and then by item key.

       This step is fairly fast because it's entirely in-memory,
       making no accesses to disk.

    b. Process items from the settled-item queue (drain the
       settled-item queue, fill the augment queue and the tree queue).

       This step accesses disk, and so the order we process the queue
       in turns out to be pretty important in order to keep our disk
       access patterns cache-friendly.  For the most part, we just
       sort each queue item by tree, then by key.  But, we have
       special handling for `EXTENT_ITEM`s, `METADATA_ITEM`s, and
       `EXTENT_DATA_REF` items: We break `EXTENT_ITEM`s and
       `METADATA_ITEM`s in to "sub-items", treating each ref embedded
       in them as a separate item.  For those embedded items that are
       `EXTENT_DATA_REF`s, and for stand-alone `EXTENT_DATA_REF`
       items, we sort them not with the `EXTENT_TREE` items, but with
       the items of the tree that the extent data ref points at.
       Recall that during the intitial scan step, we took note of
       which tree every extent data ref points at, so we can perform
       this sort without accessing disk yet.  This splitting does mean
       that we may visit/read an `EXTENT_ITEM` or `METADATA_ITEM`
       multiple times as we process the queue, but to do otherwise is
       to solve MinLA, which is NP-hard and also an optimal MinLA
       solution I still think would perform worse than this; there is
       a reasonably lengthy discussion of this in a comment in
       `./cmd/btrfs-rec/inspect/rebuildtrees/rebuild.go:sortSettledItemQueue()`.

       Now we loop over that sorted queue.  In the code, this loop is
       deceptively simple.  Read the item, then pass it to a function
       that tells us what other items are implied by it.  That
       function is large, but simple; it's just a giant table.  The
       trick is how it tells us about implied items; we give it set of
       callbacks that it calls to tell us these things; the real
       complexity is in the callbacks.  These "graph callbacks" will
       be discussed in detail below, but as an illustrative example:
       It may call `.WantOff()` with a tree ID, object ID, item type,
       and offset to specify a precise item that it believes should
       exist.

       If we encounter a `ROOT_ITEM`, add the tree described by that
       item to the tree queue.

    (Both the "can this item even imply the existence of another item"
    check and the "what items are implied by this item" routine are in
    `./lib/btrfscheck/graph.go`)

 3. Apply augments; drain the augment queue (and maybe the retry-item
    queue), fill the added-item queuee.

    It is at this point that I call out that the augment queue isn't
    implemented as a simple map/set like the others, the
    `treeAugmentQueue struct` has special handling for sets of
    different sizes; optimizing the space for empty and len()==1 sized
    sets, and falling back to normal the usual implementation for
    larger sets; this is important because those small sets are the
    overwhelming majority, and otherwise there's no way the program
    would be able to run on my 32GB RAM laptop.  Now that I think
    about it, I bet it would even be worth it to add optimized storage
    for len()==2 sized sets.

    The reason is that each "want" from above is tracked in the queue
    separately; if we were OK merging them, then this optimized
    storage wouldn't be nescessary.  But we keep them separate, so
    that:

    - For all "wants", including ones with empty sets, graph callbacks
      can check if a want has already been processed; avoiding
      re-doing any work (see the description of the graph callbacks
      below).

    - For "wants" with non-empty sets, we can see how many different
      "wants" could be satisfied with a given root, in order to decide
      which root to choose.

    Anyway, we loop over the trees in the augment queue.  For each
    tree we look at that tree's augment queue and look at all the
    choices of root nodes to add (below), and decide on a list to add.
    The we add each of those roots to the tree; the adding of each
    root triggers several calls to our item-added callback (filling
    the added-item queue), and our root-added callback.  The
    root-added callback moves any items from the retry-item queue for
    this tree to the added-item queue.

    How do we decide between choices of root nodes to add?
    `./cmd/btrfs-rec/inspect/rebuildtrees/rebuild.go:resolveTreeAugments()`
    has a good comment explaining the criteria we'd like to optimize
    for, and then code that does an OK-ish job of actually optimizing
    for that:
	
    - It loops over the augment queue for that tree, building a list
      of possible roots, for each possible root making note of 3
      things:

       a. how many "wants" that root satisfies,

       b. how far from treee the root's owner is (owner=tree is a
          distance of 0, owner=tree.parent is a distance of 1,
          owner=tree.parent.parent is a distance of 2, and so on), and
          
       c. what the generation of that root is.
       
    - We sort that list first by highest-count-first, then by
      lowest-distance-first, then by highest-generation-first.
      
    - We create a "return" set and an "illegal" set.  We loop over the
      sorted list; for each possible root if it is in the illegal set,
      we skip it, otherwise we insert it into the return set and for
      each "want" that includes this root we all all roots that
      satisfy that want to the illegal list.

It is important that the rebuilt forrest have the flag set so that it
refuses to look up a tree if it can't look up all of that tree's
ancestors; otherwise the potential-items index would be garbage as we
wouldn't have a good idea of which nodes are OK to consider; but this
does have the downside that it won't even attempt to improve a tree
with a missing parent.  Perhaps the algorithm should flip the flag
once the loop terminates, and then re-seed the tree queue with each
`ROOT_ITEM` from the `ROOT_TREE`?

#### 4.3.3.3. graph callbacks

(see: `./cmd/btrfs-rec/inspect/rebuildtrees/rebuild_wantcb.go`)

The graph callbacks are what tie the above together.

For each of these callbacks, whenever I say that it looks up something
in a tree's item index or potential-item index, that implies looking
the tree up from the forrest; if the forrest cannot look up that tree,
then the callback returns early, after either:

 - if we are in substep 1 and are processing a tree: we add the tree
   that is being processed to the tree queue.  (TODO: Wait, this
   assumes that an augment will be applied to the `ROOT_TREE` before
   the next pass... if that isn't the case, this will result in the
   loop never terminating... I guess I need to add a separate
   retry-tree queue?)

 - if we are in substep 2 and are processing an item: we add the item
   that is being processed to the retry-item queue for the tree that
   cannot be looked up

The 6 methods in the `brfscheck.GraphCallbacks` interface are:

 1. `FSErr()`: There's an error with the filesystem; this callback
    just spits it out on stderr.  I say such a trivial matter because,
    again, for a recovery tool I think it's worth putting care in to
    how you handle errors and where you expect them: We expect them
    here, so we have to check for them to avoid reading invalid data
    or whatever, but we don't actually need to do anything other than
    watch our step.

 2. `Want()`: We want an item in a given tree with a given object ID
    and item type, but we don't care about what the item's offset is.

    The callback works by searching the item index to see if it can
    find such an item; if so, it has nothing else to do and returns.
    Otherwise, it searches the potential-item index; for each matching
    item it finds it looks in the node index for the node containing
    that item, and adds the roots that would add that node, and adds
    those roots to a set.  Once it has finished searching the
    potential-item index, it adds that set to the augment queue (even
    if that set is still empty).

 3. `WantOff()`: The same, but we want a specific offset.

 4. `WantDirIndex()`: We want a `DIR_INDEX` item for a given inode and
    filename, but we don't know what the offset of that item is.

    First we scan over the item index, looking at all `DIR_INDEX`
    items for that inode number.  For each item, we can check the scan
    data to see what the filename in that `DIR_INDEX` is, so we can
    see if the item satisfies this want without accessing the disk.
    If there's a match, then there is nothing else to do, so we
    return.  Otherwise, we do that same search over the potential-item
    index; if we find any matches, then we build the set of roots to
    add to the augment queue the same as in `Want`.

 5. `WantFileExt()`: We want 1 or more `DATA_EXTENT` items in the
    given tree for the given inode, and we want them to cover from 0
    to a given size bytes of that file.

    First we walk that range in the item index, to build a list of the
    gaps that we need to fill ("Step 1" in
    `rebuild_wantcb.go:_wantRange()`).  This walk
    (`rebuild_wantcb.go:_walkRange()`) requires knowing the size of
    each file extent; so doing this quickly without hitting disk is
    why we recorded the size of each file extent in our initialization
    step.
	
	Then ("Step 2" in `_wantRange()`) we iterate over each of the
    gaps, and for each gap do a very similar walk (again, by calling
    `_walkRange()`, but this time over the potential-item index.  For
    each file extent we find that has is entirely within the gap, we
    "want" that extent, and move the beginning of of the gap forward
    to the end of that extent.  This algorithm is dumb and greedy,
    potentially making sub-optimal selections; and so could probably
    stand to be improved; but in my real-world use, it seems to be
    "good enough".

 6. `WantCSum()`: We want 1 or more `EXTENT_CSUM` items to cover the
    half-open interval [`lo_logical_addr`, `hi_logical_addr`).  Well,
    maybe.  It also takes a subvolume ID and an inode number; and
    looks up in the scan data whether that inode has the
    `INODE_NODATASUM` flag set; if it does have the flag set, then it
    returns early without looking for any `EXTENT_CSUM` items.  If it
    doesn't return early, then it performs the same want-range routine
    as `WantFileExt`, but with the appropriate tree, object ID, and
    item types for csums as opposed to data extents.

For each of these callbacks, we generate a "wantKey", a tuple
representing the function and its arguments; we check the
augment-queue to see if we've already enqueued a set of roots for that
want, and if so, that callback can return early without checking the
potential-item index.

# 5. Future work

It's in a reasonably useful place, I think; and so now I'm going to
take a break from it for a while.  But there's still lots of work to
do:

 - RAID almost certainly doesn't work.

 - Encryption is not implemented.

 - It doesn't understand (ignores) the `TREE_LOG` (because I don't
   understand the `TREE_LOG`).

 - `btrfs-rec inspect mount` should add "lost+found" directories for
   inodes that are included in the subvolume's tree but aren't
   reachable from the tree's root inode

 - I still need to implement `btrfs-rec repair SUBCMD` subcommands to
   write rebuilt-information from `btrfs-rec inspect` back to the
   filesystem.

 - I need to figure out the error handling/reporting story for
   `mount`.

 - It needs a lot more tests

    + I'd like to get the existing btrfs-progs fsck tests to run on
      it.

 - In the process of writing this email, I realized that I probably
   need to add a retry-tree queue; see the "graph callbacks" section
   in the description of the `rebuild-trees` algorithm above.

 - Shere are a number of "TODO" comments or panics in the code:

    + Some of them definitely need done.

    + Some of them are `panic("TODO")` on the basis that if it's
      seeing something on the filesystem that it doesn't recognize,
      it's probably that I didn't get to implementing that
      thing/situation, but it's possible that the thing is just
      corrupt.  This should only be for situations that the node
      passed the checksum test, so it being corrupt would have to be
      caused by a bug in btrfs rather than a failing drive or other
      corruption; I wasn't too worried about btrfs bugs.

 - `btrfs-rec inspect rebuild-trees` is slow, and can probably be made
   a lot faster.

   Just to give you an idea of the speeds, the run-times for the
   various steps on my ThinkPad E15 for a 256GB disk image are as
   follows:

        btrfs-rec inspect rebuild-mappings scan       :     7m 31s
        btrfs-rec inspect rebuild-mappings list-nodes :        47s
        btrfs-rec inspect rebuild-mappings process    :     8m 22s
        btrfs-rec inspect rebuild-trees               : 1h  4m 55s
        btrfs-rec inspect ls-files                    :    29m 55s
        btrfs-rec inspect ls-trees                    :     8m 40s

   For the most part, it's all single-threaded (with the main
   exception that in several places I/O has been moved to a separate
   thread from the main CPU-heavy thread), but a lot of the algorithms
   could be parallelized.

 - There are a lot of "tunable" values that I haven't really spent
   time tuning.  These are all annotated with `textui.Tunable()`.  I
   sort-of intended for them to be adjustable on the CLI.

 - Perhaps the `btrfs inspect rebuild-trees` algorithm could be
   adjusted to also try to rebuild trees with missing parents; see the
   above discussion of the algorithm.

# 6. Problems for merging this code into btrfs-progs

 - It's written in Go, not C.

 - It's effectively GPLv3+ (not GPLv2-only or GPLv2+) because of use
   of some code under the Apache 2.0 license (2 files in the codebase
   itself that are based off of Apache-licensed code, and use of
   unmodified 3rd-party libraries).

 - It uses ARC (Adaptive Replacement Cache), which is patented by IBM,
   and the patent doesn't expire for another 7 months.  An important
   property of ARC over LRU is that it is scan-resistant; the above
   algorithms do a lot of scanning.  On that note, now that RedHat is
   owned by IBM: who in the company do we need to get to talk to
   eachother so that we can get ARC into the Linux kernel before then?

-- 
Happy hacking,
~ Luke Shumaker
