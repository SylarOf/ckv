ckv is a simple kv engine use rust

base data structures:

Options: options stores database start option, that holds a max_fid which is atomic, memtable size , sst max size and 
other messages,it will be created when db open and be clone and move multiple times for other data strcutres start and
work.

Bloom Filter: it is used to accelerate seeking a key in sst file, it use multiple hash functions to hash a key when add to sst
file, when search in a sst file, a hash for a key will be hashed, if may_contain_key return true, it will actually start searching.

Memtable: memtable is a memory database for kv engine, it has a skiplist and a wal file that uses mmmap,
when put data in database, entry will be firstly writed to wal file then write to a skiplist in memtable,
when write in wal file, it will firstly write key_len val_len that uses varint encode, then write key and
val. when memtable is full, it will be take and push to a vec that holds immutable memtable, which will be 
flush to disk that called sst file in level 0.

Sorted String Table: sst is a disk database that store entry, its pattern is |data block 1| |data block 2|
|data block 3| |index block|, in data block it will write |overlap_len| |differ_len| |differ key| |val|, overlap_len
is the key overlaps with block base key, when write key the entry offset will be push in entry_offsets vec, when write
entry finished, then write all entry offsets , entry offsets len , checksum of data and checksum len.when blocks size
over sst max size, it will write index block that holds all block offset that stroes block base_key, offset, and block_len,
then write bloom filter of all data, then write key_count of sstable, we use pb to put index block.

Manifset File: manifest file is used to recovery database state, it will record every sst file that created and deleted, and the
level and id off sst file, when sst file is created or deleted, a record will be write to sst file, and sst level message will
be load to Manifest by replaying the Manifest file.

Iterator: it provides multiple iterator for different uses, block iter to iter and seek entry in a block, table iter to iter in multiple blocks in a sst file,  wal iter to iter entry in wal file, and merge iter to iter data in multiple sst files.

LevelHandler: it stores tables that in a specified level, in this database, all table index block will be load when database start,
the owership of sstfile owns LevelHandler, it also stores total sst size per level.

LevelManger: LevelManger stores all LevelHanler that levels holds, it will be Arc and RwLock for modify tables and move to other threads, a SompactionState that holds compaction state for all sst files, and a manifest file that record sst files level meta.

DB: it owns a LevelManger that wraps in Arc for clone and move to other threads, a Memtable wrap in Option that is easy to take, and
a Memtable vec that stores immu memtables.


compaction logic:

when db open, run compacter func should be invoked, which is async func that means we need a async run time to run it, this db we use tokio, nums compact coroutine that defines in Options will be start, which start after a random duration and be ticked ervery 5
seconds.sst files in level 0 and level max will be compacted when the number of files reach a val that Options defined and 1 to max level will be compacted when total size reach a specified val, we fist find this level to compact and the level it will be compacted to, then find next level files that overlap this level, then create a CompactDef to holds compaction meta then add splits for key range that two level holds, for every split, start a async task to create a merge iter to build new sst file, erery task, when a table builder finish successfully, it will start another task to open table and create sst file, then the table will be send to compact coroutine, then renew manifest and modif levelhandlers.

set a key:

when put a new key and val to database, it will fistly write to wal file and then skiplist, when reach memtable capacity, it will be flushed to sst file in level 0.

seek a key:

we will fist try to search it in skiplist in memtable, and then immu memtables, then level 0 sst files, and other level files, in level 0 sst files key will overlap, we need to search all table, level 1 to level max, all sst files created by compaction, so no
overlaping, we only need to find the specified table then use table iter to binary search it.