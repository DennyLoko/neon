/*
 * LastWrittenLsn cache
 * 
 * It contains 2 systems:
 * 
 * 1. A hashmap cache that contains evicted pages' buffer tags with their
 *	  LSNs, with eviction by lowest LSN made efficient by a pairingheap
 *	  constructed in that cache
 * 2. A single watermark LSN that is the highest LSN evicted from the cache in
 * 	  1). 
 */

#include "postgres.h"

#include "lib/pairingheap.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

#include "neon_lwlc.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "access/xlog.h"

static shmem_startup_hook_type prev_shmem_startup_hook;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void relsize_shmem_request(void);
#endif

typedef struct LwLsnCacheEntryKeyData {
	RelFileNode	rnode;
	ForkNumber	forkNum;
	BlockNumber	blockNum;
} LwLsnCacheEntryKeyData;

typedef struct LwLsnCacheEntryData {
	LwLsnCacheEntryKeyData key;
	XLogRecPtr	lsn;
	pairingheap_node pheapnode;
} LwLsnCacheEntryData;

typedef struct LwLsnCacheData {
	pairingheap	pheap;
	int			n_cached_entries;
	volatile XLogRecPtr	highWaterMark;
	volatile XLogRecPtr lowWaterMark;
} LwLsnCacheData;

int lsn_cache_size;

LwLsnCacheData *LwLsnCache;

LWLockPadded *LwLsnCacheLockTranche;
#define LwLsnMetadataLock (&LwLsnCacheLockTranche[0].lock)

#define NUM_LWLSN_LOCKS 1

HTAB *LwLsnCacheTable;


static void lwlc_setup(void);
static void lwlc_register_gucs(void);
static void lwlc_request_shmem(void);
static void lwlc_setup_shmem(void);

static XLogRecPtr lwlc_lookup_last_lsn(RelFileNode node, ForkNumber fork, BlockNumber blkno, XLogRecPtr *effective);
static bool lwlc_should_insert(XLogRecPtr lsn);
static void lwlc_insert_last_lsn(XLogRecPtr lsn, RelFileNode node, ForkNumber fork, BlockNumber blkno);

static void
lwlc_setup(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	lwlc_register_gucs();

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = &lwlc_setup_shmem;
	SetLastWrittenLSNForDatabase = &SetLastWrittenLsnForDatabase;
	SetLastWrittenLSNForRelFork = &SetLastWrittenLsnForRelFileNode;
	
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = &lwlc_request_shmem;
#else
	lwlc_request_shmem();
#endif
}

static void
lwlc_register_gucs(void)
{
	DefineCustomIntVariable("neon.lwlsn_cache_size",
							"Size of last written LSN cache used by Neon",
							NULL,
							&lsn_cache_size,
							-1, -1, INT_MAX,
							PGC_POSTMASTER,
							0, /* plain units */
							NULL, NULL, NULL);
}

static void
lwlc_request_shmem(void)
{
	Size requested_size = sizeof(LwLsnCacheData);

	requested_size += hash_estimate_size(lsn_cache_size, sizeof(LwLsnCacheEntryData));

	RequestAddinShmemSpace(requested_size);

	RequestNamedLWLockTranche("neon/LwLsnCache", NUM_LWLSN_LOCKS);
}

static void
lwlc_setup_shmem(void)
{
	static HASHCTL info;
	LwLsnCache = ShmemAlloc(sizeof(LwLsnCacheData));

	info.keysize = sizeof(LwLsnCacheEntryKeyData);
	info.entrysize = sizeof(LwLsnCacheEntryData);

	LwLsnCacheTable = ShmemInitHash("neon/LwLsnHashTable",
									lsn_cache_size, lsn_cache_size,
									&info,
									HASH_ELEM | HASH_BLOBS);
	LwLsnCacheLockTranche = GetNamedLWLockTranche("neon/LwLsnCache");
}

static XLogRecPtr
lwlc_lookup_last_lsn(RelFileNode node, ForkNumber fork, BlockNumber blkno,
					 XLogRecPtr *effective)
{
	uint32		hash;
	bool		found;
	XLogRecPtr	lsn;
	LwLsnCacheEntryKeyData key;
	LwLsnCacheEntryData *entry;

	key.rnode = node;
	key.forkNum = fork;
	key.blockNum = blkno;

	hash = get_hash_value(LwLsnCacheTable, &key);

	LWLockAcquire(LwLsnMetadataLock, LW_SHARED);

	entry = (LwLsnCacheEntryData *)
		hash_search_with_hash_value(LwLsnCacheTable, &key, hash,
									HASH_FIND,
									&found);

	if (found)
		lsn = entry->lsn;
	else 
	{
		/* it can't have been evicted after low watermark */
		lsn = LwLsnCache->lowWaterMark;
	}

	LWLockRelease(LwLsnMetadataLock);

	return lsn;
}


static bool
lwlc_should_insert(XLogRecPtr lsn)
{
	return LwLsnCache->lowWaterMark < lsn;
}

static void
lwlc_insert_last_lsn(XLogRecPtr lsn, RelFileNode node, ForkNumber fork, BlockNumber blkno)
{
	uint32		hash;
	bool		found;
	LwLsnCacheEntryKeyData key;
	LwLsnCacheEntryData *entry;

	MemSet(&key, 0, sizeof(key));

	key.rnode = node;
	key.forkNum = fork;
	key.blockNum = blkno;

	hash = get_hash_value(LwLsnCacheTable, &key);

	LWLockAcquire(LwLsnMetadataLock, LW_EXCLUSIVE);

	if (lsn <= LwLsnCache->lowWaterMark)
	{
		LWLockRelease(LwLsnMetadataLock);
		/* XXX: Update eviction cache */
		return;
	}

	if(LwLsnCache->highWaterMark < lsn)
		LwLsnCache->highWaterMark = lsn;

	entry = (LwLsnCacheEntryData *)
		hash_search_with_hash_value(LwLsnCacheTable, &key, hash,
									HASH_ENTER,
									&found);

	if (found)
	{
		if (lsn > entry->lsn)
		{
			pairingheap_remove(&LwLsnCache->pheap, &entry->pheapnode);
			entry->lsn = lsn;
			pairingheap_add(&LwLsnCache->pheap, &entry->pheapnode);
		}
		else
		{
			/* nothing to do */
		}
	}
	else
	{
		entry->lsn = lsn;
		pairingheap_add(&LwLsnCache->pheap, &entry->pheapnode);
		
		LwLsnCache->n_cached_entries++;

		if (LwLsnCache->n_cached_entries >= lsn_cache_size)
		{
			entry = (LwLsnCacheEntryData *)
				pairingheap_remove_first(&LwLsnCache->pheap);
			key = entry->key;
			lsn = entry->lsn;

			/*
			 * lowWaterMark is the lowest LSN that *could* still be in the
			 * cache. So, if we evict LSN > lowWatermark, that becomes the
			 * next low watermark.
			 */
			if (LwLsnCache->lowWaterMark < lsn)
				LwLsnCache->lowWaterMark = lsn;

			hash_search(LwLsnCacheTable, &key, HASH_REMOVE, &found);
			LwLsnCache->n_cached_entries--;

			Assert(found);
		}
		else
		{
			/* nothing to do */
		}
	}

	LWLockRelease(LwLsnMetadataLock);
}

XLogRecPtr
GetLastWrittenLsnForBuffer(RelFileNode node,
						   ForkNumber fork,
						   BlockNumber blkno,
						   XLogRecPtr *effective)
{
	Assert(OidIsValid(node.dbNode));
	Assert(OidIsValid(node.relNode));
	Assert(OidIsValid(node.spcNode));
	Assert(fork != InvalidForkNumber);
	Assert(BlockNumberIsValid(blkno));

	lwlc_lookup_last_lsn(node, fork, blkno, NULL);
}

void
SetLastWrittenLsnForBuffer(XLogRecPtr lsn, RelFileNode node, ForkNumber fork, BlockNumber blkno)
{
	Assert(OidIsValid(node.dbNode));
	Assert(OidIsValid(node.relNode));
	Assert(OidIsValid(node.spcNode));
	Assert(fork != InvalidForkNumber);
	Assert(BlockNumberIsValid(blkno));

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, fork, blkno);

}

XLogRecPtr
GetLastWrittenLsnForRelFileNode(RelFileNode node,
								ForkNumber fork,
								XLogRecPtr *effective)
{
	Assert(OidIsValid(node.dbNode));
	Assert(OidIsValid(node.relNode));
	Assert(OidIsValid(node.spcNode));
	Assert(fork != InvalidForkNumber);

	lwlc_lookup_last_lsn(node, fork, InvalidBlockNumber, NULL);
}

void
SetLastWrittenLsnForRelFileNode(XLogRecPtr lsn, RelFileNode node, ForkNumber fork)
{
	Assert(OidIsValid(node.dbNode));
	Assert(OidIsValid(node.relNode));
	Assert(OidIsValid(node.spcNode));
	Assert(fork != InvalidForkNumber);

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, fork, InvalidBlockNumber);

	/*
	 * A table's size has updated, thus the database is resized, thus we need
	 * to do requests at _at least_ this lsn for the latest (and greatest)
	 * database size estimates.
	 */
	SetLastWrittenLsnForDatabase(lsn, node.dbNode);
}

XLogRecPtr
GetLastWrittenLsnForDatabase(Oid datoid,
							 XLogRecPtr *effective)
{
	RelFileNode node = {0};

	Assert(OidIsValid(datoid));

	node.dbNode = datoid;

	lwlc_lookup_last_lsn(node, InvalidForkNumber, InvalidBlockNumber, NULL);
}

void
SetLastWrittenLsnForDatabase(XLogRecPtr lsn, Oid datoid)
{
	RelFileNode node = {0};

	node.dbNode = datoid;

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, InvalidForkNumber, InvalidBlockNumber);
}

XLogRecPtr
GetLastWrittenLsnForDbCluster(XLogRecPtr *effective)
{
	RelFileNode node = {0};

	lwlc_lookup_last_lsn(node, InvalidForkNumber, InvalidBlockNumber, NULL);
}

void
SetLastWrittenLsnForDbCluster(XLogRecPtr lsn)
{
	RelFileNode node = {0};

	if (lwlc_should_insert(lsn))
		lwlc_insert_last_lsn(lsn, node, InvalidForkNumber, InvalidBlockNumber);
}

