/*-------------------------------------------------------------------------
 *
 * nodeHash.c
 *	  Routines to hash relations for hashjoin
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHash.c
 *
 * See note on parallelism in nodeHashjoin.c.
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecHash	- generate an in-memory hash table of the relation
 *		ExecInitHash	- initialize node and subnodes
 *		ExecEndHash		- shutdown node and subnodes
 */

#include "postgres.h"

#include <math.h>
#include <limits.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "catalog/pg_statistic.h"
#include "commands/tablespace.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static void ExecHashIncreaseNumBatches(HashJoinTable hashtable);
static void ExecHashIncreaseNumBuckets(HashJoinTable hashtable);

static void *dense_alloc(HashJoinTable hashtable, Size size);
static void MultiExecPrivateHash(HashState *node);
static void MultiExecParallelHash(HashState *node);

static TupleTableSlot* ExecPrivateHash(HashState *node); // lab2

/* ----------------------------------------------------------------
 *		ExecHash
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecHash(PlanState *pstate)
{
	return ExecPrivateHash((HashState*)pstate);
}

/* ----------------------------------------------------------------
 *		MultiExecHash
 *
 *		build hash table for hashjoin, doing partitioning if more
 *		than one batch is required.
 * ----------------------------------------------------------------
 */
Node *
MultiExecHash(HashState *node)
{
	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStartNode(node->ps.instrument);

	MultiExecPrivateHash(node);

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStopNode(node->ps.instrument, node->hashtable->partialTuples);

	/*
	 * We do not return the hash table directly because it's not a subtype of
	 * Node, and so would violate the MultiExecProcNode API.  Instead, our
	 * parent Hashjoin node is expected to know how to fish it out of our node
	 * state.  Ugly but not really worth cleaning up, since Hashjoin knows
	 * quite a bit more about Hash besides that.
	 */
	return NULL;
}

/* ----------------------------------------------------------------
 *		MultiExecPrivateHash
 *
 *		parallel-oblivious version, building a backend-private
 *		hash table and (if necessary) batch files.
 * ----------------------------------------------------------------
 */
static void
MultiExecPrivateHash(HashState *node)
{
	elog(ERROR, "Hash node does not support MultiExecProcNode call convention");
}


// fetch a single tuple from child node, insert it into  
// the hash table, then return the tuple.
static TupleTableSlot*
ExecPrivateHash(HashState *node)
{
	PlanState  *childNode;
	List	   *hashkeys;
	HashJoinTable hashtable;
	TupleTableSlot *slot;
	ExprContext *econtext;
	uint32		hashvalue;

	/*
	 * get state info from node
	 */
	childNode = outerPlanState(node);
	hashtable = node->hashtable;

	/*
	 * set expression context
	 */
	hashkeys = node->hashkeys;
	econtext = node->ps.ps_ExprContext;

	/*
	 * Get one tuple from the node below the Hash node and insert into the
	 * hash table (or temp files).
	*/
	slot = ExecProcNode(childNode);
	
	if (TupIsNull(slot))
		{
			ELOGDEBUG("Hash %s got null",node->debugInnerOuter==0?"inner":"outer");
			return NULL;
			}
	/* We have to compute the hash value */
	econtext->ecxt_outertuple = slot;
	if (ExecHashGetHashValue(hashtable, econtext, hashkeys,
							false, hashtable->keepNulls,
							&hashvalue))
	{
		// ignore skew bucket, related codes removed
		hashtable->totalTuples += 1;
	}
	int keyval = DebugGetKey(hashtable, econtext, hashkeys);
	ELOGDEBUG("Hash %s got %d",node->debugInnerOuter==0?"inner":"outer", keyval);

	/* resize the hash table if needed (NTUP_PER_BUCKET exceeded) */
	// if (hashtable->nbuckets != hashtable->nbuckets_optimal)
	// 	ExecHashIncreaseNumBuckets(hashtable);
	// MIIGON: not needed in assignment 2: ignore batching

	// insert it into the hashtable for later matching. (ignore skew optimization)
	node->lastInsert = ExecHashTableInsert(hashtable, slot, hashvalue);
	hashtable->totalTuples += 1;

	/* Account for the buckets in spaceUsed (reported in EXPLAIN ANALYZE) */
	hashtable->spaceUsed += hashtable->nbuckets * sizeof(HashJoinTuple);
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;

	hashtable->partialTuples = hashtable->totalTuples;
	
	return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for Hash node
 * ----------------------------------------------------------------
 */
HashState *
ExecInitHash(Hash *node, EState *estate, int eflags)
{
	HashState  *hashstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hashstate = makeNode(HashState);
	hashstate->ps.plan = (Plan *) node;
	hashstate->ps.state = estate;
	hashstate->ps.ExecProcNode = ExecHash;
	hashstate->hashtable = NULL;
	hashstate->hashkeys = NIL;	/* will be set by parent HashJoin */

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hashstate->ps);

	/*
	 * initialize child nodes
	 */
	outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize our result slot and type. No need to build projection
	 * because this node doesn't do projections.
	 */
	ExecInitResultTupleSlotTL(&hashstate->ps, &TTSOpsMinimalTuple);
	hashstate->ps.ps_ProjInfo = NULL;

	/*
	 * initialize child expressions
	 */
	Assert(node->plan.qual == NIL);
	hashstate->hashkeys =
		ExecInitExprList(node->hashkeys, (PlanState *) hashstate);

	return hashstate;
}

/* ---------------------------------------------------------------
 *		ExecEndHash
 *
 *		clean up routine for Hash node
 * ----------------------------------------------------------------
 */
void
ExecEndHash(HashState *node)
{
	PlanState  *outerPlan;

	/*
	 * free exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * shut down the subplan
	 */
	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}


/* ----------------------------------------------------------------
 *		ExecHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
HashJoinTable
ExecHashTableCreate(HashState *state, List *hashOperators, List *hashCollations, bool keepNulls)
{
	Hash	   *node;
	HashJoinTable hashtable;
	Plan	   *outerNode;
	size_t		space_allowed;
	int			nbuckets;
	int			nbatch;
	double		rows;
	int			num_skew_mcvs;
	int			log2_nbuckets;
	int			nkeys;
	int			i;
	ListCell   *ho;
	ListCell   *hc;
	MemoryContext oldcxt;

	/*
	 * Get information about the size of the relation to be hashed (it's the
	 * "outer" subtree of this node, but the inner relation of the hashjoin).
	 * Compute the appropriate size of the hash table.
	 */
	node = (Hash *) state->ps.plan;
	outerNode = outerPlan(node);

	/*
	 * If this is shared hash table with a partial plan, then we can't use
	 * outerNode->plan_rows to estimate its size.  We need an estimate of the
	 * total number of rows across all copies of the partial plan.
	 */
	rows = node->plan.parallel_aware ? node->rows_total : outerNode->plan_rows;

	ExecChooseHashTableSize(rows, outerNode->plan_width,
							OidIsValid(node->skewTable),
							state->parallel_state != NULL,
							state->parallel_state != NULL ?
							state->parallel_state->nparticipants - 1 : 0,
							&space_allowed,
							&nbuckets, &nbatch, &num_skew_mcvs);

	/* nbuckets must be a power of 2 */
	log2_nbuckets = my_log2(nbuckets);
	Assert(nbuckets == (1 << log2_nbuckets));

	/*
	 * Initialize the hash table control block.
	 *
	 * The hashtable control block is just palloc'd from the executor's
	 * per-query memory context.  Everything else should be kept inside the
	 * subsidiary hashCxt or batchCxt.
	 */
	hashtable = (HashJoinTable) palloc(sizeof(HashJoinTableData));
	hashtable->nbuckets = nbuckets;
	hashtable->nbuckets_original = nbuckets;
	hashtable->nbuckets_optimal = nbuckets;
	hashtable->log2_nbuckets = log2_nbuckets;
	hashtable->log2_nbuckets_optimal = log2_nbuckets;
	hashtable->buckets.unshared = NULL;
	hashtable->keepNulls = keepNulls;
	hashtable->skewEnabled = false;
	hashtable->skewBucket = NULL;
	hashtable->skewBucketLen = 0;
	hashtable->nSkewBuckets = 0;
	hashtable->skewBucketNums = NULL;
	hashtable->nbatch = nbatch;
	hashtable->curbatch = 0;
	hashtable->nbatch_original = nbatch;
	hashtable->nbatch_outstart = nbatch;
	hashtable->growEnabled = false; // lab2: turn off batching
	hashtable->totalTuples = 0;
	hashtable->partialTuples = 0;
	hashtable->skewTuples = 0;
	hashtable->innerBatchFile = NULL;
	hashtable->outerBatchFile = NULL;
	hashtable->spaceUsed = 0;
	hashtable->spacePeak = 0;
	hashtable->spaceAllowed = space_allowed;
	hashtable->spaceUsedSkew = 0;
	hashtable->spaceAllowedSkew =
		hashtable->spaceAllowed * SKEW_WORK_MEM_PERCENT / 100;
	hashtable->chunks = NULL;
	hashtable->current_chunk = NULL;
	hashtable->parallel_state = state->parallel_state;
	hashtable->area = state->ps.state->es_query_dsa;
	hashtable->batches = NULL;

#ifdef HJDEBUG
	printf("Hashjoin %p: initial nbatch = %d, nbuckets = %d\n",
		   hashtable, nbatch, nbuckets);
#endif

	/*
	 * Create temporary memory contexts in which to keep the hashtable working
	 * storage.  See notes in executor/hashjoin.h.
	 */
	hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
											   "HashTableContext",
											   ALLOCSET_DEFAULT_SIZES);

	hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
												"HashBatchContext",
												ALLOCSET_DEFAULT_SIZES);

	/* Allocate data that will live for the life of the hashjoin */

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	/*
	 * Get info about the hash functions to be used for each hash key. Also
	 * remember whether the join operators are strict.
	 */
	nkeys = list_length(hashOperators);
	hashtable->outer_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->inner_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->hashStrict = (bool *) palloc(nkeys * sizeof(bool));
	hashtable->collations = (Oid *) palloc(nkeys * sizeof(Oid));
	i = 0;
	forboth(ho, hashOperators, hc, hashCollations)
	{
		Oid			hashop = lfirst_oid(ho);
		Oid			left_hashfn;
		Oid			right_hashfn;

		if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
			elog(ERROR, "could not find hash function for hash operator %u",
				 hashop);
		fmgr_info(left_hashfn, &hashtable->outer_hashfunctions[i]);
		fmgr_info(right_hashfn, &hashtable->inner_hashfunctions[i]);
		hashtable->hashStrict[i] = op_strict(hashop);
		hashtable->collations[i] = lfirst_oid(hc);
		i++;
	}

	// NOTE: batching code removed (for assignment 2)

	MemoryContextSwitchTo(oldcxt);

	/*
		* Prepare context for the first-scan space allocations; allocate the
		* hashbucket array therein, and set each bucket "empty".
		*/
	MemoryContextSwitchTo(hashtable->batchCxt);

	hashtable->buckets.unshared = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));

	/*
		* Set up for skew optimization, if possible and there's a need for
		* more than one batch.  (In a one-batch join, there's no point in
		* it.)
		*/
	// if (nbatch > 1)
	// 	ExecHashBuildSkewHash(hashtable, node, num_skew_mcvs);
	// NOTE: IGNORE skew optimization

	MemoryContextSwitchTo(oldcxt);

	return hashtable;
}


/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows and average row width).
 *
 * This is exported so that the planner's costsize.c can use it.
 */

/* Target bucket loading (tuples per bucket) */
#define NTUP_PER_BUCKET			1

void
ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
						bool try_combined_work_mem,
						int parallel_workers,
						size_t *space_allowed,
						int *numbuckets,
						int *numbatches,
						int *num_skew_mcvs)
{
	int			tupsize;
	double		inner_rel_bytes;
	long		bucket_bytes;
	long		hash_table_bytes;
	long		skew_table_bytes;
	long		max_pointers;
	long		mppow2;
	int			nbatch = 1;
	int			nbuckets;
	double		dbuckets;

	/* Force a plausible relation size if no info */
	if (ntuples <= 0.0)
		ntuples = 1000.0;

	/*
	 * Estimate tupsize based on footprint of tuple in hashtable... note this
	 * does not allow for any palloc overhead.  The manipulations of spaceUsed
	 * don't count palloc overhead either.
	 */
	tupsize = HJTUPLE_OVERHEAD +
		MAXALIGN(SizeofMinimalTupleHeader) +
		MAXALIGN(tupwidth);
	inner_rel_bytes = ntuples * tupsize;

	/*
	 * Target in-memory hashtable size is work_mem kilobytes.
	 */
	hash_table_bytes = work_mem * 1024L;

	/*
	 * Parallel Hash tries to use the combined work_mem of all workers to
	 * avoid the need to batch.  If that won't work, it falls back to work_mem
	 * per worker and tries to process batches in parallel.
	 */
	if (try_combined_work_mem)
		hash_table_bytes += hash_table_bytes * parallel_workers;

	*space_allowed = hash_table_bytes;

	/*
	 * If skew optimization is possible, estimate the number of skew buckets
	 * that will fit in the memory allowed, and decrement the assumed space
	 * available for the main hash table accordingly.
	 *
	 * We make the optimistic assumption that each skew bucket will contain
	 * one inner-relation tuple.  If that turns out to be low, we will recover
	 * at runtime by reducing the number of skew buckets.
	 *
	 * hashtable->skewBucket will have up to 8 times as many HashSkewBucket
	 * pointers as the number of MCVs we allow, since ExecHashBuildSkewHash
	 * will round up to the next power of 2 and then multiply by 4 to reduce
	 * collisions.
	 */
	if (useskew)
	{
		skew_table_bytes = hash_table_bytes * SKEW_WORK_MEM_PERCENT / 100;

		/*----------
		 * Divisor is:
		 * size of a hash tuple +
		 * worst-case size of skewBucket[] per MCV +
		 * size of skewBucketNums[] entry +
		 * size of skew bucket struct itself
		 *----------
		 */
		*num_skew_mcvs = skew_table_bytes / (tupsize +
											 (8 * sizeof(HashSkewBucket *)) +
											 sizeof(int) +
											 SKEW_BUCKET_OVERHEAD);
		if (*num_skew_mcvs > 0)
			hash_table_bytes -= skew_table_bytes;
	}
	else
		*num_skew_mcvs = 0;

	/*
	 * Set nbuckets to achieve an average bucket load of NTUP_PER_BUCKET when
	 * memory is filled, assuming a single batch; but limit the value so that
	 * the pointer arrays we'll try to allocate do not exceed work_mem nor
	 * MaxAllocSize.
	 *
	 * Note that both nbuckets and nbatch must be powers of 2 to make
	 * ExecHashGetBucketAndBatch fast.
	 */
	max_pointers = *space_allowed / sizeof(HashJoinTuple);
	max_pointers = Min(max_pointers, MaxAllocSize / sizeof(HashJoinTuple));
	/* If max_pointers isn't a power of 2, must round it down to one */
	mppow2 = 1L << my_log2(max_pointers);
	if (max_pointers != mppow2)
		max_pointers = mppow2 / 2;

	/* Also ensure we avoid integer overflow in nbatch and nbuckets */
	/* (this step is redundant given the current value of MaxAllocSize) */
	max_pointers = Min(max_pointers, INT_MAX / 2);

	dbuckets = ceil(ntuples / NTUP_PER_BUCKET);
	dbuckets = Min(dbuckets, max_pointers);
	nbuckets = (int) dbuckets;
	/* don't let nbuckets be really small, though ... */
	nbuckets = Max(nbuckets, 1024);
	/* ... and force it to be a power of 2. */
	nbuckets = 1 << my_log2(nbuckets);

	/*
	 * If there's not enough space to store the projected number of tuples and
	 * the required bucket headers, we will need multiple batches.
	 */
	bucket_bytes = sizeof(HashJoinTuple) * nbuckets;
	if (inner_rel_bytes + bucket_bytes > hash_table_bytes)
	{
		/* We'll need multiple batches */
		long		lbuckets;
		double		dbatch;
		int			minbatch;
		long		bucket_size;

		/*
		 * If Parallel Hash with combined work_mem would still need multiple
		 * batches, we'll have to fall back to regular work_mem budget.
		 */
		if (try_combined_work_mem)
		{
			ExecChooseHashTableSize(ntuples, tupwidth, useskew,
									false, parallel_workers,
									space_allowed,
									numbuckets,
									numbatches,
									num_skew_mcvs);
			return;
		}

		/*
		 * Estimate the number of buckets we'll want to have when work_mem is
		 * entirely full.  Each bucket will contain a bucket pointer plus
		 * NTUP_PER_BUCKET tuples, whose projected size already includes
		 * overhead for the hash code, pointer to the next tuple, etc.
		 */
		bucket_size = (tupsize * NTUP_PER_BUCKET + sizeof(HashJoinTuple));
		lbuckets = 1L << my_log2(hash_table_bytes / bucket_size);
		lbuckets = Min(lbuckets, max_pointers);
		nbuckets = (int) lbuckets;
		nbuckets = 1 << my_log2(nbuckets);
		bucket_bytes = nbuckets * sizeof(HashJoinTuple);

		/*
		 * Buckets are simple pointers to hashjoin tuples, while tupsize
		 * includes the pointer, hash code, and MinimalTupleData.  So buckets
		 * should never really exceed 25% of work_mem (even for
		 * NTUP_PER_BUCKET=1); except maybe for work_mem values that are not
		 * 2^N bytes, where we might get more because of doubling. So let's
		 * look for 50% here.
		 */
		Assert(bucket_bytes <= hash_table_bytes / 2);

		/* Calculate required number of batches. */
		dbatch = ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes));
		dbatch = Min(dbatch, max_pointers);
		minbatch = (int) dbatch;
		nbatch = 2;
		while (nbatch < minbatch)
			nbatch <<= 1;
	}

	Assert(nbuckets > 0);
	Assert(nbatch > 0);

	*numbuckets = nbuckets;
	*numbatches = nbatch;
}


/* ----------------------------------------------------------------
 *		ExecHashTableDestroy
 *
 *		destroy a hash table
 * ----------------------------------------------------------------
 */
void
ExecHashTableDestroy(HashJoinTable hashtable)
{
	int			i;

	/*
	 * Make sure all the temp files are closed.  We skip batch 0, since it
	 * can't have any temp files (and the arrays might not even exist if
	 * nbatch is only 1).  Parallel hash joins don't use these files.
	 */
	if (hashtable->innerBatchFile != NULL)
	{
		for (i = 1; i < hashtable->nbatch; i++)
		{
			if (hashtable->innerBatchFile[i])
				BufFileClose(hashtable->innerBatchFile[i]);
			if (hashtable->outerBatchFile[i])
				BufFileClose(hashtable->outerBatchFile[i]);
		}
	}

	/* Release working memory (batchCxt is a child, so it goes away too) */
	MemoryContextDelete(hashtable->hashCxt);

	/* And drop the control block */
	pfree(hashtable);
}

/*
 * ExecHashIncreaseNumBatches
 *		increase the original number of batches in order to reduce
 *		current memory consumption
 */
static void
ExecHashIncreaseNumBatches(HashJoinTable hashtable)
{
	// elog(ERROR, "hash join: could not increase numBatch since batching is disabled for ass2");
}

/*
 * ExecHashIncreaseNumBuckets
 *		increase the original number of buckets in order to reduce
 *		number of tuples per bucket
 */
static void
ExecHashIncreaseNumBuckets(HashJoinTable hashtable)
{
	HashMemoryChunk chunk;

	/* do nothing if not an increase (it's called increase for a reason) */
	if (hashtable->nbuckets >= hashtable->nbuckets_optimal)
		return;

#ifdef HJDEBUG
	printf("Hashjoin %p: increasing nbuckets %d => %d\n",
		   hashtable, hashtable->nbuckets, hashtable->nbuckets_optimal);
#endif

	hashtable->nbuckets = hashtable->nbuckets_optimal;
	hashtable->log2_nbuckets = hashtable->log2_nbuckets_optimal;

	Assert(hashtable->nbuckets > 1);
	Assert(hashtable->nbuckets <= (INT_MAX / 2));
	Assert(hashtable->nbuckets == (1 << hashtable->log2_nbuckets));

	/*
	 * Just reallocate the proper number of buckets - we don't need to walk
	 * through them - we can walk the dense-allocated chunks (just like in
	 * ExecHashIncreaseNumBatches, but without all the copying into new
	 * chunks)
	 */
	hashtable->buckets.unshared =
		(HashJoinTuple *) repalloc(hashtable->buckets.unshared,
								   hashtable->nbuckets * sizeof(HashJoinTuple));

	memset(hashtable->buckets.unshared, 0,
		   hashtable->nbuckets * sizeof(HashJoinTuple));

	/* scan through all tuples in all chunks to rebuild the hash table */
	for (chunk = hashtable->chunks; chunk != NULL; chunk = chunk->next.unshared)
	{
		/* process all tuples stored in this chunk */
		size_t		idx = 0;

		while (idx < chunk->used)
		{
			HashJoinTuple hashTuple = (HashJoinTuple) (HASH_CHUNK_DATA(chunk) + idx);
			int			bucketno;
			int			batchno;

			ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
									  &bucketno, &batchno);

			/* add the tuple to the proper bucket */
			hashTuple->next.unshared = hashtable->buckets.unshared[bucketno];
			hashtable->buckets.unshared[bucketno] = hashTuple;

			/* advance index past the tuple */
			idx += MAXALIGN(HJTUPLE_OVERHEAD +
							HJTUPLE_MINTUPLE(hashTuple)->t_len);
		}

		/* allow this loop to be cancellable */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * ExecHashTableInsert
 *		insert a tuple into the hash table depending on the hash value
 *		it may just go to a temp file for later batches
 *
 * Note: the passed TupleTableSlot may contain a regular, minimal, or virtual
 * tuple; the minimal case in particular is certain to happen while reloading
 * tuples from batch files.  We could save some cycles in the regular-tuple
 * case by not forcing the slot contents into minimal form; not clear if it's
 * worth the messiness required.
 */
HashJoinTuple
ExecHashTableInsert(HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue)
{
	bool		shouldFree;
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
	int			bucketno;
	int			batchno;

	ExecHashGetBucketAndBatch(hashtable, hashvalue,
							  &bucketno, &batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	// if (batchno == hashtable->curbatch)
	// {
	// NOTE: assuming single batch
	/*
		* put the tuple in hash table
		*/
	HashJoinTuple hashTuple;
	int			hashTupleSize;
	double		ntuples = (hashtable->totalTuples - hashtable->skewTuples);

	/* Create the HashJoinTuple */
	hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
	hashTuple = (HashJoinTuple) dense_alloc(hashtable, hashTupleSize);

	hashTuple->hashvalue = hashvalue;
	ELOGDEBUG("inserting hash value %p into bucket %d", hashvalue, bucketno);
	memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);

	/*
		* We always reset the tuple-matched flag on insertion.  This is okay
		* even when reloading a tuple from a batch file, since the tuple
		* could not possibly have been matched to an outer tuple before it
		* went into the batch file.
		*/
	HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

	/* Push it onto the front of the bucket's list */
	hashTuple->next.unshared = hashtable->buckets.unshared[bucketno];
	hashtable->buckets.unshared[bucketno] = hashTuple;

	// NOTE: assuming single bucket, so some codes has been removed

	/* Account for space used, and back off if we've used too much */
	hashtable->spaceUsed += hashTupleSize;
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;
	if (hashtable->spaceUsed +
		hashtable->nbuckets_optimal * sizeof(HashJoinTuple)
		> hashtable->spaceAllowed)
		ExecHashIncreaseNumBatches(hashtable);

	if (shouldFree)
		heap_free_minimal_tuple(tuple);
	
	return hashTuple;
}

int DebugGetKey(HashJoinTable hashtable, ExprContext *econtext, List *hashkeys) {
	MemoryContext oldContext;
	Datum		keyval;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	ListCell *hk;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		
		bool		isNull;

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);
		break;
	}
	MemoryContextSwitchTo(oldContext);
	return DatumGetUInt64(keyval);;
}

/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in econtext->ecxt_outertuple (thus Vars in
 * the hashkeys expressions need to have OUTER_VAR as varno). If outer_tuple
 * is false (meaning it's the HashJoin's inner node, Hash), econtext,
 * hashkeys, and slot need to be from Hash, with hashkeys/slot referencing and
 * being suitable for tuples from the node below the Hash. Conversely, if
 * outer_tuple is true, econtext is from HashJoin, and hashkeys/slot need to
 * be appropriate for tuples from HashJoin's outer node.
 *
 * A true result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A false result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then false is never returned.)
 */
bool
ExecHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	if (outer_tuple)
		hashfunctions = hashtable->outer_hashfunctions;
	else
		hashfunctions = hashtable->inner_hashfunctions;

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (hashtable->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i], hashtable->collations[i], keyval));
			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecHashGetBucketAndBatch
 *		Determine the bucket number and batch number for a hash value
 *
 * Note: on-the-fly increases of nbatch must not change the bucket number
 * for a given hash code (since we don't move tuples to different hash
 * chains), and must only cause the batch number to remain the same or
 * increase.  Our algorithm is
 *		bucketno = hashvalue MOD nbuckets
 *		batchno = ROR(hashvalue, log2_nbuckets) MOD nbatch
 * where nbuckets and nbatch are both expected to be powers of 2, so we can
 * do the computations by shifting and masking.  (This assumes that all hash
 * functions are good about randomizing all their output bits, else we are
 * likely to have very skewed bucket or batch occupancy.)
 *
 * nbuckets and log2_nbuckets may change while nbatch == 1 because of dynamic
 * bucket count growth.  Once we start batching, the value is fixed and does
 * not change over the course of the join (making it possible to compute batch
 * number the way we do here).
 *
 * nbatch is always a power of 2; we increase it only by doubling it.  This
 * effectively adds one more bit to the top of the batchno.  In very large
 * joins, we might run out of bits to add, so we do this by rotating the hash
 * value.  This causes batchno to steal bits from bucketno when the number of
 * virtual buckets exceeds 2^32.  It's better to have longer bucket chains
 * than to lose the ability to divide batches.
 */
void
ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno)
{
	uint32		nbuckets = (uint32) hashtable->nbuckets;
	uint32		nbatch = (uint32) hashtable->nbatch;

	if (nbatch > 1)
	{
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = pg_rotate_right32(hashvalue,
									 hashtable->log2_nbuckets) & (nbatch - 1);
	}
	else
	{
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = 0;
	}
}

// probe the hash bucket for one matching tuple
// for the probing phase of the symmetric hash join algorithm 
//
// assuming econtext->ecxt_outertuple is the current outer tuple,
// and econtext->ecxt_innertuple is the current inner tuple.
//
// direction: 0 - probe inner table with outer tuple, 1 - probe outer table with inner tuple
static pg_attribute_always_inline bool ExecProbeHashBucket(HashJoinState *hjstate, ExprContext *econtext, int direction) {
    List *hjclauses = hjstate->hashclauses;
    HashJoinTable hashtable;
    HashJoinTuple hashTuple;
	TupleTableSlot *hashTupleSlot;
    uint32 hashvalue;
	int curBucketNo;

	if(direction == 0) { // probe inner table with outer tuple
		hashtable = hjstate->hj_HashTableInner;
		hashTuple = hjstate->hj_CurTupleInner; // null if starting search
		hashvalue = hjstate->hj_CurHashValueOuter;
		hashTupleSlot = hjstate->hj_InnerHashTupleSlot;
		curBucketNo = hjstate->hj_CurBucketNoOuter;
	} else { // probe outer table with inner tuple
		hashtable = hjstate->hj_HashTableOuter;
		hashTuple = hjstate->hj_CurTupleOuter; // null if starting search
		hashvalue = hjstate->hj_CurHashValueInner;
		hashTupleSlot = hjstate->hj_OuterHashTupleSlot;
		curBucketNo = hjstate->hj_CurBucketNoInner;
	}
	
	if (hashTuple != NULL)
		hashTuple = hashTuple->next.unshared;
	else
		hashTuple = hashtable->buckets.unshared[curBucketNo];

    while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue)
		{
			ELOGDEBUG("trying %p == %p", hashTuple->hashvalue, hashvalue);
			TupleTableSlot *restuple;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			restuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
											 hashTupleSlot,
											 false);	/* do not pfree */
			if(direction == 0)
				econtext->ecxt_innertuple = restuple; // outer is already in ecxt_outertuple
			else
				econtext->ecxt_outertuple = restuple; // inner is already in ecxt_innertuple

			// exec join clause between econtext->ecxt_innertuple and econtext->ecxt_outertuple
			if (ExecQualAndReset(hjclauses, econtext))
			{
				if(direction == 0) 
					hjstate->hj_CurTupleInner = hashTuple;
				else
					hjstate->hj_CurTupleOuter = hashTuple;
				return true;
			}
		} else {
			ELOGDEBUG("discarding %p == %p", hashTuple->hashvalue, hashvalue);
		}

		hashTuple = hashTuple->next.unshared;
	}

    /*
     * no match
     */
    return false;
}

// probe the hash bucket for one matching tuple
// for the probing phase of the symmetric hash join algorithm 
// this one probes the inner hash table with one outer tuple
bool ExecProbeInnerHashBucketWithOuterTuple(HashJoinState *hjstate, ExprContext *econtext) {
	return ExecProbeHashBucket(hjstate, econtext, 0);
}

// probe the hash bucket for one matching tuple
// for the probing phase of the symmetric hash join algorithm 
// this one probes the outer hash table with one inner tuple
bool ExecProbeOuterHashBucketWithInnerTuple(HashJoinState *hjstate, ExprContext *econtext) {
	return ExecProbeHashBucket(hjstate, econtext, 1);
}

/*
 * ExecPrepHashTableForUnmatched
 *		set up for a series of ExecScanHashTableForUnmatched calls
 */
void
ExecPrepHashTableForUnmatched(HashJoinState *hjstate)
{
	/*----------
	 * During this scan we use the HashJoinState fields as follows:
	 *
	 * hj_CurBucketNo: next regular bucket to scan
	 * hj_CurTuple: last tuple returned, or NULL to start next bucket
	 *----------
	 */
	hjstate->hj_CurBucketNoInner = 0;
	hjstate->hj_CurTupleInner = NULL;
	hjstate->hj_CurBucketNoOuter = 0;
	hjstate->hj_CurTupleOuter = NULL;
}
/*
 * ExecScanHashTableForUnmatched
 *		scan the hash table for unmatched inner tuples
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 * sourceTable: 0 - inner, 1 - outer
 */
bool
ExecScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext, int sourceTable)
{
	HashJoinTable hashtable;
	HashJoinTuple hashTuple;
	TupleTableSlot *hashTupleSlot;
	int *CurBucketNoRef;
	if(sourceTable == 0) {
		hashtable = hjstate->hj_HashTableInner;
		hashTuple = hjstate->hj_CurTupleInner;
		CurBucketNoRef = &hjstate->hj_CurBucketNoOuter;
		hashTupleSlot = hjstate->hj_InnerHashTupleSlot;
	} else {
		hashtable = hjstate->hj_HashTableOuter;
		hashTuple = hjstate->hj_CurTupleOuter;
		CurBucketNoRef = &hjstate->hj_CurBucketNoInner;
		hashTupleSlot = hjstate->hj_OuterHashTupleSlot;
	}

	for (;;)
	{
		/*
		 * hj_CurTuple is the address of the tuple last returned from the
		 * current bucket, or NULL if it's time to start scanning a new
		 * bucket.
		 */
		if (hashTuple != NULL)
			hashTuple = hashTuple->next.unshared;
		else if (*CurBucketNoRef < hashtable->nbuckets)
		{
			hashTuple = hashtable->buckets.unshared[*CurBucketNoRef];
			(*CurBucketNoRef)++;
		}
		else
			break;				/* finished all buckets */

		while (hashTuple != NULL)
		{
			if (!HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple)))
			{
				TupleTableSlot *restuple;

				/* insert hashtable's tuple into exec slot */
				restuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
												 hashTupleSlot,
												 false);	/* do not pfree */
				if(sourceTable == 0) {
					econtext->ecxt_innertuple = restuple;
				} else {
					econtext->ecxt_outertuple = restuple;
				}

				/*
				 * Reset temp memory each time; although this function doesn't
				 * do any qual eval, the caller will, so let's keep it
				 * parallel to ExecScanHashBucket.
				 */
				ResetExprContext(econtext);

				if(sourceTable == 0) {
					hjstate->hj_CurTupleInner = hashTuple;
				} else {
					hjstate->hj_CurTupleOuter = hashTuple;
				}
				return true;
			}

			hashTuple = hashTuple->next.unshared;
		}

		/* allow this loop to be cancellable */
		CHECK_FOR_INTERRUPTS();
	}

	/*
	 * no more unmatched tuples
	 */
	return false;
}

/*
 * ExecHashTableReset
 *
 *		reset hash table header for new batch
 */
void
ExecHashTableReset(HashJoinTable hashtable)
{
	MemoryContext oldcxt;
	int			nbuckets = hashtable->nbuckets;

	/*
	 * Release all the hash buckets and tuples acquired in the prior pass, and
	 * reinitialize the context for a new pass.
	 */
	MemoryContextReset(hashtable->batchCxt);
	oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

	/* Reallocate and reinitialize the hash bucket headers. */
	hashtable->buckets.unshared = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));

	hashtable->spaceUsed = 0;

	MemoryContextSwitchTo(oldcxt);

	/* Forget the chunks (the memory was freed by the context reset above). */
	hashtable->chunks = NULL;
}

/*
 * ExecHashTableResetMatchFlags
 *		Clear all the HeapTupleHeaderHasMatch flags in the table
 */
void
ExecHashTableResetMatchFlags(HashJoinTable hashtable)
{
	HashJoinTuple tuple;
	int			i;

	/* Reset all flags in the main table ... */
	for (i = 0; i < hashtable->nbuckets; i++)
	{
		for (tuple = hashtable->buckets.unshared[i]; tuple != NULL;
			 tuple = tuple->next.unshared)
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
	}

	/* ... and the same for the skew buckets, if any */
	for (i = 0; i < hashtable->nSkewBuckets; i++)
	{
		int			j = hashtable->skewBucketNums[i];
		HashSkewBucket *skewBucket = hashtable->skewBucket[j];

		for (tuple = skewBucket->tuples; tuple != NULL; tuple = tuple->next.unshared)
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
	}
}


void
ExecReScanHash(HashState *node)
{
	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}

/*
 * ExecHashGetSkewBucket
 *
 *		Returns the index of the skew bucket for this hashvalue,
 *		or INVALID_SKEW_BUCKET_NO if the hashvalue is not
 *		associated with any active skew bucket.
 */
int
ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue)
{
	int			bucket;

	/*
	 * Always return INVALID_SKEW_BUCKET_NO if not doing skew optimization (in
	 * particular, this happens after the initial batch is done).
	 */
	if (!hashtable->skewEnabled)
		return INVALID_SKEW_BUCKET_NO;

	/*
	 * Since skewBucketLen is a power of 2, we can do a modulo by ANDing.
	 */
	bucket = hashvalue & (hashtable->skewBucketLen - 1);

	/*
	 * While we have not hit a hole in the hashtable and have not hit the
	 * desired bucket, we have collided with some other hash value, so try the
	 * next bucket location.
	 */
	while (hashtable->skewBucket[bucket] != NULL &&
		   hashtable->skewBucket[bucket]->hashvalue != hashvalue)
		bucket = (bucket + 1) & (hashtable->skewBucketLen - 1);

	/*
	 * Found the desired bucket?
	 */
	if (hashtable->skewBucket[bucket] != NULL)
		return bucket;

	/*
	 * There must not be any hashtable entry for this hash value.
	 */
	return INVALID_SKEW_BUCKET_NO;
}

/*
 * Reserve space in the DSM segment for instrumentation data.
 */
void
ExecHashEstimate(HashState *node, ParallelContext *pcxt)
{
	size_t		size;

	/* don't need this if not instrumenting or no workers */
	if (!node->ps.instrument || pcxt->nworkers == 0)
		return;

	size = mul_size(pcxt->nworkers, sizeof(HashInstrumentation));
	size = add_size(size, offsetof(SharedHashInfo, hinstrument));
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * Set up a space in the DSM for all workers to record instrumentation data
 * about their hash table.
 */
void
ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt)
{
	size_t		size;

	/* don't need this if not instrumenting or no workers */
	if (!node->ps.instrument || pcxt->nworkers == 0)
		return;

	size = offsetof(SharedHashInfo, hinstrument) +
		pcxt->nworkers * sizeof(HashInstrumentation);
	node->shared_info = (SharedHashInfo *) shm_toc_allocate(pcxt->toc, size);
	memset(node->shared_info, 0, size);
	node->shared_info->num_workers = pcxt->nworkers;
	shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id,
				   node->shared_info);
}

/*
 * Locate the DSM space for hash table instrumentation data that we'll write
 * to at shutdown time.
 */
void
ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt)
{
	SharedHashInfo *shared_info;

	/* don't need this if not instrumenting */
	if (!node->ps.instrument)
		return;

	shared_info = (SharedHashInfo *)
		shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
	node->hinstrument = &shared_info->hinstrument[ParallelWorkerNumber];
}

/*
 * Copy instrumentation data from this worker's hash table (if it built one)
 * to DSM memory so the leader can retrieve it.  This must be done in an
 * ExecShutdownHash() rather than ExecEndHash() because the latter runs after
 * we've detached from the DSM segment.
 */
void
ExecShutdownHash(HashState *node)
{
	if (node->hinstrument && node->hashtable)
		ExecHashGetInstrumentation(node->hinstrument, node->hashtable);
}

/*
 * Retrieve instrumentation data from workers before the DSM segment is
 * detached, so that EXPLAIN can access it.
 */
void
ExecHashRetrieveInstrumentation(HashState *node)
{
	SharedHashInfo *shared_info = node->shared_info;
	size_t		size;

	if (shared_info == NULL)
		return;

	/* Replace node->shared_info with a copy in backend-local memory. */
	size = offsetof(SharedHashInfo, hinstrument) +
		shared_info->num_workers * sizeof(HashInstrumentation);
	node->shared_info = palloc(size);
	memcpy(node->shared_info, shared_info, size);
}

/*
 * Copy the instrumentation data from 'hashtable' into a HashInstrumentation
 * struct.
 */
void
ExecHashGetInstrumentation(HashInstrumentation *instrument,
						   HashJoinTable hashtable)
{
	instrument->nbuckets = hashtable->nbuckets;
	instrument->nbuckets_original = hashtable->nbuckets_original;
	instrument->nbatch = hashtable->nbatch;
	instrument->nbatch_original = hashtable->nbatch_original;
	instrument->space_peak = hashtable->spacePeak;
}

/*
 * Allocate 'size' bytes from the currently active HashMemoryChunk
 */
static void *
dense_alloc(HashJoinTable hashtable, Size size)
{
	HashMemoryChunk newChunk;
	char	   *ptr;

	/* just in case the size is not already aligned properly */
	size = MAXALIGN(size);

	/*
	 * If tuple size is larger than threshold, allocate a separate chunk.
	 */
	if (size > HASH_CHUNK_THRESHOLD)
	{
		/* allocate new chunk and put it at the beginning of the list */
		newChunk = (HashMemoryChunk) MemoryContextAlloc(hashtable->batchCxt,
														HASH_CHUNK_HEADER_SIZE + size);
		newChunk->maxlen = size;
		newChunk->used = size;
		newChunk->ntuples = 1;

		/*
		 * Add this chunk to the list after the first existing chunk, so that
		 * we don't lose the remaining space in the "current" chunk.
		 */
		if (hashtable->chunks != NULL)
		{
			newChunk->next = hashtable->chunks->next;
			hashtable->chunks->next.unshared = newChunk;
		}
		else
		{
			newChunk->next.unshared = hashtable->chunks;
			hashtable->chunks = newChunk;
		}

		return HASH_CHUNK_DATA(newChunk);
	}

	/*
	 * See if we have enough space for it in the current chunk (if any). If
	 * not, allocate a fresh chunk.
	 */
	if ((hashtable->chunks == NULL) ||
		(hashtable->chunks->maxlen - hashtable->chunks->used) < size)
	{
		/* allocate new chunk and put it at the beginning of the list */
		newChunk = (HashMemoryChunk) MemoryContextAlloc(hashtable->batchCxt,
														HASH_CHUNK_HEADER_SIZE + HASH_CHUNK_SIZE);

		newChunk->maxlen = HASH_CHUNK_SIZE;
		newChunk->used = size;
		newChunk->ntuples = 1;

		newChunk->next.unshared = hashtable->chunks;
		hashtable->chunks = newChunk;

		return HASH_CHUNK_DATA(newChunk);
	}

	/* There is enough space in the current chunk, let's add the tuple */
	ptr = HASH_CHUNK_DATA(hashtable->chunks) + hashtable->chunks->used;
	hashtable->chunks->used += size;
	hashtable->chunks->ntuples += 1;

	/* return pointer to the start of the tuple memory */
	return ptr;
}

/*
 * If we are currently attached to a shared hash join batch, detach.  If we
 * are last to detach, clean up.
 */
void
ExecHashTableDetachBatch(HashJoinTable hashtable)
{
	if (hashtable->parallel_state != NULL &&
		hashtable->curbatch >= 0)
	{
		int			curbatch = hashtable->curbatch;
		ParallelHashJoinBatch *batch = hashtable->batches[curbatch].shared;

		/* Make sure any temporary files are closed. */
		sts_end_parallel_scan(hashtable->batches[curbatch].inner_tuples);
		sts_end_parallel_scan(hashtable->batches[curbatch].outer_tuples);

		/* Detach from the batch we were last working on. */
		if (BarrierArriveAndDetach(&batch->batch_barrier))
		{
			/*
			 * Technically we shouldn't access the barrier because we're no
			 * longer attached, but since there is no way it's moving after
			 * this point it seems safe to make the following assertion.
			 */
			Assert(BarrierPhase(&batch->batch_barrier) == PHJ_BATCH_DONE);

			/* Free shared chunks and buckets. */
			while (DsaPointerIsValid(batch->chunks))
			{
				HashMemoryChunk chunk =
				dsa_get_address(hashtable->area, batch->chunks);
				dsa_pointer next = chunk->next.shared;

				dsa_free(hashtable->area, batch->chunks);
				batch->chunks = next;
			}
			if (DsaPointerIsValid(batch->buckets))
			{
				dsa_free(hashtable->area, batch->buckets);
				batch->buckets = InvalidDsaPointer;
			}
		}

		/*
		 * Track the largest batch we've been attached to.  Though each
		 * backend might see a different subset of batches, explain.c will
		 * scan the results from all backends to find the largest value.
		 */
		hashtable->spacePeak =
			Max(hashtable->spacePeak,
				batch->size + sizeof(dsa_pointer_atomic) * hashtable->nbuckets);

		/* Remember that we are not attached to a batch. */
		hashtable->curbatch = -1;
	}
}

/*
 * Detach from all shared resources.  If we are last to detach, clean up.
 */
void
ExecHashTableDetach(HashJoinTable hashtable)
{
	if (hashtable->parallel_state)
	{
		ParallelHashJoinState *pstate = hashtable->parallel_state;
		int			i;

		/* Make sure any temporary files are closed. */
		if (hashtable->batches)
		{
			for (i = 0; i < hashtable->nbatch; ++i)
			{
				sts_end_write(hashtable->batches[i].inner_tuples);
				sts_end_write(hashtable->batches[i].outer_tuples);
				sts_end_parallel_scan(hashtable->batches[i].inner_tuples);
				sts_end_parallel_scan(hashtable->batches[i].outer_tuples);
			}
		}

		/* If we're last to detach, clean up shared memory. */
		if (BarrierDetach(&pstate->build_barrier))
		{
			if (DsaPointerIsValid(pstate->batches))
			{
				dsa_free(hashtable->area, pstate->batches);
				pstate->batches = InvalidDsaPointer;
			}
		}

		hashtable->parallel_state = NULL;
	}
}
