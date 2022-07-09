/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 * PARALLELISM
 *
 * Hash joins can participate in parallel query execution in several ways.  A
 * parallel-oblivious hash join is one where the node is unaware that it is
 * part of a parallel plan.  In this case, a copy of the inner plan is used to
 * build a copy of the hash table in every backend, and the outer plan could
 * either be built from a partial or complete path, so that the results of the
 * hash join are correspondingly either partial or complete.  A parallel-aware
 * hash join is one that behaves differently, coordinating work between
 * backends, and appears as Parallel Hash Join in EXPLAIN output.  A Parallel
 * Hash Join always appears with a Parallel Hash node.
 *
 * Parallel-aware hash joins use the same per-backend state machine to track
 * progress through the hash join algorithm as parallel-oblivious hash joins.
 * In a parallel-aware hash join, there is also a shared state machine that
 * co-operating backends use to synchronize their local state machines and
 * program counters.  The shared state machine is managed with a Barrier IPC
 * primitive.  When all attached participants arrive at a barrier, the phase
 * advances and all waiting participants are released.
 *
 * When a participant begins working on a parallel hash join, it must first
 * figure out how much progress has already been made, because participants
 * don't wait for each other to begin.  For this reason there are switch
 * statements at key points in the code where we have to synchronize our local
 * state machine with the phase, and then jump to the correct part of the
 * algorithm so that we can get started.
 *
 * One barrier called build_barrier is used to coordinate the hashing phases.
 * The phase is represented by an integer which begins at zero and increments
 * one by one, but in the code it is referred to by symbolic names as follows:
 *
 *   PHJ_BUILD_ELECTING              -- initial state
 *   PHJ_BUILD_ALLOCATING            -- one sets up the batches and table 0
 *   PHJ_BUILD_HASHING_INNER         -- all hash the inner rel
 *   PHJ_BUILD_HASHING_OUTER         -- (multi-batch only) all hash the outer
 *   PHJ_BUILD_DONE                  -- building done, probing can begin
 *
 * While in the phase PHJ_BUILD_HASHING_INNER a separate pair of barriers may
 * be used repeatedly as required to coordinate expansions in the number of
 * batches or buckets.  Their phases are as follows:
 *
 *   PHJ_GROW_BATCHES_ELECTING       -- initial state
 *   PHJ_GROW_BATCHES_ALLOCATING     -- one allocates new batches
 *   PHJ_GROW_BATCHES_REPARTITIONING -- all repartition
 *   PHJ_GROW_BATCHES_FINISHING      -- one cleans up, detects skew
 *
 *   PHJ_GROW_BUCKETS_ELECTING       -- initial state
 *   PHJ_GROW_BUCKETS_ALLOCATING     -- one allocates new buckets
 *   PHJ_GROW_BUCKETS_REINSERTING    -- all insert tuples
 *
 * If the planner got the number of batches and buckets right, those won't be
 * necessary, but on the other hand we might finish up needing to expand the
 * buckets or batches multiple times while hashing the inner relation to stay
 * within our memory budget and load factor target.  For that reason it's a
 * separate pair of barriers using circular phases.
 *
 * The PHJ_BUILD_HASHING_OUTER phase is required only for multi-batch joins,
 * because we need to divide the outer relation into batches up front in order
 * to be able to process batches entirely independently.  In contrast, the
 * parallel-oblivious algorithm simply throws tuples 'forward' to 'later'
 * batches whenever it encounters them while scanning and probing, which it
 * can do because it processes batches in serial order.
 *
 * Once PHJ_BUILD_DONE is reached, backends then split up and process
 * different batches, or gang up and work together on probing batches if there
 * aren't enough to go around.  For each batch there is a separate barrier
 * with the following phases:
 *
 *  PHJ_BATCH_ELECTING       -- initial state
 *  PHJ_BATCH_ALLOCATING     -- one allocates buckets
 *  PHJ_BATCH_LOADING        -- all load the hash table from disk
 *  PHJ_BATCH_PROBING        -- all probe
 *  PHJ_BATCH_DONE           -- end
 *
 * Batch 0 is a special case, because it starts out in phase
 * PHJ_BATCH_PROBING; populating batch 0's hash table is done during
 * PHJ_BUILD_HASHING_INNER so we can skip loading.
 *
 * Initially we try to plan for a single-batch hash join using the combined
 * work_mem of all participants to create a large shared hash table.  If that
 * turns out either at planning or execution time to be impossible then we
 * fall back to regular work_mem sized hash tables.
 *
 * To avoid deadlocks, we never wait for any barrier unless it is known that
 * all other backends attached to it are actively executing the node or have
 * already arrived.  Practically, that means that we never return a tuple
 * while attached to a barrier, unless the barrier has reached its final
 * state.  In the slightly special case of the per-batch barrier, we return
 * tuples while in PHJ_BATCH_PROBING phase, but that's OK because we use
 * BarrierArriveAndDetach() to advance it to PHJ_BATCH_DONE without waiting.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"


/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_PROBE_INNER			3
#define HJ_NEED_NEW_INNER		4
#define HJ_PROBE_OUTER			5
#define HJ_FILL_TUPLES	 		6
const char *debugstatemap[] = {
	"ZERO", "HJ_BUILD_HASHTABLE", "HJ_NEED_NEW_OUTER",
	"HJ_PROBE_INNER", "HJ_NEED_NEW_INNER", "HJ_PROBE_OUTER",
	"HJ_FILL_TUPLES"
};
// #define HJ_NEED_NEW_BATCH		8
// NOTE: no batching for ass2

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

/* ----------------------------------------------------------------
 *		ExecHashJoinImpl
 *
 *		This function implements the Hybrid Hashjoin algorithm.  It is marked
 *		with an always-inline attribute so that ExecHashJoin() and
 *		ExecParallelHashJoin() can inline it.  Compilers that respect the
 *		attribute should create versions specialized for parallel == true and
 *		parallel == false with unnecessary branches removed.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
static pg_attribute_always_inline TupleTableSlot *
ExecHashJoinImpl(PlanState *pstate, bool parallel)
{
	HashJoinState *node = castNode(HashJoinState, pstate);
	HashState  *hashNodeInner;
	HashState  *hashNodeOuter;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	HashJoinTable hashtableInner;
	HashJoinTable hashtableOuter;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	uint32		hashvalue;
	int			batchno;

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNodeInner = (HashState *) innerPlanState(node);
	hashNodeOuter = (HashState *) outerPlanState(node);
	hashtableInner = node->hj_HashTableInner;
	hashtableOuter = node->hj_HashTableOuter;
	econtext = node->js.ps.ps_ExprContext;
	// parallel_state = hashNode->parallel_state; // NOTE: ignored in ass2

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * run the hash join state machine
	 */
	for (;;)
	{
		static laststate = 0;
		if(node->hj_JoinState != laststate) {
			laststate = node->hj_JoinState;	
			ELOGDEBUG("state: %s", debugstatemap[node->hj_JoinState]);
		}
		/*
		 * It's possible to iterate this loop many times before returning a
		 * tuple, in some pathological cases such as needing to move much of
		 * the current batch to a later batch.  So let's check for interrupts
		 * each time through.
		 */
		CHECK_FOR_INTERRUPTS();

		switch (node->hj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner and outer relation.
				 */
				Assert(hashtableInner == NULL);
				Assert(hashtableOuter == NULL);

				// NOTE: empty-outer optimization removed here. 

				hashNodeInner->debugInnerOuter = 0;
				hashNodeOuter->debugInnerOuter = 1;

				hashtableInner = ExecHashTableCreate(hashNodeInner,
												node->hj_HashOperators,
												node->hj_Collations,
												HJ_FILL_INNER(node));
				node->hj_HashTableInner = hashtableInner;

				hashtableOuter = ExecHashTableCreate(hashNodeOuter,
												node->hj_HashOperators,
												node->hj_Collations,
												HJ_FILL_OUTER(node));
				node->hj_HashTableOuter = hashtableOuter;

				hashNodeInner->hashtable = hashtableInner;
				hashNodeOuter->hashtable = hashtableOuter;

				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */
				// hashtable->nbatch_outstart = hashtable->nbatch;
				// NOTE: ignored in ass2;

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->hj_OuterNotEmpty = false;
				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				/*
				 * Try to get the next outer tuple
				 * (hash node will insert it into the outer hashtable for us)
				 */
				outerTupleSlot = ExecProcNode((PlanState *)hashNodeOuter);

				if (TupIsNull(outerTupleSlot))
				{
					node->hj_OuterExhausted = true;

					if(node->hj_InnerExhausted) {
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_TUPLES;
					} else {
						node->hj_JoinState = HJ_NEED_NEW_INNER;
					}

					
					continue;
				}

				econtext->ecxt_outertuple = outerTupleSlot;

				// get hash value of the new outer tuple in inner table
				// ecxt_outertuple is the target tuple to get hash value from, according to ExecHashGetHashValue
				hashNodeInner->ps.ps_ExprContext->ecxt_outertuple = outerTupleSlot;
            	ExecHashGetHashValue(
					hashtableInner, hashNodeInner->ps.ps_ExprContext,
					hashNodeOuter->hashkeys, true, /* outer tuple */
					hashtableInner->keepNulls, &hashvalue);

				/*
				 * Find the corresponding bucket for this tuple in the inner hash table
				 */
				
				node->hj_CurHashValueOuter = hashvalue;
				ExecHashGetBucketAndBatch(hashtableInner, hashvalue,
										  &node->hj_CurBucketNoOuter, &batchno);
				node->hj_CurTupleOuter = hashNodeOuter->lastInsert;
				node->hj_CurTupleInner = NULL;

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_PROBE_INNER;

			case HJ_PROBE_INNER:
				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecProbeInnerHashBucketWithOuterTuple(node, econtext))
				{
					/* out of matches; asks for more inner to probe */
					node->hj_JoinState = node->hj_InnerExhausted?HJ_NEED_NEW_OUTER:HJ_NEED_NEW_INNER;
					continue;
				}

				ELOGDEBUG("potential %p and %p as match" , node->hj_CurTupleInner, node->hj_CurTupleOuter);

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecProbeHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTupleOuter));
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTupleInner));
					ELOGDEBUG("marked %p and %p as match" , node->hj_CurTupleInner, node->hj_CurTupleOuter);

					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						// could be NEED_NEW_INNER, doesn't really matter.
						// node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}

					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					//if (node->js.single_match)
						//node->hj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_NEED_NEW_INNER: 
			// NOTE: bad practice! should really reuse code with HJ_NEED_NEW_OUTER
			// instead of copying over.

				/*
				 * Try to get the next inner tuple
				 * (hash node will insert it into the inner hashtable for us)
				 */
				innerTupleSlot = ExecProcNode((PlanState *)hashNodeInner);

				if (TupIsNull(innerTupleSlot))
				{
					node->hj_InnerExhausted = true;

					if(node->hj_OuterExhausted) {
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_TUPLES;
					} else {
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
					}

					continue;
				}

				econtext->ecxt_innertuple = innerTupleSlot;

				// get hash value of the new inner tuple in outer table
				// ecxt_outertuple is the target tuple to get hash value from, according to ExecHashGetHashValue
				hashNodeOuter->ps.ps_ExprContext->ecxt_outertuple = innerTupleSlot;
				ExecHashGetHashValue(
					hashtableOuter, hashNodeOuter->ps.ps_ExprContext,
					hashNodeInner->hashkeys, true, /* outer tuple */
					hashtableOuter->keepNulls, &hashvalue);

				/*
				 * Find the corresponding bucket for this tuple in the outer hash table
				 */
				node->hj_CurHashValueInner = hashvalue;
				ExecHashGetBucketAndBatch(hashtableOuter, hashvalue,
										  &node->hj_CurBucketNoInner, &batchno);
				node->hj_CurTupleInner = hashNodeInner->lastInsert;
				node->hj_CurTupleOuter = NULL;

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_PROBE_OUTER;

			case HJ_PROBE_OUTER:
			// NOTE: bad practice! should really reuse code with HJ_PROBE_INNER
			// instead of copying over.

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecProbeOuterHashBucketWithInnerTuple(node, econtext))
				{
					ELOGDEBUG("out of match");
					/* out of matches; ask for more outers to probe */
					node->hj_JoinState = node->hj_OuterExhausted?HJ_NEED_NEW_INNER:HJ_NEED_NEW_OUTER;
					continue;
				}
				ELOGDEBUG("potential %p and %p as match" , node->hj_CurTupleInner, node->hj_CurTupleOuter);

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecProbeHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTupleOuter));
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTupleInner));
					ELOGDEBUG("marked %p and %p as match" , node->hj_CurTupleInner, node->hj_CurTupleOuter);

					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						// could be NEED_NEW_OUTER, doesn't really matter.
						// node->hj_JoinState = HJ_NEED_NEW_INNER;
						continue;
					}

					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next inner tuple.
					 */
					if (node->js.single_match) {
						//node->hj_JoinState = HJ_NEED_NEW_INNER;
						ELOGDEBUG("**** single match, end");
					}
						

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_TUPLES:
				// last step: null-fill unmatched tuples when necessary

				if(node->hj_InnerNeedFill) {
					/*
					* We have finished matching, but we are doing left/full join,
					* so any unmatched inner tuples in the hashtable have to be
					* emitted.
					*/
					if (!ExecScanHashTableForUnmatched(node, econtext, 0))
					{
						/* no more unmatched tuples */
						node->hj_InnerNeedFill = false;
						continue;
					}

					/*
					* Generate a fake join tuple with nulls for the outer tuple,
					* and return it if it passes the non-join quals.
					*/
					econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				} else if(node->hj_OuterNeedFill) {
					/*
					* We have finished matching, but we are doing right/full join,
					* so any unmatched outer tuples in the hashtable have to be
					* emitted.
					*/
					if (!ExecScanHashTableForUnmatched(node, econtext, 1))
					{
						/* no more unmatched tuples */
						node->hj_OuterNeedFill = false;
						continue;
					}
					ELOGDEBUG("right");

					/*
					* Generate a fake join tuple with nulls for the inner tuple,
					* and return it if it passes the non-join quals.
					*/
					econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				} else {
					// finish hashjoin
					return NULL;
				}

				break;

			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		Parallel-oblivious version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoin(PlanState *pstate)
{
	/*
	 * On sufficiently smart compilers this should be inlined with the
	 * parallel-aware branches removed.
	 */
	return ExecHashJoinImpl(pstate, false);
}

/* ----------------------------------------------------------------
 *		ExecParallelHashJoin
 *
 *		Parallel-aware version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecParallelHashJoin(PlanState *pstate)
{
	/*
	 * On sufficiently smart compilers this should be inlined with the
	 * parallel-oblivious branches removed.
	 */
	elog(ERROR, "parallel symmetric hash join not supported. (assignment2)");
	return ExecHashJoinImpl(pstate, true);
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
	HashJoinState *hjstate;
	Plan	   *outerNode;
	Hash	   *innerNode;
	TupleDesc	outerDesc,
				innerDesc;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;

	/*
	 * See ExecHashJoinInitializeDSM() and ExecHashJoinInitializeWorker()
	 * where this function may be replaced with a parallel version, if we
	 * managed to launch a parallel query.
	 */
	hjstate->js.ps.ExecProcNode = ExecHashJoin;
	hjstate->js.jointype = node->join.jointype;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	innerNode = innerPlan(node);

	outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
	outerDesc = ExecGetResultType(outerPlanState(hjstate));
	innerPlanState(hjstate) = ExecInitNode(innerNode, estate, eflags);
	innerDesc = ExecGetResultType(innerPlanState(hjstate));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&hjstate->js.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	hjstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
			break;
		case JOIN_RIGHT:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
			break;
		case JOIN_FULL:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 * 
	 * more voodoo: did the same with outer plan too lol XD -miigon 7/9/22
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_InnerHashTupleSlot = slot;
	}
	{
		HashState  *hashstate = (HashState *) outerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_OuterHashTupleSlot = slot;
	}

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
	hjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
	hjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hjstate);

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTableOuter = NULL;
	hjstate->hj_HashTableInner = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValueOuter = 0;
	hjstate->hj_CurHashValueInner = 0;
	hjstate->hj_CurBucketNoOuter = 0;
	hjstate->hj_CurBucketNoInner = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTupleOuter = NULL;
	hjstate->hj_CurTupleInner = NULL;

	hjstate->hj_OuterHashKeys = ExecInitExprList(node->hashkeys,
												 (PlanState *) hjstate);
	hjstate->hj_HashOperators = node->hashoperators;
	hjstate->hj_Collations = node->hashcollations;

	hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
	hjstate->hj_OuterNotEmpty = false;

	hjstate->hj_InnerExhausted = false;
	hjstate->hj_OuterExhausted = false;
	hjstate->hj_OuterNeedFill = HJ_FILL_OUTER(hjstate);
	hjstate->hj_InnerNeedFill = HJ_FILL_INNER(hjstate);

	return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{
	/*
	 * Free hash tables
	 */
	if (node->hj_HashTableOuter)
	{
		ExecHashTableDestroy(node->hj_HashTableOuter);
		node->hj_HashTableOuter = NULL;
	}
	if (node->hj_HashTableInner)
	{
		ExecHashTableDestroy(node->hj_HashTableInner);
		node->hj_HashTableInner = NULL;
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->hj_OuterHashTupleSlot);
	ExecClearTuple(node->hj_InnerHashTupleSlot);
	

	/*
	 * clean up subtrees
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}

void
ExecReScanHashJoin(HashJoinState *node)
{
	/*
	 * In a multi-batch join, we currently have to do rescans the hard way,
	 * primarily because batch temp files may have already been released. But
	 * if it's a single-batch join, and there is no parameter change for the
	 * inner subnode, then we can just re-use the existing hash table without
	 * rebuilding it.
	 */
	if (node->hj_HashTableOuter != NULL)
	{
		/* must destroy and rebuild hash table */
		HashState  *hashNode = castNode(HashState, outerPlanState(node));

		/* for safety, be sure to clear child plan node's pointer too */
		Assert(hashNode->hashtable == node->hj_HashTableOuter);
		hashNode->hashtable = NULL;

		ExecHashTableDestroy(node->hj_HashTableOuter);
		node->hj_HashTableOuter = NULL;
		node->hj_JoinState = HJ_BUILD_HASHTABLE;
	}

	if (node->hj_HashTableInner != NULL)
	{
		/* must destroy and rebuild hash table */
		HashState  *hashNode = castNode(HashState, innerPlanState(node));

		/* for safety, be sure to clear child plan node's pointer too */
		Assert(hashNode->hashtable == node->hj_HashTableInner);
		hashNode->hashtable = NULL;

		ExecHashTableDestroy(node->hj_HashTableInner);
		node->hj_HashTableInner = NULL;
		node->hj_JoinState = HJ_BUILD_HASHTABLE;
	}

	/* Always reset intra-tuple state */
	node->hj_CurHashValueOuter = 0;
	node->hj_CurBucketNoOuter = 0;
	node->hj_CurTupleInner = NULL;
	node->hj_CurHashValueInner = 0;
	node->hj_CurBucketNoInner = 0;
	node->hj_CurTupleOuter = NULL;
	node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;

	node->hj_FirstOuterTupleSlot = NULL;

	node->hj_InnerExhausted = false;
	node->hj_OuterExhausted = false;
	node->hj_OuterNeedFill = HJ_FILL_OUTER(node);
	node->hj_InnerNeedFill = HJ_FILL_INNER(node);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->js.ps.lefttree->chgParam == NULL)
		ExecReScan(node->js.ps.lefttree);
	if (node->js.ps.righttree->chgParam == NULL)
		ExecReScan(node->js.ps.righttree);
}

void
ExecShutdownHashJoin(HashJoinState *node)
{
	// elog(ERROR, "parallel symmetic hash join not supported.");
}

void
ExecHashJoinEstimate(HashJoinState *state, ParallelContext *pcxt)
{
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelHashJoinState));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecHashJoinInitializeDSM(HashJoinState *state, ParallelContext *pcxt)
{
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	HashState  *hashNode;
	ParallelHashJoinState *pstate;

	/*
	 * Disable shared hash table mode if we failed to create a real DSM
	 * segment, because that means that we don't have a DSA area to work with.
	 */
	if (pcxt->seg == NULL)
		return;

	ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);

	/*
	 * Set up the state needed to coordinate access to the shared hash
	 * table(s), using the plan node ID as the toc key.
	 */
	pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelHashJoinState));
	shm_toc_insert(pcxt->toc, plan_node_id, pstate);

	/*
	 * Set up the shared hash join state with no batches initially.
	 * ExecHashTableCreate() will prepare at least one later and set nbatch
	 * and space_allowed.
	 */
	pstate->nbatch = 0;
	pstate->space_allowed = 0;
	pstate->batches = InvalidDsaPointer;
	pstate->old_batches = InvalidDsaPointer;
	pstate->nbuckets = 0;
	pstate->growth = PHJ_GROWTH_OK;
	pstate->chunk_work_queue = InvalidDsaPointer;
	pg_atomic_init_u32(&pstate->distributor, 0);
	pstate->nparticipants = pcxt->nworkers + 1;
	pstate->total_tuples = 0;
	LWLockInitialize(&pstate->lock,
					 LWTRANCHE_PARALLEL_HASH_JOIN);
	BarrierInit(&pstate->build_barrier, 0);
	BarrierInit(&pstate->grow_batches_barrier, 0);
	BarrierInit(&pstate->grow_buckets_barrier, 0);

	/* Set up the space we'll use for shared temporary files. */
	SharedFileSetInit(&pstate->fileset, pcxt->seg);

	/* Initialize the shared state in the hash node. */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->parallel_state = pstate;
}

/* ----------------------------------------------------------------
 *		ExecHashJoinReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecHashJoinReInitializeDSM(HashJoinState *state, ParallelContext *cxt)
{
	// elog(ERROR, "parallel symmetic hash join not supported.");
}

void
ExecHashJoinInitializeWorker(HashJoinState *state,
							 ParallelWorkerContext *pwcxt)
{
	HashState  *hashNode;
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	ParallelHashJoinState *pstate =
	shm_toc_lookup(pwcxt->toc, plan_node_id, false);

	/* Attach to the space for shared temporary files. */
	SharedFileSetAttach(&pstate->fileset, pwcxt->seg);

	/* Attach to the shared state in the hash node. */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->parallel_state = pstate;

	ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
}
