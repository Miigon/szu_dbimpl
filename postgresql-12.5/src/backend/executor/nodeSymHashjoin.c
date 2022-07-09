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
 *		ExecSymHashJoinImpl
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
ExecSymHashJoinImpl(PlanState *pstate, bool parallel)
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
					if (!ExecScanHashTableForUnmatchedSymmetric(node, econtext, 0))
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
					if (!ExecScanHashTableForUnmatchedSymmetric(node, econtext, 1))
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
 *		ExecSymHashJoin
 *
 *		Parallel-oblivious version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecSymHashJoin(PlanState *pstate)
{
	/*
	 * On sufficiently smart compilers this should be inlined with the
	 * parallel-aware branches removed.
	 */
	return ExecSymHashJoinImpl(pstate, false);
}
/* ----------------------------------------------------------------
 *		ExecInitSymHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitSymHashJoin(HashJoin *node, EState *estate, int eflags)
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
	hjstate->js.ps.ExecProcNode = ExecSymHashJoin;
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
ExecEndSymHashJoin(HashJoinState *node)
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
ExecShutdownSymHashJoin(HashJoinState *node)
{
	// elog(ERROR, "parallel symmetic hash join not supported.");
}

// void
// ExecHashJoinEstimate(HashJoinState *state, ParallelContext *pcxt)
// {
// 	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelHashJoinState));
// 	shm_toc_estimate_keys(&pcxt->estimator, 1);
// }
