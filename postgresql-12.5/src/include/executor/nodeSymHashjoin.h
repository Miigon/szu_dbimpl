/*-------------------------------------------------------------------------
 *
 * nodeSymHashjoin.h
 *	  prototypes for nodeSymHashjoin.c
 *
 * src/include/executor/nodeSymHashjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESYMHASHJOIN_H
#define NODESYMHASHJOIN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"
#include "storage/buffile.h"

extern SymHashJoinState *ExecInitSymHashJoin(SymHashJoin *node, EState *estate, int eflags);
extern void ExecEndSymHashJoin(SymHashJoinState *node);
extern void ExecShutdownSymHashJoin(SymHashJoinState *node);

#endif							/* NODESYMHASHJOIN_H */
