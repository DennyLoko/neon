//
// Created by Matthias on 2022-12-12.
//

#ifndef NEON_NEON_LWLSNCACHE_H
#define NEON_NEON_LWLSNCACHE_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilenode.h"

/* Get the actual last written LSN, and fill the effective LwLSN */
XLogRecPtr GetLastWrittenLsnForBuffer(RelFileNode node, ForkNumber fork,
									  BlockNumber blkno, XLogRecPtr *effective);
XLogRecPtr GetLastWrittenLsnForRelFileNode(RelFileNode node, ForkNumber fork,
										   XLogRecPtr *effective);
XLogRecPtr GetLastWrittenLsnForDatabase(Oid datoid, XLogRecPtr *effective);
XLogRecPtr GetLastWrittenLsnForDbCluster(XLogRecPtr *effective);

void SetLastWrittenLsnForBuffer(XLogRecPtr lsn, RelFileNode node, ForkNumber fork, BlockNumber blkno);
void SetLastWrittenLsnForRelFileNode(XLogRecPtr lsn, RelFileNode node, ForkNumber fork);
void SetLastWrittenLsnForDatabase(XLogRecPtr lsn, Oid datoid);
void SetLastWrittenLsnForDbCluster(XLogRecPtr lsn);

#endif //NEON_NEON_LWLSNCACHE_H
