/**
 * @file The back-end header file of CLBPT.
 */

#ifndef __CLBPT_CORE_H_INCLUDED
#define __CLBPT_CORE_H_INCLUDED

#include "clbpt_type.h"

#define PACKET_NOP (0x3FFFFFFF00000000L)
#define getKeyFromPacket(X) (int)(((X) >> 31) & 0x80000000 | ((X) >> 32) & 0x7FFFFFFF)
#define isSearchPacket(X) ((!((uchar)((X) >> 63) & 0x1)) && ((uint)(X) == 0x7FFFFFFF))
#define isRangePacket(X) ((!((uchar)((X) >> 63) & 0x1)) && ((uchar)((X) >> 31) & 0x1))
#define isInsertPacket(X) (((uchar)((X) >> 63) & 0x1) && ((uint)(X) != 0))
#define isDeletePacket(X) (((uchar)((X) >> 63) & 0x1) && ((uint)(X) == 0))
#define getKeyFromEntry(X) (int)(((X.key) << 1) & 0x80000000 | (X.key) & 0x7FFFFFFF)

/**
 * @brief Select and sort from waitng buffer.
 * @param tree.
 * @return Is the waiting buffer empty or not.
 */
int _clbptSelectFromWaitBuffer(clbpt_tree tree);

/**
 * @brief Handle the executing buffer.
 * @param tree.
 * @return Success or not.
 */
int _clbptHandleExecuteBuffer(clbpt_tree tree);

/**
 * @brief initialization of clbpt
 * @param tree.
 * @return Success or not.
 */
int _clbptInitialize(clbpt_tree tree);

#endif /* __CLBPT_CORE_H_INCLUDED */
