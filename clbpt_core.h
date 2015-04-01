/**
 * @file The back-end header file of CLBPT.
 */

#ifndef __CLBPT_CORE_H_INCLUDED
#define __CLBPT_CORE_H_INCLUDED

#include "clbpt_type.h"

// Temporary. Replace this by compiler option later.
#define CLBPT_ORDER 8

#define PACKET_NOP (0x3FFFFFFF00000000L)
#define getKeyFromPacket(X) (int)(((X) >> 31) & 0x80000000 | ((X) >> 32) & 0x7FFFFFFF)
#define getUpperKeyFromRangePacket(X) (int)(((X) << 1) & 0x80000000 | (X) & 0x7FFFFFFF)
#define isSearchPacket(X) (!((uint8_t)((X) >> 63) & 0x1) && ((uint32_t)(X) == 0x7FFFFFFF))
#define isRangePacket(X) ((!((uint8_t)((X) >> 63) & 0x1)) && ((uint8_t)((X) >> 31) & 0x1))
#define isInsertPacket(X) (((uint8_t)((X) >> 63) & 0x1) && !((uint32_t)(X) == 0))
#define isDeletePacket(X) (((uint8_t)((X) >> 63) & 0x1) && ((uint32_t)(X) == 0))
#define getKeyFromEntry(X) (int)(((X.key) << 1) & 0x80000000 | (X.key) & 0x7FFFFFFF)

#define half_c(X) (((int)X+1)/2)
#define half_f(X) (((int)X)/2)

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

/**
 * @brief initialization of clbpt
 * @param tree.
 * @return Success or not.
 */
int _clbptReleaseLeaf(clbpt_tree tree);

#endif /* __CLBPT_CORE_H_INCLUDED */
