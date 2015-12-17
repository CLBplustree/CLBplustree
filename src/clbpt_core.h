/**
 * @file The back-end header file of CLBPT.
 */

#ifndef __CLBPT_CORE_H_INCLUDED
#define __CLBPT_CORE_H_INCLUDED

#include "clbpt_type.h"
#include "kma.h"

#define PACKET_NOP (0x3FFFFFFF00000000L)
#define getKeyFromPacket(X) (int)(((X) >> 31) & 0x80000000 | ((X) >> 32) & 0x7FFFFFFF)
#define getUpperKeyFromRangePacket(X) (int)(((X) << 1) & 0x80000000 | (X) & 0x7FFFFFFF)
#define isNopPacket(X) ((X) == PACKET_NOP)
#define isSearchPacket(X) (!((uint8_t)((X) >> 63) & 0x1) && ((uint32_t)(X) == 0x7FFFFFFF))
#define isRangePacket(X) ((!((uint8_t)((X) >> 63) & 0x1)) && ((uint8_t)((X) >> 31) & 0x1))
#define isInsertPacket(X) (((uint8_t)((X) >> 63) & 0x1) && !((uint32_t)(X) == 0))
#define isDeletePacket(X) (((uint8_t)((X) >> 63) & 0x1) && ((uint32_t)(X) == 0))
#define getKeyFromEntry(X) (int)(((X.key) << 1) & 0x80000000 | (X.key) & 0x7FFFFFFF)
#define getKey(X) (int)(((X) << 1) & 0x80000000 | (X) & 0x7FFFFFFF)

//#define half_c(X) (((int)X+1)/2)
#define half_f(X) (((int)X)/2)

// insert_leaf
#define needInsertionRollback -1
// handle_node
#define leftMostNodeBorrowMerge -1
#define mergeWithLeftMostNode -2

void _clbptDebug(const char* Format, ...);

/**
 * @brief Get Devices' info of clbpt
 * @param platform.
 * @return Success or not.
 */
int _clbptGetDevices(clbpt_platform platform);

/**
 * @brief Create command queues of clbpt
 * @param platform.
 * @return Success or not.
 */
int _clbptCreateQueues(clbpt_platform platform);

/**
 * @brief Create kernels of clbpt
 * @param platform.
 * @return Success or not.
 */
int _clbptCreateKernels(clbpt_platform platform);

/**
 * @brief Initialization of clbpt
 * @param tree.
 * @return Success or not.
 */
int _clbptInitialize(clbpt_tree tree);

/**
 * @brief Select and sort from waitng buffer.
 * @param tree.
 * @return Is the waiting buffer empty or not.
 */
int _clbptSelectFromWaitBuffer(clbpt_tree tree);

/**
 * @brief Handle the executing buffer.
 * @param tree.
 * @return Is the executing buffer empty or not.
 */
int _clbptHandleExecuteBuffer(clbpt_tree tree);

/**
 * @brief initialization of clbpt
 * @param tree.
 * @return Success or not.
 */
int _clbptReleaseLeaf(clbpt_tree tree);

#endif /* __CLBPT_CORE_H_INCLUDED */
