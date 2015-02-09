/**
 * @file The back-end header file of CLBPT.
 */

#ifndef __CLBPT_CORE_H_INCLUDED
#define __CLBPT_CORE_H_INCLUDED

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

#endif /* __CLBPT_CORE_H_INCLUDED */
