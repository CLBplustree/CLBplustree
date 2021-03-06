/**
 * @file The header file of CLBPT.
 */

#ifndef __CLBPT_H_INCLUDED
#define __CLBPT_H_INCLUDED
	
#include "clbpt_type.h"

#define CLBPT_SUCCESS 0

#define CLBPT_UNKNOWN_ERROR 5566

int clbptDebug(const char* Format, ...);

/**
 * @brief Create a new CLBPT platform.
 * @param dst_platform The platform.
 * @param context OpenCL context.
 * @return Error code.
 */
int clbptCreatePlatform(clbpt_platform *dst_platform_ptr, cl_context context);

/**
 * @brief Release a CLBPT platform.
 * @param platform The platform.
 * @return Error code.
 */
int clbptReleasePlatform(clbpt_platform platform);

/**
 * @brief Create a new CLBPT.
 * @param dst_tree_ptr The address of B+tree.
 * @param platform CLBPT platform.
 * @param order The order of the B+tree. 0 for maximum available order.
 * @param record_size The size of record in byte.
 * @return Error code.
 */
int clbptCreateTree(clbpt_tree *dst_tree_ptr, clbpt_platform platform, const int order, const size_t record_size);

/**
 * @brief Release clbpt_tree.
 * @param tree Target clbpt_tree.
 * @return Error code.
 */
int clbptReleaseTree(clbpt_tree tree);

/**
 * @brief Create a pair group list.
 * @param pair_group_list_ptr The address of pair group list.
 * @param num_pair_groups The number of pair groups for result of range searches.
 * @param l_keys The array of lower keys.
 * @param u_keys The array of upper keys.
 * @return Error code.
 */
int clbptCreatePairGroupList(clbpt_pair_group_list *pair_group_list_ptr, int num_pair_groups, CLBPT_KEY_TYPE *l_keys, CLBPT_KEY_TYPE *u_keys);

/**
 * @brief Release the pair group list.
 * @param pair_group_list_ptr The address of pair group list.
 * @param num_pair_groups The number of pair groups for result of range searches.
 * @return Error code.
 */
int clbptReleasePairGroupList(clbpt_pair_group_list *pair_group_list_ptr, int num_pair_groups);

/**
 * @brief Enqueue multiple searches instruction.
 *
 * This function is non-blocking. The search results will store in record_ptrs after finishing processing.
 * While the search is failed, the position will filled NULL to indicate.
 *
 * @param tree The CLBPT.
 * @param num_keys Specify the number of input keys.
 * @param keys An array of query keys with size of num_keys.
 * @param records After search instructions complete, this array will be filled of records.
 * @return Error code.
 */
int clbptEnqueueSearches(clbpt_tree tree, int num_keys, CLBPT_KEY_TYPE *keys, void **record_list);

/**
 * @brief Enqueue multiple range searches instruction.
 *
 * This function is non-blocking. The search results will store in record_list_array after finishing processing.
 * The index of one element in record_list_array is identical to the index of its corresponding key pair.
 * Each element consists of a list of pointers pointing to records whose keys are inside the range.
 * The last of the list is put a NULL pointer.
 * Users can allocate an list with size of (u_keys - l_keys + 2) to ensure no overflow.
 *
 * @param tree The CLBPT.
 * @param num_keys Specify the number of input keys.
 * @param l_keys An array of lower bound keys with size of num_keys.
 * @param u_keys An array of upper bound keys with size of num_keys.
 * @param record_list After search instructions complete, this array will be filled of lists consisting of record pointers.
 * @return Error code.
 */
int clbptEnqueueRangeSearches(clbpt_tree tree, int num_keys, CLBPT_KEY_TYPE *l_keys, CLBPT_KEY_TYPE *u_keys, clbpt_pair_group_list pair_group_list);

/**
 * @brief Enqueue multiple insertions instruction.
 * 
 * This function is non-blocking. The key-value pair will store in CLBPT.
 * Note that CLBPT does NOT support one-to-many mapping. Insertions with old keys will update the record.
 *
 * @param tree The CLBPT.
 * @param num_inserts The number of insertions.
 * @param keys An array of keys with size of num_inserts.
 * @param records An array of records with size of num_inserts.
 * @return Error code.
 */
int clbptEnqueueInsertions(clbpt_tree tree, int num_inserts, CLBPT_KEY_TYPE *keys, void **records);

/**
 * @brief Enqueue multiple deletions instruction.
 * 
 * This function is non-blocking. The key specified will be deleted in CLBPT.
 *
 * @param tree The CLBPT.
 * @param num_deletes The number of deletions.
 * @param keys An array of keys with size of num_deletes.
 * @return Error code.
 */
int clbptEnqueueDeletions(clbpt_tree tree, int num_deletes, CLBPT_KEY_TYPE *keys);

/**
 * @brief This function will block the process until all enqueued tasks has finished.
 *
 * @param The CLBPT.
 * @return Error code.
 */
int clbptFinish(clbpt_tree tree);
	
#endif /* __CLBPT_H_INCLUDED */
