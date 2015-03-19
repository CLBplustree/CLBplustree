/**
 * @file The back-end source file
 */
 
#include "clbpt_core.h"
#include "clbpt_type.h"
#include <assert.h>

// Static Global Variables
static cl_context context;
static cl_command_queue queue;
static cl_kernel *kernels;
static cl_kernel kernel;

static clbpt_property property;
static cl_mem property_d;

static clbpt_node *root;

static cl_int err;
static cl_mem wait_buf_d, execute_buf_d, result_buf_d;
static cl_mem execute_buf_d_temp, result_buf_d_temp;

static size_t global_work_size;
static size_t local_work_size = 256;	// get this value when initializing
static uint32_t buf_size = CLBPT_BUF_SIZE;

inline int half_c(int input)
{
	return (input + 1) / 2;
}

inline int half_f(int input)
{
	return input / 2;
}

int search_leaf(int32_t key, void *node_addr);
int range_leaf(int32_t key, int32_t key_upper, void *node_addr);
int insert_leaf(int32_t key, void *node_addr);
int delete_leaf(int32_t key, void *node_addr);

int _clbptSelectFromWaitBuffer(clbpt_tree tree)
{
	unsigned int isEmpty = 1;

	context = tree->platform->context;
	queue = tree->platform->queue;
	kernels = tree->platform->kernels;

	// clmem initialize
	static cl_mem isEmpty_d;

	// clmem allocation
	wait_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(clbpt_packet), tree->wait_buf, &err);
	execute_buf_d = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	result_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(void *), (void *)tree->execute_result_buf, &err);
	execute_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	result_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(void *), NULL, &err);
	isEmpty_d = clCreateBuffer(context, 0, sizeof(uint8_t), NULL, &err);

	// kernel _clbptPacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	err = clSetKernelArg(kernel, 0, sizeof(isEmpty_d), (void *)&isEmpty_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 1, sizeof(wait_buf_d), (void *)&wait_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 2, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 3, sizeof(buf_size), (void *)&buf_size);
	assert(err == 0);

	// kernel _clbptPacketSort
	kernel = kernels[CLBPT_PACKET_SORT];
	err = clSetKernelArg(kernel, 0, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 1, sizeof(result_buf_d), (void *)&result_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 2, sizeof(execute_buf_d_temp), (void *)&execute_buf_d_temp);
	assert(err == 0);
	err = clSetKernelArg(kernel, 3, sizeof(result_buf_d_temp), (void *)&result_buf_d_temp);
	assert(err == 0);
	err = clSetKernelArg(kernel, 4, sizeof(buf_size), (void *)&buf_size);
	assert(err == 0);

	// PacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	isEmpty = 1;
	global_work_size = buf_size;
	err = clEnqueueWriteBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == 0);
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	err = clEnqueueReadBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == 0);
	if (isEmpty)
	{
		tree->buf_status = CLBPT_STATUS_DONE;
		return isEmpty;
	}

	// PacketSort
	kernel = kernels[CLBPT_PACKET_SORT];
	global_work_size = 256;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	tree->execute_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, execute_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	assert(err == 0);
	tree->result_buf = (void **)clEnqueueMapBuffer(queue, result_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(void *), 0, NULL, NULL, &err);
	assert(err == 0);
	tree->buf_status = CLBPT_STATUS_WAIT;
	return isEmpty;	/* not done */
}

int _clbptHandleExecuteBuffer(clbpt_tree tree)
{
	context = tree->platform->context;
	queue = tree->platform->queue;
	kernels = tree->platform->kernels;

	property = tree->property;

	// clmem initialize

	// clmem allocation
	//property_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, 1 * sizeof(clbpt_property), tree->property, &err);

	// kernel _clbptSearch
	kernel = kernels[CLBPT_SEARCH];
	err = clSetKernelArg(kernel, 0, sizeof(result_buf_d), (void *)&result_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 1, sizeof(property_d), (void *)&property_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 2, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 3, sizeof(buf_size), (void *)&buf_size);
	assert(err == 0);

	// Search
	global_work_size = buf_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	tree->result_buf = (void **)clEnqueueMapBuffer(queue, result_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(void *), 0, NULL, NULL, &err);
	assert(err == 0);

	// handle leaf nodes
	int i, result;
	clbpt_packet pkt;
	int32_t key, key_upper;
	void *node_addr;
	
	for(i = 0; i < buf_size; i++)
	{
		pkt = execute_buf[i];
		key = getKeyFromPacket(pkt);
		node_addr = result_buf[i];

		if (isSearchPacket(pkt))
		{
			result = search_leaf(key, node_addr);
		}
		else if (isRangePacket(pkt))
		{
			key_upper = getUpperKeyFromRangePacket(pkt);
			result = range_leaf(key, key_upper, node_addr);
		}
		else if (isInsertPacket(pkt))
		{
			result = insert_leaf(key, node_addr);
		}
		else if (isDeletePacket(pkt))
		{
			result = delete_leaf(key, node_addr);
		}
	}

	return CLBPT_STATUS_DONE;
}

int _clbptInitialize(clbpt_tree tree)
{
	// create leaf node
	tree->leaf = (clbpt_leaf_node *)malloc(1 * sizeof(clbpt_leaf_node));
	tree->leaf->head = NULL;
	tree->leaf->num_entry = 0;
	tree->leaf->next_node = NULL;

	root = tree->root;
	property = tree->property;
	kernels = tree->platform->kernels;

	// clmem initialize

	// clmem allocation
	property_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, 1 * sizeof(clbpt_property), &tree->property, &err);

	// kernel _clbptInitialize
	kernel = kernels[CLBPT_INITIALIZE];
	
	err = clSetKernelArg(kernel, 0, sizeof(root), (void *)&root);
	assert(err == 0);
	err = clSetKernelArg(kernel, 1, sizeof(property_d), (void *)&property_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 2, sizeof(tree->heap), (void *)&(tree->heap));
	assert(err == 0);

	// Initialize
	global_work_size = 1;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);	
	tree->property = (clbpt_property)clEnqueueMapBuffer(queue, property_d, CL_TRUE, CL_MAP_READ, 0, sizeof(clbpt_property), 0, NULL, NULL, &err);
	assert(err == 0);

	return CLBPT_STATUS_DONE;
}

int _clbptReleaseLeaf(clbpt_tree tree)
{
	return CLBPT_STATUS_DONE;
}

int search_leaf(int32_t key, void *node_addr)
{
	int existed = 0;
	clbpt_leaf_node *node = node_addr;
	clbpt_leaf_entry *entry = node->head;

	if (entry != NULL &&
		*((int32_t *)entry->record_ptr) == key)
	{
		existed = 1;
	}
	else
	{
		while(entry->next != NULL &&
			*((int32_t *)entry->next->record_ptr) < key)
		{
			if (*((int32_t *)entry->next->record_ptr) == key)
			{
				existed = 1;
				break;
			}
			else
			{
				entry = entry->next;
			}
		}
	}

	if (existed)
	{
		printf("SEARCH SUCCESS: record: %d is in the B+ Tree\n", key);
		return 0;
	}
	else
	{
		printf("SEARCH SUCCESS: record: %d is NOT in the B+ Tree\n", key);
		return 1;
	}
}

int range_leaf(int32_t key, int32_t key_upper, void *node_addr)
{
	int i, num_records = 0;
	clbpt_leaf_node *node_temp, *node = node_addr;
	clbpt_leaf_entry *start, *end, *entry = node->head;

	// find start
	while(entry != NULL && *((int32_t *)entry->record_ptr) <= key_upper)
	{
		if (*((int32_t *)entry->record_ptr) >= key)
		{
			start = entry;
			num_records += 1;
			break;
		}
		entry = entry->next;
	}

	while(entry->next != NULL &&
		*((int32_t *)entry->next->record_ptr) <= key_upper)
	{
		num_records += 1;
		entry = entry->next;
	}
	end = entry;

	if (num_records > 0)
	{
		printf("RANGE SUCCESS: records: \n");
		for(entry = start, i = num_records; i > 0; entry = entry->next, i--)
		{
			printf("%d ", *((CLBPT_RECORD_TYPE *)entry->record_ptr));
		}
		printf("are inside the range[%d, %d]", *((CLBPT_RECORD_TYPE *)start->record_ptr), *((CLBPT_RECORD_TYPE *)end->record_ptr));
	}

	return num_records;
}

int insert_leaf(int32_t key, void *node_addr)
{
	int m, existed = 0;
	clbpt_leaf_node *node_temp, *node = node_addr;
	clbpt_leaf_entry *entry_temp, *entry = node->head;

	if (entry != NULL &&
		*((int32_t *)entry->record_ptr) == key)
	{
		existed = 1;
	}
	else
	{
		while(entry->next != NULL &&
			*((int32_t *)entry->next->record_ptr) < key)
		{
			if (*((int32_t *)entry->next->record_ptr) == key)
			{
				existed = 1;
				break;
			}
			else
			{
				entry = entry->next;
			}
		}
	}

	if (!existed)	// Insert
	{
		if (node->num_entry + 1 < CLBPT_ORDER)
		{
			entry_temp = entry->next;
			entry->next = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
			entry->next->record_ptr = (CLBPT_RECORD_TYPE *)malloc(sizeof(CLBPT_RECORD_TYPE));
			*((int32_t *)entry->next->record_ptr) = key;
			entry->next->next = entry_temp;
			node->num_entry += 1;
		}
		else	// Need Split
		{
			node_temp = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
			m = half_f(CLBPT_ORDER);
			node->num_entry = m;
			node_temp->num_entry = CLBPT_ORDER - m;

			entry_temp = node->head;
			while(m-- > 0)
			{
				entry_temp = entry_temp->next;
			}
			node_temp->head = entry_temp;
			node->next_node = node_temp;
			// insert entry_temp to internal node
		}
	}
	else	// No Insert
	{
		printf("INSERT FAILED: record: %d is already in the B+ Tree\n", key);
	}

	return existed;
}

int delete_leaf(int32_t key, void *node_addr)
{
	int m, existed = 0;
	clbpt_leaf_node *node_temp, *node = node_addr;
	clbpt_leaf_entry *entry_temp, *entry = node->head;

	if (entry != NULL &&
		*((int32_t *)entry->record_ptr) == key)
	{
		existed = 1;
	}
	else
	{
		while(entry->next != NULL &&
			*((int32_t *)entry->next->record_ptr) < key)
		{
			if (*((int32_t *)entry->next->record_ptr) == key)
			{
				existed = 1;
				break;
			}
			else
			{
				entry = entry->next;
			}
		}
	}

	if (existed)	// Delete
	{
		entry_temp = entry->next;
		entry = entry->next->next;
		free(entry_temp->record_ptr);
		entry_temp->next = NULL;
		free(entry_temp);
		node->num_entry -= 1;

		if (node->num_entry - 1 < half_c(CLBPT_ORDER))	// Need Borrow or Merge
		{
			if (node->next_node != NULL &&
				node->next_node->num_entry - 1 < half_c(CLBPT_ORDER))	// Merge
			{
				node_temp = node->next_node;
				node->num_entry += node->next_node->num_entry;
				node->next_node = node->next_node->next_node;
				entry_temp = node->next_node->head;
				node_temp->head = NULL;
				node_temp->next_node = NULL;
				free(node_temp);
				// delete entry_temp to internal node 
			}
			else if (node->next_node != NULL)	// Borrow
			{
				entry_temp = node->next_node->head;
				// delete entry_temp to internal node
				entry_temp = node->next_node->head = node->next_node->head->next;
				// insert entry_temp to internal node
				node->num_entry += 1;
				node->next_node->num_entry -= 1;
			}
			else	// Rightmost node nothing to borrow
			{}
		}
	}
	else	// Nothing to Delete
	{
		printf("DELETE FAILED: record: %d was not in the B+ Tree\n", key);
	}

	return !existed;
}