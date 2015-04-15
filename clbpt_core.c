/**
 * @file The back-end source file
 */
 
#include "clbpt_core.h"
#include <stdio.h>
#include <assert.h>

// Static Global Variables
static cl_context context;
static cl_command_queue queue;
static cl_kernel *kernels;
static cl_kernel kernel;
static char kernels_name[NUM_KERNELS][35] = {
	"_clbptPacketSelect",
	"_clbptPacketSort",
	"_clbptInitialize",
	"_clbptSearch",
	"_clbptWPacketInit",
	"_clbptWPacketBufferHandler",
	"_clbptWPacketBufferRootHandler",
	"_clbptWPacketCompact",
	"_clbptWPacketSuperGroupHandler"
};

static clbpt_ins_pkt *ins;
static uint32_t num_ins;
static clbpt_del_pkt *del;
static uint32_t num_del;
static void **addr;

static clbpt_property property;
static cl_mem property_d;

static clbpt_int_node *root;

static cl_int err;
static size_t cb;
static cl_mem wait_buf_d, execute_buf_d, result_buf_d;
static cl_mem execute_buf_d_temp, result_buf_d_temp;

static size_t global_work_size;
static size_t local_work_size = 256;	// get this value in _clbptInitialize
static uint32_t buf_size = CLBPT_BUF_SIZE;

int handle_node(void *node_addr);
int haldle_leftmost_node(clbpt_leaf_node *node);
int search_leaf(int32_t key, void *node_addr);
int range_leaf(int32_t key, int32_t key_upper, void *node_addr);
int insert_leaf(int32_t key, void *node_addr);
int delete_leaf(int32_t key, void *node_addr);
void show_leaf(clbpt_leaf_node *leaf);	// function for testing

int _clbptCreateKernels(clbpt_platform platform)
{
	int i;

	platform->kernels = (cl_kernel *)malloc(sizeof(cl_kernel) * NUM_KERNELS);
	for(i = 0; i < NUM_KERNELS; i++)
	{
		kernels[i] = clCreateKernel(platform->program, kernels_name[i], &err);
		if(err != CL_SUCCESS)
		{
			printf("kernel error %d\n",err);
			return err;
		}
	}
	return CL_SUCCESS;
}

int _clbptInitialize(clbpt_tree tree)
{
	root = tree->root;
	property = tree->property;
	kernels = tree->platform->kernels;

	// create leaf node
	tree->leaf = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
	tree->leaf->head = NULL;
	tree->leaf->num_entry = 0;
	tree->leaf->next_node = NULL;
	tree->leaf->prev_node = NULL;
	tree->leaf->parent = NULL;
	tree->leaf->parent_key = 0;

	// initialize root
	root = (void *)tree->leaf;

	// create node_addr buffer
	tree->node_addr_buf = (void **)malloc(sizeof(void *) * buf_size);

	// create ins, del pkt buffer
	ins = (clbpt_ins_pkt *)malloc(sizeof(clbpt_ins_pkt) * buf_size/2);
	addr = (void **)malloc(sizeof(void *) * buf_size/2);
	num_ins = 0;
	del = (clbpt_del_pkt *)malloc(sizeof(clbpt_del_pkt) * buf_size/2);
	num_del = 0;

	// get CL_DEVICE_MAX_WORK_ITEM_SIZES	
	size_t max_work_item_sizes[3];
	err = clGetDeviceInfo(tree->platform->devices[0], CL_DEVICE_MAX_WORK_ITEM_SIZES, 0, NULL, &cb);
	assert(err == 0);
	err = clGetDeviceInfo(tree->platform->devices[0], CL_DEVICE_MAX_WORK_ITEM_SIZES, cb, &max_work_item_sizes[0], NULL);
	assert(err == 0);
	local_work_size = max_work_item_sizes[0];	// one dimension

	// clmem initialize

	// clmem allocation
	property_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, sizeof(clbpt_property), &tree->property, &err);

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
	//tree->property = (clbpt_property)clEnqueueMapBuffer(queue, property_d, CL_TRUE, CL_MAP_READ, 0, sizeof(clbpt_property), 0, NULL, NULL, &err);
	//assert(err == 0);

	return CLBPT_STATUS_DONE;
}

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
	tree->wait_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, wait_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	assert(err == 0);

	// PacketSort
	kernel = kernels[CLBPT_PACKET_SORT];
	global_work_size = local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	tree->execute_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, execute_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	assert(err == 0);
	tree->execute_result_buf = (void **)clEnqueueMapBuffer(queue, result_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(void *), 0, NULL, NULL, &err);
	assert(err == 0);

	tree->buf_status = CLBPT_STATUS_WAIT;
	return isEmpty;	/* not done */
}

int _clbptHandleExecuteBuffer(clbpt_tree tree)
{
	context = tree->platform->context;
	queue = tree->platform->queue;
	kernels = tree->platform->kernels;
	//property = tree->property;

	// clmem initialize

	// clmem allocation
	//property_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, sizeof(clbpt_property), tree->property, &err);

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
	tree->node_addr_buf = (void **)clEnqueueMapBuffer(queue, result_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(void *), 0, NULL, NULL, &err);
	assert(err == 0);

	// handle leaf nodes
	int i, j, k
	int result, node_result;
	clbpt_packet pkt;
	int32_t key, key_upper;
	void *node_addr = NULL;

	for(i = 0, j = 0; i < buf_size; i++)
	{
		pkt = tree->execute_buf[i];
		key = getKeyFromPacket(pkt);
		if (node_addr != tree->node_addr_buf[i])
		{
			node_result = handle_node(node_addr);
			
			if (node_result > 0)	// insertion pkts rollback to waiting buffer
			{
				k = i-1;
				while(node_result > 0)
				{
					for(; k >= 0; k--)
					{
						if (isInsertPacket(tree->execute_buf[k]))
						{
							while(wait_buf[j] != PACKET_NOP) j++;
							wait_buf[j] = tree->execute_buf[k];
							delete_leaf(getKeyFromPacket(tree->execute_buf[k]), node_addr);
							node_result--;
							break;
						}
					}
				}
				node_result = handle_node(node_addr);
			}

			node_addr = tree->node_addr_buf[i];
		}

		if (isNopPacket(pkt))
		{
			break;
		}
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
	if (node_result == leftMostNodeBorrowMerge)
	{
		haldle_leftmost_node(tree->leaf);
	}

	// clmem initialize
	static cl_mem ins_d;
	static cl_mem del_d;
	static cl_mem addr_d;

	// clmem allocation
	ins_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_ins * sizeof(clbpt_ins_pkt), ins, &err);
	assert(err == 0);
	addr_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_ins * sizeof(void *), (void *)addr, &err);
	assert(err == 0);
	del_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_del * sizeof(clbpt_del_pkt), del, &err);
	assert(err == 0);

	// kernel _clbptWPacketInit
	kernel = kernels[CLBPT_WPACKET_INIT];
	err = clSetKernelArg(kernel, 0, sizeof(ins_d), (void *)&ins_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 1, sizeof(addr_d), (void *)&addr_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 2, sizeof(num_ins), (void *)&num_ins);
	assert(err == 0);
	err = clSetKernelArg(kernel, 3, sizeof(del_d), (void *)&del_d);
	assert(err == 0);
	err = clSetKernelArg(kernel, 4, sizeof(num_del), (void *)&num_del);
	assert(err == 0);
	err = clSetKernelArg(kernel, 5, sizeof(tree->heap), (void *)&(tree->heap));
	assert(err == 0);
	err = clSetKernelArg(kernel, 6, sizeof(property_d), (void *)&property_d);
	assert(err == 0);

	// WPacketInit
	global_work_size = num_ins > num_del ? num_ins : num_del;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);	

	return CLBPT_STATUS_DONE;
}

int _clbptReleaseLeaf(clbpt_tree tree)
{
	clbpt_leaf_node *leaf = tree->leaf;
	clbpt_leaf_node *node;
	clbpt_leaf_entry *entry;

	// free all entries
	while(leaf->head != NULL)
	{
		entry = leaf->head->next;
		free(leaf->head->record_ptr);
		leaf->head->next = NULL;
		leaf->head = entry;
	}
	
	// free all nodes
	while(leaf != NULL)
	{
		node = leaf->next_node;
		leaf->head = NULL;
		leaf->num_entry = 0;
		leaf->next_node = NULL;
		free(leaf);
		leaf = node;
	}

	// free ins, del pkt buffer
	free(ins);
	free(del);

	return CLBPT_STATUS_DONE;
}

int handle_node(void *node_addr)
{
	int m;
	clbpt_leaf_node *node_temp, *node = node_addr;
	clbpt_leaf_entry *entry_temp;

	if (node->num_entry >= CLBPT_ORDER)	// Need Split
	{
		if (node->num_entry > 2*(CLBPT_ORDER-1))
		{
			// insertion pkts rollback to waiting buffer
			return node->num_entry - 2*(CLBPT_ORDER-1);
		}
		node_temp = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
		m = half_f(node->num_entry);
		node_temp->num_entry = node->num_entry - m;
		node->num_entry = m;

		entry_temp = node->head;
		while(m-- > 0)
		{
			entry_temp = entry_temp->next;
		}
		node_temp->head = entry_temp;
		node_temp->prev_node = node;
		node_temp->next_node = node->next_node;
		node_temp->parent = node->parent;
		node_temp->parent_key = *((int32_t *)entry_temp->record_ptr);
		node->next_node = node_temp;

		// insert entry_temp to internal node
		clbpt_entry entry_d;
		entry_d.key = node_temp->parent_key;
		entry_d.child = NULL;
		ins[num_ins].target = node->parent;
		ins[num_ins].entry = entry_d;
		addr[num_ins] = (void *)node_temp;
		num_ins++;
	}
	else if (node->num_entry < half_f(CLBPT_ORDER))	// Need Borrow or Merge
	{
		if (node->prev_node != NULL)
		{
			//if (node->prev_node->num_entry - 1 < half_f(CLBPT_ORDER))	// Merge
			if (node->num_entry + node->prev_node->num_entry < CLBPT_ORDER)	// Merge
			{
				node = node->prev_node;
				node_temp = node->next_node;
				node->num_entry += node_temp->num_entry;
				node->next_node = node_temp->next_node;
				if (node_temp->next_node != NULL)
				{
					node_temp->next_node->prev_node = node;
				}

				// delete entry_temp to internal node
				del[num_del].target = node_temp->parent;
				del[num_del].key = node_temp->parent_key;
				num_del++;

				node_temp->head = NULL;
				node_temp->num_entry = 0;
				node_temp->next_node = NULL;
				node_temp->prev_node = NULL;
				node_temp->parent = NULL;
				node_temp->parent_key = 0;
				free(node_temp);
			}
			else	// Borrow
			{
				node_temp = node->prev_node;
				// delete old parent_key to internal node
				del[num_del].target = node->parent;
				del[num_del].key = node->parent_key;
				num_del++;

				m = half_f(node_temp->num_entry + node->num_entry);
				node->num_entry = node_temp->num_entry + node->num_entry - m;
				node_temp->num_entry = m;
				entry_temp = node_temp->head;
				while(m-- > 0)
				{
					entry_temp = entry_temp->next;
				}
				node->head = entry_temp;
				node->parent_key = *((int32_t *)entry_temp->record_ptr);
				// insert new parent_key to internal node
				clbpt_entry entry_d;
				entry_d.key = node->parent_key;
				entry_d.child = NULL;
				ins[num_ins].target = node->parent;
				ins[num_ins].entry = entry_d;
				addr[num_ins] = (void *)node;
				num_ins++;
			}
		}
		else	// Leftmost node borrows/merges from/with right sibling later
		{
			return leftMostNodeBorrow;
		}
		/*
		if (node->next_node != NULL &&
			node->next_node->num_entry - 1 < half_c(CLBPT_ORDER))	// Merge
		{
			node_temp = node->next_node;
			node->num_entry += node->next_node->num_entry;
			node->next_node = node_temp->next_node;

			// delete entry_temp to internal node
			del[num_del].target = node_temp->parent;
			del[num_del].key = *((int32_t *)node_temp->head->record_ptr);
			num_del++;

			node_temp->head = NULL;
			node_temp->next_node = NULL;
			free(node_temp);
		}
		else if (node->next_node != NULL)	// Borrow
		{
			entry_temp = node->next_node->head;
			// delete entry_temp to internal node
			del[num_del].target = node->next_node->parent;
			del[num_del].key = *((int32_t *)entry_temp->record_ptr);
			num_del++;

			entry_temp = node->next_node->head = node->next_node->head->next;
			// insert entry_temp to internal node
			clbpt_entry entry_d;
			entry_d.key = *((int32_t *)entry_temp->record_ptr);
			entry_d.child = NULL;
			ins[num_ins].target = node->next->parent;
			ins[num_ins].entry = entry_d;
			addr[num_ins] = (void *)node->next;
			num_ins++;
			
			node->num_entry += 1;
			node->next_node->num_entry -= 1;
		}
		else	// Rightmost node nothing to borrow
		{}
		*/
	}
	return 0;
}

int haldle_leftmost_node(clbpt_leaf_node *node)
{
	int m, parent_key_update = 0;
	clbpt_leaf_node *node_temp;
	clbpt_leaf_entry *entry_temp;

	if (node->next_node != NULL)
	{
		//if (node->next_node->num_entry - 1 < half_f(CLBPT_ORDER))	// Merge
		if (node->num_entry + node->next_node->num_entry < CLBPT_ORDER)	// Merge
		{
			node_temp = node->next_node;
			node->num_entry += node_temp->num_entry;
			node->next_node = node_temp->next_node;
			if (node_temp->next_node != NULL)
			{
				node_temp->next_node->prev_node = node;
			}

			// delete parent_key to internal node
			del[num_del].target = node_temp->parent;
			del[num_del].key = node_temp->parent_key;
			num_del++;

			node_temp->head = NULL;
			node_temp->num_entry = 0;
			node_temp->next_node = NULL;
			node_temp->prev_node = NULL;
			node_temp->parent = NULL;
			node_temp->parent_key = 0;
			free(node_temp);
		}
		else	// borrow
		{
			node_temp = node->next_node;
			entry_temp = node_temp->head;
			if (*((int32_t *)entry_temp->record_ptr) == node_temp->parent_key)
			{
				parent_key_update = 1;
				// delete old parent_key to internal node
				del[num_del].target = node_temp->parent;
				del[num_del].key = node_temp->parent_key;
				num_del++;
			}
			m = half_f(node->num_entry + node_temp->num_entry);
			node_temp->num_entry = node->num_entry + node_temp->num_entry - m;
			node->num_entry = m;
			entry_temp = node->head;
			while(m-- > 0)
			{
				entry_temp = entry_temp->next;
			}
			node_temp->head = entry_temp;
			if (parent_key_update)
			{
				node_temp->parent_key = *((int32_t *)entry_temp->record_ptr);
				// insert new parent_key to internal node
				clbpt_entry entry_d;
				entry_d.key = node_temp->parent_key;
				entry_d.child = NULL;
				ins[num_ins].target = node_temp->parent;
				ins[num_ins].entry = entry_d;
				addr[num_ins] = (void *)node_temp;
				num_ins++;
			}
		}
	}
	return 0;
}

int search_leaf(int32_t key, void *node_addr)
{
	int existed = 0;
	clbpt_leaf_node *node = node_addr;
	clbpt_leaf_entry *entry, *entry_free;

	entry = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
	entry->next = node->head;
	entry_free = entry;

	// scan through
	while(entry->next != NULL)
	{
		if (*((int32_t *)entry->next->record_ptr) == key)
		{
			existed = 1;
			break;
		}
		if (*((int32_t *)entry->next->record_ptr) > key)
		{
			break;
		}
		entry = entry->next;
	}

	free(entry_free);

	if (existed)
	{
		printf("FOUND: record: %d is in the B+ Tree\n", key);
		return 0;
	}
	else
	{
		printf("NOT FOUND: record: %d is NOT in the B+ Tree\n", key);
		return 1;
	}
}

int range_leaf(int32_t key, int32_t key_upper, void *node_addr)
{
	int i, num_records = 0;
	clbpt_leaf_node *node = node_addr;
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

	if (entry != NULL)
	{
		while(entry->next != NULL &&
			*((int32_t *)entry->next->record_ptr) <= key_upper)
		{
			num_records += 1;
			entry = entry->next;
		}
		end = entry;
	}

	if (num_records > 0)
	{
		printf("RANGE SUCCESS: records: \n");
		for(entry = start, i = num_records; i > 0; entry = entry->next, i--)
		{
			printf("%d ", *((CLBPT_RECORD_TYPE *)entry->record_ptr));
		}
		printf("are inside the range[%d, %d]\n", *((CLBPT_RECORD_TYPE *)start->record_ptr), *((CLBPT_RECORD_TYPE *)end->record_ptr));
	}

	return num_records;
}

int insert_leaf(int32_t key, void *node_addr)
{
	int m, existed = 0;
	clbpt_leaf_node *node_temp, *node;
	clbpt_leaf_entry *entry_temp, *entry, *entry_free;

	node = node_addr;
	entry = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
	entry->next = node->head;
	entry_free = entry;

	// scan through
	while(entry->next != NULL)
	{
		if (*((int32_t *)entry->next->record_ptr) == key)
		{
			existed = 1;
			break;
		}
		if (*((int32_t *)entry->next->record_ptr) > key)
		{
			break;
		}
		entry = entry->next;
	}

	if (!existed)	// Insert
	{
		entry_temp = entry->next;
		entry->next = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
		entry->next->record_ptr = (CLBPT_RECORD_TYPE *)malloc(sizeof(CLBPT_RECORD_TYPE));
		*((int32_t *)entry->next->record_ptr) = key;
		entry->next->next = entry_temp;
		if (entry_temp == node->head)
		{
			node->head = entry->next;
		}
		node->num_entry++;	//
/*
		if (node->num_entry + 1 < CLBPT_ORDER)	// Not full
		{
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
			node_temp->prev_node = node;
			node_temp->next_node = node->next_node;
			node->next_node = node_temp;

			// insert entry_temp to internal node
			clbpt_entry entry_d;
			entry_d.key = *((int32_t *)entry_temp->record_ptr);
			entry_d.child = NULL;
			ins[num_ins].target = node->parent;
			ins[num_ins].entry = entry_d;
			addr[num_ins] = (void *)node_temp;
			num_ins++;
		}
*/
	}
	else	// No Insert
	{
		printf("INSERT FAILED: record: %d is already in the B+ Tree\n", key);
	}

	free(entry_free);

	return existed;
}

int delete_leaf(int32_t key, void *node_addr)	// not sure is it able to borrow yet
{
	int m, existed = 0;
	clbpt_leaf_node *node_temp, *node = node_addr;
	clbpt_leaf_entry *entry_temp, *entry, *entry_free;

	entry = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
	entry->next = node->head;
	entry_free = entry;

	// scan through
	while(entry->next != NULL)
	{
		if (*((int32_t *)entry->next->record_ptr) == key)
		{
			existed = 1;
			break;
		}
		if (*((int32_t *)entry->next->record_ptr) > key)
		{
			break;
		}
		entry = entry->next;
	}

	if (existed)	// Delete
	{
		entry_temp = entry->next;
		entry->next = entry_temp->next;
		if (entry_temp == node->head)
		{
			node->head = entry->next;
		}
		free(entry_temp->record_ptr);
		entry_temp->next = NULL;
		free(entry_temp);

		node->num_entry--;
/*
		if (node->num_entry < half_c(CLBPT_ORDER))	// Need Borrow or Merge
		{
			if (node->prev_node != NULL &&
				node->prev_node->num_entry - 1 < half_c(CLBPT_ORDER))	// Merge
			{
				node = node->prev_node;
				node_temp = node->next_node;
				node->num_entry += node_temp->num_entry;
				node->next_node = node_temp->next_node;
				if (node_temp->next_node != NULL)
				{
					node_temp->next_node->prev_node = node;
				}

				// delete entry_temp to internal node
				del[num_del].target = node_temp->parent;
				del[num_del].key = *((int32_t *)node_temp->head->record_ptr);
				num_del++;

				node_temp->head = NULL;
				node_temp->num_entry = 0;
				node_temp->next_node = NULL;
				node_temp->prev_node = NULL;
				node_temp->parent = NULL;
				free(node_temp);
			}
			else if (node->prev_node != NULL)	// Borrow
			{
				node = node->prev_node;
				entry_temp = node->next_node->head;
				// delete entry_temp to internal node
				del[num_del].target = node->next_node->parent;
				del[num_del].key = *((int32_t *)entry_temp->record_ptr);
				num_del++;

				entry_temp = node->next_node->head = node->next_node->head->next;
				// insert entry_temp to internal node
				clbpt_entry entry_d;
				entry_d.key = *((int32_t *)entry_temp->record_ptr);
				entry_d.child = NULL;
				ins[num_ins].target = node->next->parent;
				ins[num_ins].entry = entry_d;
				addr[num_ins] = (void *)node->next;
				num_ins++;
				
				node->num_entry += 1;
				node->next_node->num_entry -= 1;
			}
			else	// Leftmost node borrows from right sibling
			{
				leftmost_node_borrow = 1;
			}
			///*
			if (node->next_node != NULL &&
				node->next_node->num_entry - 1 < half_c(CLBPT_ORDER))	// Merge
			{
				node_temp = node->next_node;
				node->num_entry += node->next_node->num_entry;
				node->next_node = node_temp->next_node;

				// delete entry_temp to internal node
				del[num_del].target = node_temp->parent;
				del[num_del].key = *((int32_t *)node_temp->head->record_ptr);
				num_del++;

				node_temp->head = NULL;
				node_temp->next_node = NULL;
				free(node_temp);
			}
			else if (node->next_node != NULL)	// Borrow
			{
				entry_temp = node->next_node->head;
				// delete entry_temp to internal node
				del[num_del].target = node->next_node->parent;
				del[num_del].key = *((int32_t *)entry_temp->record_ptr);
				num_del++;

				entry_temp = node->next_node->head = node->next_node->head->next;
				// insert entry_temp to internal node
				clbpt_entry entry_d;
				entry_d.key = *((int32_t *)entry_temp->record_ptr);
				entry_d.child = NULL;
				ins[num_ins].target = node->next->parent;
				ins[num_ins].entry = entry_d;
				addr[num_ins] = (void *)node->next;
				num_ins++;
				
				node->num_entry += 1;
				node->next_node->num_entry -= 1;
			}
			else	// Rightmost node nothing to borrow
			{}
			//
		}
*/
	}
	else	// Nothing to Delete
	{
		printf("DELETE FAILED: record: %d was not in the B+ Tree\n", key);
	}

	free(entry_free);

	return !existed;
}

void show_leaf(clbpt_leaf_node *leaf)	// function for testing
{
	int count;
	clbpt_leaf_node *temp = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
	temp->next_node = leaf;
	clbpt_leaf_entry *entry;

	if (temp->next_node->head == NULL)
	{
		printf("Leaf is EMPTY\n");
		return;
	}
	while(temp->next_node != NULL)
	{
		count = temp->next_node->num_entry;
		entry = temp->next_node->head;
		while(count-- > 0)
		{
			printf("|%d", *((int *)entry->record_ptr));
			entry = entry->next;
		}
		printf("|   ");
		temp->next_node = temp->next_node->next_node;
	}

	printf("\n");
}