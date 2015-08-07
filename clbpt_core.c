/**
 * @file The back-end source file
 */
 
#include "clbpt_core.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


// Size and Order
static size_t max_local_work_size;		// get this value in _clbptInitialize
static uint32_t order = CLBPT_ORDER;	// get this value in _clbptInitialize


int handle_node(clbpt_tree tree, void *node_addr, void *leftmost_node_addr);
int handle_leftmost_node(clbpt_tree tree, clbpt_leaf_node *node);
int search_leaf(int32_t key, void *node_addr, void *result_addr);
int range_leaf(int32_t key, int32_t key_upper, void *node_addr, void *result_addr);
int insert_leaf(int32_t key, void *node_addr, CLBPT_RECORD_TYPE record, size_t record_size);
int delete_leaf(int32_t key, void *node_addr);

void show_pkt_buf(clbpt_packet *pkt_buf, uint32_t buf_size);	// function for testing
void show_leaves(clbpt_leaf_node *leaf);						// function for testing
void _clbptPrintTree(clbpt_property *property);
//void show_tree(clbpt_tree tree);

int _clbptGetDevices(clbpt_platform platform)
{
	int i;
	size_t cb;
	cl_uint num_devices;
	cl_context context = platform->context;
	cl_device_id *devices;
	char *devName;
	char *devVer;

	// Get a list of devices
	clGetContextInfo(context, CL_CONTEXT_DEVICES, 0, NULL, &cb);
	devices = (cl_device_id *)malloc(sizeof(cl_device_id) * cb);
	platform->devices = devices;
	clGetContextInfo(context, CL_CONTEXT_DEVICES, cb, &devices[0], 0);

	// Get the number of devices
	clGetContextInfo(context, CL_CONTEXT_NUM_DEVICES, 0, NULL, &cb);
	clGetContextInfo(context, CL_CONTEXT_NUM_DEVICES, cb, &num_devices, 0);
	fprintf(stderr, "There are %d device(s) in the context\n", num_devices);
	platform->num_devices = num_devices;

	// Show devices info
	/*
	for(i = 0; i < num_devices; i++)
	{
		// get device name
		clGetDeviceInfo(devices[i], CL_DEVICE_NAME, 0, NULL, &cb);
		devName = (char*)malloc(cb * sizeof(char));
		clGetDeviceInfo(devices[i], CL_DEVICE_NAME, cb, &devName[0], NULL);
		devName[cb] = 0;
		fprintf(stderr, "Device: %s", devName);
		free(devName);
		
		// get device supports version
		clGetDeviceInfo(devices[i], CL_DEVICE_VERSION, 0, NULL, &cb);
		devVer = (char*)malloc(cb * sizeof(char));
		clGetDeviceInfo(devices[i], CL_DEVICE_VERSION, cb, &devVer[0], NULL);
		devVer[cb] = 0;
		fprintf(stderr, " (supports %s)\n", devVer);
		free(devVer);
	}
	*/

	return CL_SUCCESS;
}

int _clbptCreateQueues(clbpt_platform platform)
{
	cl_int err;

	cl_queue_properties queue_device_prop[] = {
		CL_QUEUE_PROPERTIES,
		(cl_command_queue_properties)(CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE | CL_QUEUE_ON_DEVICE | CL_QUEUE_ON_DEVICE_DEFAULT),
		0
	};

	platform->queue = clCreateCommandQueueWithProperties(
		platform->context,
		platform->devices[0],
		NULL,
		&err
	);
	platform->queue_device = clCreateCommandQueueWithProperties(
		platform->context,
		platform->devices[0],
		queue_device_prop,
		&err
	);

	if (err != CL_SUCCESS || platform->queue == 0 || platform->queue_device == 0)
	{
		clReleaseContext(platform->context);
		fprintf(stderr, "Error occurs when creating commandQueues\n");

		return err;
	}
	else
	{
		return CL_SUCCESS;
	}
}

int _clbptCreateKernels(clbpt_platform platform)
{
	int i;
	cl_int err;
	cl_kernel *kernels = (cl_kernel *)malloc(NUM_KERNELS * sizeof(cl_kernel));
	char kernels_name[NUM_KERNELS][35] = {
		"_clbptPacketSelect",
		"_clbptPacketSort",
		"_clbptInitialize",
		"_clbptSearch",
		"_clbptWPacketBufferHandler",
		"_clbptWPacketBufferRootHandler",
		"_clbptWPacketInit",
		"_clbptWPacketCompact",
		"_clbptWPacketSuperGroupHandler"
	};

	for(i = 0; i < NUM_KERNELS; i++)
	{
		kernels[i] = clCreateKernel(platform->program, kernels_name[i], &err);
		if (err != CL_SUCCESS)
		{
			fprintf(stderr, "kernel error %d\n", err);
			return err;
		}
	}
	platform->kernels = kernels;

	return CL_SUCCESS;
}

int _clbptInitialize(clbpt_tree tree)
{
	size_t cb;
	cl_int err;
	cl_device_id	device = tree->platform->devices[0];
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_program		program = tree->platform->program;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	size_t global_work_size;
	size_t local_work_size;

	// Allocate SVM memory for KMA
	fprintf(stderr, "kma create START\n");
	tree->heap_size = 2048 * sizeof(void*);
	err = kma_create_svm(device, context, queue, program, tree->heap_size, &(tree->heap));
	assert(err == CL_SUCCESS);
	fprintf(stderr, "kma create SUCCESS\n");

	// Assign buf_size
	tree->buf_size = CLBPT_BUF_SIZE;

	// Create leaf node
	tree->leaf = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
	tree->leaf->head = NULL;
	tree->leaf->num_entry = 0;
	tree->leaf->next_node = NULL;
	tree->leaf->prev_node = NULL;
	tree->leaf->mirror = NULL;
	tree->leaf->parent_key = 0;

	// Create property
	tree->property = (clbpt_property *)malloc(sizeof(clbpt_property));
	tree->property->root = (void *)tree->leaf;

	// Create node_addr buffer
	tree->node_addr_buf = (void **)malloc(tree->buf_size * sizeof(void *));

	// Create ins, del pkt buffer
	tree->ins = (clbpt_ins_pkt *)malloc(tree->buf_size * sizeof(clbpt_ins_pkt));
	tree->leafnode_addr = (void **)malloc(tree->buf_size * sizeof(void *));
	tree->leafmirror_addr = (void **)malloc(tree->buf_size * sizeof(void *));
	tree->del = (clbpt_del_pkt *)malloc(tree->buf_size * sizeof(clbpt_del_pkt));

	// Get CL_DEVICE_MAX_WORK_ITEM_SIZES	
	size_t max_work_item_sizes[3];
	err = clGetDeviceInfo(device, CL_DEVICE_MAX_WORK_ITEM_SIZES, 0, NULL, &cb);
	assert(err == CL_SUCCESS);
	err = clGetDeviceInfo(device, CL_DEVICE_MAX_WORK_ITEM_SIZES, sizeof(max_work_item_sizes), &max_work_item_sizes[0], NULL);
	assert(err == CL_SUCCESS);
	//fprintf(stderr, "Device Maximum Work Item Sizes = %zu x %zu x %zu\n", max_work_item_sizes[0], max_work_item_sizes[1], max_work_item_sizes[2]);
	//max_local_work_size = max_work_item_sizes[0];	// one dimension
	//order = max_local_work_size/2;
	max_local_work_size = 256;	// one dimension
	order = 4;
	fprintf(stderr, "Tree Order = %d\n", order);

	// clmem allocation
	tree->wait_buf_d = clCreateBuffer(context, 0, tree->buf_size * sizeof(clbpt_packet), NULL, &err);
	assert(err == CL_SUCCESS);
	tree->execute_buf_d = clCreateBuffer(context, 0, tree->buf_size * sizeof(clbpt_packet), NULL, &err);
	assert(err == CL_SUCCESS);
	tree->result_buf_d = clCreateBuffer(context, 0, tree->buf_size * sizeof(void *), NULL, &err);
	assert(err == CL_SUCCESS);
	tree->execute_buf_d_temp = clCreateBuffer(context, 0, tree->buf_size * sizeof(clbpt_packet), NULL, &err);
	assert(err == CL_SUCCESS);
	tree->result_buf_d_temp = clCreateBuffer(context, 0, tree->buf_size * sizeof(void *), NULL, &err);
	assert(err == CL_SUCCESS);
	tree->property_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, sizeof(clbpt_property), tree->property, &err);
	assert(err == CL_SUCCESS);

	// kernel _clbptInitialize
	kernel = kernels[CLBPT_INITIALIZE];
	err = clSetKernelArg(kernel, 0, sizeof(tree->property->root), (void *)&tree->property->root);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(tree->property_d), (void *)&tree->property_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArgSVMPointer(kernel, 2, tree->heap);
	//err = clSetKernelArg(kernel, 2, sizeof(tree->heap), (void *)&(tree->heap));
	assert(err == CL_SUCCESS);

	// Initialize
	global_work_size = local_work_size = 1;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->property_d, CL_TRUE, 0, sizeof(clbpt_property), tree->property, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	tree->leaf->mirror = (clbpt_leafmirror *)tree->property->root;

	return CL_SUCCESS;
}

int _clbptSelectFromWaitBuffer(clbpt_tree tree)
{
	int i;
	uint8_t isEmpty = 1;

	size_t global_work_size;
	size_t local_work_size;

	cl_int err;
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	// Fill end of wait_buf with NOPs
	for(i = tree->wait_buf_index; i < tree->buf_size; i++)
	{
		tree->wait_buf[i] = PACKET_NOP;
	}

	// clmem initialize
	cl_mem isEmpty_d;

	// clmem allocation
	isEmpty_d = clCreateBuffer(context, 0, sizeof(uint8_t), NULL, &err);
	assert(err == CL_SUCCESS);

	err = clEnqueueWriteBuffer(queue, tree->wait_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->wait_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueWriteBuffer(queue, tree->result_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(void *), tree->result_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	// kernel _clbptPacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	err = clSetKernelArg(kernel, 0, sizeof(isEmpty_d), (void *)&isEmpty_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(tree->wait_buf_d), (void *)&tree->wait_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(tree->execute_buf_d), (void *)&tree->execute_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(tree->buf_size), (void *)&tree->buf_size);
	assert(err == CL_SUCCESS);

	// kernel _clbptPacketSort
	kernel = kernels[CLBPT_PACKET_SORT];
	err = clSetKernelArg(kernel, 0, sizeof(tree->execute_buf_d), (void *)&tree->execute_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(tree->result_buf_d), (void *)&tree->result_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(tree->execute_buf_d_temp), (void *)&tree->execute_buf_d_temp);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(tree->result_buf_d_temp), (void *)&tree->result_buf_d_temp);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 4, sizeof(tree->buf_size), (void *)&tree->buf_size);
	assert(err == CL_SUCCESS);

	// PacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	isEmpty = 1;
	global_work_size = tree->buf_size;
	local_work_size = max_local_work_size;
	err = clEnqueueWriteBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->wait_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->wait_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	if (isEmpty)	// Return if the wait_buf is empty
		return isEmpty;

	// PacketSort
	kernel = kernels[CLBPT_PACKET_SORT];
	global_work_size = local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->execute_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->execute_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->result_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(void *), tree->execute_result_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	return isEmpty;
}

int _clbptHandleExecuteBuffer(clbpt_tree tree)
{
	cl_int err;
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	size_t global_work_size;
	size_t local_work_size;

	// clmem initialize

	// clmem allocation

	// Get the number of non-NOP packets in execute buffer
	int num_packets;
	for(num_packets = 0; num_packets < tree->buf_size; num_packets++)
	{
		if (isNopPacket((clbpt_packet)tree->execute_buf[num_packets]))
			break;
	}
	fprintf(stderr, "Host: num_pkt=%d\n", num_packets);

	// kernel _clbptSearch
	kernel = kernels[CLBPT_SEARCH];
	err = clSetKernelArg(kernel, 0, sizeof(tree->result_buf_d), (void *)&tree->result_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(tree->property_d), (void *)&tree->property_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(tree->execute_buf_d), (void *)&tree->execute_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(num_packets), (void *)&num_packets);
	assert(err == CL_SUCCESS);

	// Search
	global_work_size = tree->buf_size;
	local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->result_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(void *), tree->node_addr_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	// Handle leaf nodes
	int i, j, k;
	int *instr_result = (int *)calloc(tree->buf_size, sizeof(int));
	int node_result = 0;
	clbpt_packet pkt;
	int32_t key, key_upper;
	void *node_addr;
	void *leftmost_node_addr = NULL;
	tree->num_ins = 0;
	tree->num_del = 0;

	node_addr = tree->node_addr_buf[0];

	// Scan through the execute buffer
	for(i = 0, j = 0; i < tree->buf_size; i++)
	{
		pkt = tree->execute_buf[i];
		key = getKeyFromPacket(pkt);

		if (isNopPacket(pkt))	// Reach the end of the execute buffer
		{
			if (i == 0)	// Do nothing if the execute buffer is empty
				break;

			fprintf(stderr, "End of packets\n");
			node_result = handle_node(tree, node_addr, leftmost_node_addr);	// Handle the last operated node
			fprintf(stderr, "leaf's entry with key %d\n", tree->leaf->head->key);

			if (node_result > 0)	// Need to rollback insertion packets to waiting buffer
			{
				//<DEBUG>
				fprintf(stderr, "Insert ROLLBACK, # of pkts = %d\n", node_result);
				//</DEBUG>

				k = i-1;
				for(; node_result > 0; node_result--)
				{
					while(!isInsertPacket(tree->execute_buf[k]))	// Search backward for insertion packets in execute buffer
					{
						k--;
					}
					while(tree->wait_buf[j] != PACKET_NOP)	// Search forward for empty space in wait buffer
					{
						j++;
					}
					tree->wait_buf[j] = tree->execute_buf[k];	// Rollback the insertion packet to wait buffer
					//<DEBUG>
					fprintf(stderr, "before delete\n");
					show_leaves(tree->leaf);
					//</DEBUG>

					if (instr_result[k] == 0)	// Delete the entry if the rollback insertion packet did insert
					{
						delete_leaf(getKeyFromPacket(tree->execute_buf[k]), node_addr);
						fprintf(stderr, "roll back delete\n");
					}
						
					k--;
				}
				node_result = handle_node(tree, node_addr, leftmost_node_addr);	// After rollback, re-handle the node
			}
			if (node_result == leftMostNodeBorrowMerge)
			{
				fprintf(stderr, "leftMostNodeBorrowMerge\n");
				leftmost_node_addr = node_addr;
			}
			else if (node_result == MergeWithLeftMostNode)
			{
				leftmost_node_addr = NULL;
			}

			if (leftmost_node_addr != NULL &&
				((clbpt_leaf_node *)node_addr)->mirror->parent !=
				((clbpt_leaf_node *)leftmost_node_addr)->mirror->parent)	// node's parent is different with leftmost node's, handle leftmost node
			{
				handle_leftmost_node(tree, leftmost_node_addr);
				fprintf(stderr, "leaf's entry with key %d\n", tree->leaf->head->key);
				leftmost_node_addr = NULL;
			}

			break;
		}

		if (node_addr != tree->node_addr_buf[i])	// Accessed node changed
		{
			node_result = handle_node(tree, node_addr, leftmost_node_addr);	// Handle the previous node

			if (node_result > 0)	// Need to rollback insertion packets to waiting buffer
			{
				//<DEBUG>
				fprintf(stderr, "Insert ROLLBACK, # of pkts = %d\n", node_result);
				//</DEBUG>

				k = i - 1;
				for(; node_result > 0; node_result--)
				{
					while (!isInsertPacket(tree->execute_buf[k]))	// Search backward for insertion packets in execute buffer
					{
						k--;
					}
					while (tree->wait_buf[j] != PACKET_NOP)	// Search forward for empty space in wait buffer
					{
						j++;
					}
					tree->wait_buf[j] = tree->execute_buf[k];	// Rollback the insertion packet to wait buffer
					//<DEBUG>
					fprintf(stderr, "before delete\n");
					show_leaves(tree->leaf);
					//</DEBUG>

					if (instr_result[k] == 0)	// Delete the entry if the rollback insertion packet did insert
						delete_leaf(getKeyFromPacket(tree->execute_buf[k]), node_addr);
					k--;
				}
				node_result = handle_node(tree, node_addr, leftmost_node_addr);	// After rollback, re-handle the node
			}
			if (node_result == leftMostNodeBorrowMerge)
			{
				fprintf(stderr, "leftMostNodeBorrowMerge\n");
				leftmost_node_addr = node_addr;
			}

			if (leftmost_node_addr != NULL &&
				((clbpt_leaf_node *)node_addr)->mirror->parent !=
				((clbpt_leaf_node *)leftmost_node_addr)->mirror->parent)	// node's parent is different with leftmost node's, handle leftmost node
			{
				handle_leftmost_node(tree, leftmost_node_addr);
				fprintf(stderr, "leaf's entry with key %d\n", tree->leaf->head->key);
				leftmost_node_addr = NULL;
			}

			node_addr = tree->node_addr_buf[i];
		}

		if (isSearchPacket(pkt))
		{
			instr_result[i] = search_leaf(key, node_addr, tree->execute_result_buf[i]);
		}
		else if (isRangePacket(pkt))
		{
			key_upper = getUpperKeyFromRangePacket(pkt);
			instr_result[i] = range_leaf(key, key_upper, node_addr, tree->execute_result_buf[i]);
		}
		else if (isInsertPacket(pkt))
		{
			//<DEBUG>
			fprintf(stderr, "insert key = %d\n", key);
			//int temp = *((int32_t *)((clbpt_leaf_node *)node_addr)->head->record_ptr);
			if (((clbpt_leaf_node *)node_addr)->head != NULL)
				fprintf(stderr, "insert to node with head = %d\n", ((clbpt_leaf_node *)node_addr)->head->key);
			//</DEBUG>
			instr_result[i] = insert_leaf(key, node_addr, (CLBPT_RECORD_TYPE)tree->execute_result_buf[i], tree->record_size);
			fprintf(stderr, "leaf's entry with key %d\n", tree->leaf->head->key);
			//<DEBUG>
			fprintf(stderr, "After insert\n");
			show_leaves(tree->leaf);
			//</DEBUG>
		}
		else if (isDeletePacket(pkt))
		{
			fprintf(stderr, "leaf's entry with key %d\n", tree->leaf->head->key);
			instr_result[i] = delete_leaf(key, node_addr);
			//<DEBUG>
			fprintf(stderr, "leaf's entry with key %d\n", tree->leaf->head->key);
			fprintf(stderr, "After delete\n");
			show_leaves(tree->leaf);
			//</DEBUG>
		}
	}

	//<DEBUG>
	fprintf(stderr, "num of pkts in buf = %d\n", i);
	show_leaves(tree->leaf);
	//</DEBUG>


	if (tree->num_ins == 0 && tree->num_del == 0)
	{
		return CL_SUCCESS;
	}

	// clmem initialize
	static cl_mem ins_d;
	static cl_mem del_d;
	static cl_mem leafnode_addr_d;
	static cl_mem leafmirror_addr_d;

	// clmem allocation
	if (tree->num_ins > 0)
	{
		ins_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, tree->num_ins * sizeof(clbpt_ins_pkt), tree->ins, &err);
		assert(err == CL_SUCCESS);
		leafnode_addr_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, tree->num_ins * sizeof(void *), (void *)tree->leafnode_addr, &err);
		assert(err == CL_SUCCESS);
		leafmirror_addr_d = clCreateBuffer(context, 0, tree->num_ins * sizeof(void *), NULL, &err);
		assert(err == CL_SUCCESS);

		//<DEBUG>
		err = clEnqueueReadBuffer(queue, ins_d, CL_TRUE, 0, tree->num_ins * sizeof(clbpt_ins_pkt), tree->ins, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		fprintf(stderr, "insert packets to internal node:\n");
		for(i = 0; i < tree->num_ins; i++)
		{
			fprintf(stderr, "insert %d, target: %p\n", tree->ins[i].entry.key, tree->ins[i].target);
		}
		//</DEBUG>
	}
	if (tree->num_del > 0)
	{
		del_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, tree->num_del * sizeof(clbpt_del_pkt), tree->del, &err);
		assert(err == CL_SUCCESS);

		//<DEBUG>
		err = clEnqueueReadBuffer(queue, del_d, CL_TRUE, 0, tree->num_del * sizeof(clbpt_del_pkt), tree->del, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		fprintf(stderr, "delete packets to internal node:\n");
		for(i = 0; i < tree->num_del; i++)
		{
			fprintf(stderr, "delete %d, target: %p\n", tree->del[i].key, tree->del[i].target);
		}
		//</DEBUG>
	}

	// kernel _clbptWPacketInit
	kernel = kernels[CLBPT_WPACKET_INIT];
	err = clSetKernelArg(kernel, 0, sizeof(ins_d), (void *)&ins_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(leafnode_addr_d), (void *)&leafnode_addr_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(leafmirror_addr_d), (void *)&leafmirror_addr_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(tree->num_ins), (void *)&tree->num_ins);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 4, sizeof(del_d), (void *)&del_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 5, sizeof(tree->num_del), (void *)&tree->num_del);
	assert(err == CL_SUCCESS);
	err = clSetKernelArgSVMPointer(kernel, 6, tree->heap);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 7, sizeof(tree->property_d), (void *)&tree->property_d);
	assert(err == CL_SUCCESS);

	// WPacketInit
	global_work_size = tree->num_ins > tree->num_del ? tree->num_ins : tree->num_del;
	local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	fprintf(stderr, "enqueue wpacket init SUCCESS\n");
	err = clEnqueueReadBuffer(queue, tree->property_d, CL_TRUE, 0, sizeof(clbpt_property), (tree->property), 0, NULL, NULL);

	// Assign leafmirror_addr to node_sibling's parent
	if (tree->num_ins > 0)
	{
		err = clEnqueueReadBuffer(queue, leafmirror_addr_d, CL_TRUE, 0, tree->num_ins * sizeof(void *), tree->leafmirror_addr, 0, NULL, NULL);
		assert(err == CL_SUCCESS);

		for(i = 0; i < tree->num_ins; i++)
		{
			((clbpt_leaf_node *)tree->leafnode_addr[i])->mirror = tree->leafmirror_addr[i];
		}
		//clReleaseMemObject(ins_d);
		//clReleaseMemObject(leafnode_addr_d);
		//clReleaseMemObject(leafmirror_addr_d);
	}

	fprintf(stderr, "leafmirror_addr assign SUCCESS\n");
	fprintf(stderr, "leaf's entry with key %d\n", tree->leaf->head->key);

	_clbptPrintTree(tree->property);

	return CL_SUCCESS;
}

int _clbptReleaseLeaf(clbpt_tree tree)
{
	clbpt_leaf_node *leaf = tree->leaf;
	clbpt_leaf_node *node;
	clbpt_leaf_entry *entry;

	// Free all entries
	while(leaf->head != NULL)
	{
		entry = leaf->head->next;
		free(leaf->head->record_ptr);
		leaf->head->next = NULL;
		leaf->head = entry;
	}

	// Free all nodes
	while(leaf != NULL)
	{
		node = leaf->next_node;
		leaf->head = NULL;
		leaf->num_entry = 0;
		leaf->next_node = NULL;
		free(leaf);
		leaf = node;
	}

	// Free root
	free(tree->property);

	// Free ins_pkt, del_pkt, node_addr buffers
	free(tree->ins);
	free(tree->del);
	free(tree->leafnode_addr);
	free(tree->leafmirror_addr);
	free(tree->node_addr_buf);
	clSVMFree(tree->platform->context, tree->heap);

	return CL_SUCCESS;
}

int handle_node(clbpt_tree tree, void *node_addr, void *leftmost_node_addr)
{
	int m;
	clbpt_leaf_node *node_sibling, *node = node_addr;
	clbpt_leaf_entry *entry_head;

	fprintf(stderr, "handle node START\n");
	if (node->num_entry >= order)	// Need to Split
	{
		if (node->num_entry > 2*(order-1))
		{
			// Insertion pkts rollback to waiting buffer
			return node->num_entry - 2*(order-1);
		}
		node_sibling = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
		m = half_f(node->num_entry);
		printf("num of node : %d\n", node->num_entry);
		node_sibling->num_entry = node->num_entry - m;
		node->num_entry = m;
		printf("split result: %d, %d\n", node->num_entry, node_sibling->num_entry);

		entry_head = node->head;
		for(; m > 0; m--)
		{
			entry_head = entry_head->next;
		}
		node_sibling->head = entry_head;
		node_sibling->prev_node = node;
		node_sibling->next_node = node->next_node;
		node_sibling->mirror = node->mirror;
		node_sibling->parent_key = entry_head->key;
		node->next_node = node_sibling;

		// After splitting, insert parent_key to internal node
		clbpt_entry entry_d;
		entry_d.key = node_sibling->parent_key;
		entry_d.child = NULL;
		tree->ins[num_ins].target = node->mirror;
		tree->ins[num_ins].entry = entry_d;
		tree->leafnode_addr[num_ins] = (void *)node_sibling;
		fprintf(stderr, "insert to internal with head = %d\n", node_sibling->head->key);
		tree->num_ins++;
	}
	else if (node->num_entry < half_f(order))	// Need Borrow or Merge
	{
		if (node->prev_node != NULL && node->prev_node->mirror->parent == node->mirror->parent)
		{
			if (node->num_entry + node->prev_node->num_entry < order)	// Merge (with left sibling)
			{
				fprintf(stderr, "Merging...\n");

				node = node->prev_node;
				node_sibling = node->next_node;
				node->num_entry += node_sibling->num_entry;
				node->next_node = node_sibling->next_node;
				if (node_sibling->next_node != NULL)
				{
					node_sibling->next_node->prev_node = node;
				}

				// Delete parent_key to internal node
				tree->del[num_del].target = node_sibling->mirror;
				tree->del[num_del].key = node_sibling->parent_key;
				tree->num_del++;

				node_sibling->head = NULL;
				node_sibling->num_entry = 0;
				node_sibling->next_node = NULL;
				node_sibling->prev_node = NULL;
				node_sibling->mirror = NULL;
				node_sibling->parent_key = 0;
				free(node_sibling);

				if (node == leftmost_node_addr)
					return MergeWithLeftMostNode;
			}
			else	// Borrow (from left sibling)
			{
				node_sibling = node->prev_node;

				fprintf(stderr, "node with head %d\n", node->head->key);

				// Delete old parent_key to internal node
				tree->del[num_del].target = node->mirror;
				tree->del[num_del].key = node->parent_key;
				tree->num_del++;

				m = half_f(node_sibling->num_entry + node->num_entry);
				node->num_entry = node_sibling->num_entry + node->num_entry - m;
				node_sibling->num_entry = m;
				fprintf(stderr, "left node with %d entries, right node with %d entries\n", node_sibling->num_entry, node->num_entry);
				entry_head = node_sibling->head;
				for(; m > 0; m--)
				{
					entry_head = entry_head->next;
				}
				entry_head->next = node->head;

				node->head = entry_head;	// Update head of the node
				node->parent_key = entry_head->key;

				// Insert new parent_key to internal node
				clbpt_entry entry_d;
				entry_d.key = node->parent_key;
				entry_d.child = NULL;
				tree->ins[num_ins].target = node->mirror;
				tree->ins[num_ins].entry = entry_d;
				tree->leafnode_addr[num_ins] = (void *)node;
				tree->num_ins++;
			}
		}
		else	// Leftmost node borrows/merges from/with right sibling later
		{
			return leftMostNodeBorrowMerge;
		}
	}

	return 0;
}

int handle_leftmost_node(clbpt_tree tree, clbpt_leaf_node *node)
{
	int m, parent_key_update = 0;
	clbpt_leaf_node *node_sibling;
	clbpt_leaf_entry *entry_head;

	fprintf(stderr, "handle leftmost_node START with num_entry = %d\n", node->num_entry);
	if (node->num_entry >= half_f(order)) return 0;
	if (node->next_node != NULL)
	{
		
		if (node->num_entry + node->next_node->num_entry < order)	// Merge (with right sibling)
		{
			fprintf(stderr, "Merge\n");
			node_sibling = node->next_node;
			node->num_entry += node_sibling->num_entry;
			node->next_node = node_sibling->next_node;
			if (node_sibling->next_node != NULL)
			{
				node_sibling->next_node->prev_node = node;
			}

			// Delete parent_key to internal node
			tree->del[num_del].target = node_sibling->mirror;
			tree->del[num_del].key = node_sibling->parent_key;
			tree->num_del++;

			node_sibling->head = NULL;
			node_sibling->num_entry = 0;
			node_sibling->next_node = NULL;
			node_sibling->prev_node = NULL;
			node_sibling->mirror = NULL;
			node_sibling->parent_key = 0;
			free(node_sibling);

			fprintf(stderr, "node with num_entries = %d\n", node->num_entry);
		}
		else	// Borrow (from right sibling)
		{
			node_sibling = node->next_node;
			entry_head = node_sibling->head;
			if (entry_head->key == node_sibling->parent_key)
			{
				parent_key_update = 1;

				// Delete old parent_key to internal node
				tree->del[num_del].target = node_sibling->mirror;
				tree->del[num_del].key = node_sibling->parent_key;
				tree->num_del++;
			}
			m = half_f(node->num_entry + node_sibling->num_entry);
			node_sibling->num_entry = node->num_entry + node_sibling->num_entry - m;
			node->num_entry = m;
			entry_head = node->head;
			for(; m > 0; m--)
			{
				entry_head = entry_head->next;
			}
			entry_head->next = node_sibling->head;

			node_sibling->head = entry_head;
			if (parent_key_update)
			{
				node_sibling->parent_key = entry_head->key;

				// Insert new parent_key to internal node
				clbpt_entry entry_d;
				entry_d.key = node_sibling->parent_key;
				entry_d.child = NULL;
				tree->ins[num_ins].target = node_sibling->mirror;
				tree->ins[num_ins].entry = entry_d;
				tree->leafnode_addr[num_ins] = (void *)node_sibling;
				tree->num_ins++;
			}
		}
	}

	return 0;
}

int search_leaf(int32_t key, void *node_addr, void *result_addr)
{
	int existed = 0;
	int num_records_found;
	clbpt_leaf_node *node = node_addr;
	clbpt_leaf_entry *entry, *entry_free;

	entry = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
	entry->next = node->head;
	entry_free = entry;

	// Scan through entries
	while(entry->next != NULL)
	{
		if (entry->next->key == key)
		{
			existed = 1;
			result_addr = entry->next->record_ptr;
			break;
		}
		if (entry->next->key > key)
		{
			result_addr = NULL;
			break;
		}
		entry = entry->next;
	}

	free(entry_free);

	if (existed)
	{
		fprintf(stderr, "FOUND: (key: %d, record: %d) is in the B+ Tree\n", key, *((CLBPT_RECORD_TYPE *)result_addr));
		num_records_found = 1;
	}
	else
	{
		fprintf(stderr, "NOT FOUND: key: %d is NOT in the B+ Tree\n", key);
		num_records_found = 0;
	}

	return num_records_found;
}

int range_leaf(int32_t key, int32_t key_upper, void *node_addr, void *result_addr)
{
	int i, j, num_records = 0;
	clbpt_leaf_node *node = node_addr;
	clbpt_leaf_entry *start = NULL, *end = NULL, *entry = node->head;

	// find start
	while(entry != NULL && entry->key <= key_upper)
	{
		if (entry->key >= key)
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
			entry->next->key <= key_upper)
			//*((int32_t *)entry->next->record_ptr) <= key_upper)
		{
			num_records += 1;
			entry = entry->next;
		}
		end = entry;
	}

	if (num_records > 0)
	{
		result_addr = (void *)malloc(sizeof(void *) * (num_records+1));
		((int *)result_addr)[0] = num_records;
		fprintf(stderr, "RANGE FOUND:\n");
		for(entry = start, i = num_records, j = 1; i > 0; entry = entry->next, i--, j++)
		{
			((void **)result_addr)[j] = entry->record_ptr;
			fprintf(stderr, "\t(key: %d, record: %d)\n", entry->key, *((CLBPT_RECORD_TYPE *)entry->record_ptr));
		}
		fprintf(stderr, "are inside the range[%d, %d]\n", start->key, end->key);
	}
	else
	{
		result_addr = NULL;
		fprintf(stderr, "RANGE NOT FOUND: nothing is inside the range[%d, %d]\n", start->key, end->key);
	}

	return num_records;
}

int insert_leaf(int32_t key, void *node_addr, CLBPT_RECORD_TYPE record, size_t record_size)
{
	int existed = 0;
	clbpt_leaf_node *node;
	clbpt_leaf_entry *entry_prev, *entry_new, *entry;

	node = node_addr;
	entry_prev = NULL;
	entry = node->head;

	// Scan through entries
	while(entry != NULL)
	{
		if (entry->key == key)
		{
			existed = 1;
			break;
		}
		if (entry->key > key)
		{
			break;
		}
		entry_prev = entry;
		entry = entry->next;
	}

	if (!existed)	// Insert
	{
		entry_new = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
		entry_new->key = key;
		entry_new->record_ptr = (CLBPT_RECORD_TYPE *)malloc(record_size);
		*((CLBPT_RECORD_TYPE *)entry_new->record_ptr) = record;
		entry_new->prev = entry_prev;
		entry_new->next = entry;
		if (node->head == NULL)
		{
			node->head = entry_new;
		}
		if (entry != NULL)
		{
			entry->prev = entry_new;
			if (entry == node->head )	// Insert before head entry
			{
				node->head = entry_new;
			}
		}
		if (entry_prev != NULL)
		{
			entry_prev->next = entry_new;
		}
		node->num_entry++;
	}
	else	// No Insert
	{
		fprintf(stderr, "INSERT FAILED: key: %d is already in the B+ Tree\n", key);
	}

	return existed;
}

int delete_leaf(int32_t key, void *node_addr)
{
	int existed = 0;
	clbpt_leaf_node *node;
	clbpt_leaf_entry *entry_prev, *entry;

	node = node_addr;
	entry = node->head;

	fprintf(stderr, "node_head with key = %d\n", node->head->key);

	// Scan through entries
	while(entry != NULL)
	{
		if (entry->key == key)
		{
			existed = 1;
			break;
		}
		if (entry->key > key)
		{
			break;
		}
		entry = entry->next;
	}

	if (existed)	// Delete
	{
		if (entry == node->head)	// Delete head entry
		{
			node->head = entry->next;
			fprintf(stderr, "node_head with key = %d\n", node->head->key);
		}
		if ((entry_prev = entry->prev) != NULL)
		{
			entry_prev->next = entry->next;
		}
		entry->key = 0;
		free(entry->record_ptr);
		entry->record_ptr = NULL;
		entry->next = NULL;
		free(entry);

		node->num_entry--;
	}
	else	// Nothing to Delete
	{
		fprintf(stderr, "DELETE FAILED: key: %d was not in the B+ Tree\n", key);
	}

	return !existed;
}

int _clbptDisplayTree(clbpt_tree tree)
{
	return CL_SUCCESS;
}

/*
void show_tree(clbpt_tree tree)	// function for testing
{
	cl_int err;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	size_t global_work_size;
	size_t local_work_size;

	// kernel _clbptPrintTreeKernelWrapper
	kernel = kernels[CLBPT_PRINT_TREE_KERNEL_WRAPPER];
	err = clSetKernelArg(kernel, 0, sizeof(tree->property_d), (void *)&tree->property_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArgSVMPointer(kernel, 1, tree->heap);
	assert(err == CL_SUCCESS);

	// PrintTreeKernelWrapper
	global_work_size = local_work_size = 1;
	fprintf(stderr, "Internal node:\n");
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
}
*/

void _clbptPrintLeaf(int level, clbpt_leaf_node *leaf)	// function for testing
{
	int count;
	clbpt_leaf_entry *entry;

	for (int i = 0; i < level; i++) {
		fprintf(stderr, " ");
	}
	fprintf(stderr, "Leaf:%p (mirror=%p) ", leaf, leaf->mirror);
	count = leaf->num_entry;
	entry = leaf->head;
	while (count-- > 0)
	{
		fprintf(stderr, "|(key: %d, rec: %d)", entry->key, *((CLBPT_RECORD_TYPE *)entry->record_ptr));
		entry = entry->next;
	}
	fprintf(stderr, "|   ");
	fprintf(stderr, "\n");

}

void
_clbptPrintMirror(
	int level,
	clbpt_leafmirror *mirror
	)
{
	for (int i = 0; i < level; i++)
		fprintf(stderr, " ");
	fprintf(stderr, "Mirror:%p (parent=%p, leaf=%p)\n",
		(void *)mirror, (void *)mirror->parent, mirror->leaf);
	_clbptPrintLeaf(level + 1, mirror->leaf);
}

void
_clbptPrintNode(
	int level_proc,
	int level_tree,
	clbpt_int_node *node
	)
{
	for (int i = 0; i < level_proc; i++)
		fprintf(stderr, " ");
	fprintf(stderr, "Node:%p (num_entry=%u, parent=%p, parent_key=%d)\n",
		(void *)node, node->num_entry, (void *)node->parent,
		getKey(node->parent_key));
	if (level_proc < level_tree - 2)
	for (int i = 0; i < node->num_entry; i++) {
		for (int j = 0; j < level_proc; j++)
			fprintf(stderr, " ");
		fprintf(stderr, ">Entry #%d (key=%d, child=%p)\n",
			i, getKey(node->entry[i].key), (void *)node->entry[i].child);
		_clbptPrintNode(level_proc + 1, level_tree,
			(clbpt_int_node *)node->entry[i].child);
	}
	else
	for (int i = 0; i < node->num_entry; i++) {
		for (int j = 0; j < level_proc; j++)
			fprintf(stderr, " ");
		fprintf(stderr, ">Entry #%d (key=%d, child=%p)\n",
			i, getKey(node->entry[i].key), (void *)node->entry[i].child);
		_clbptPrintMirror(level_proc + 1,
			(clbpt_leafmirror *)node->entry[i].child);
	}

}

void
_clbptPrintTree(
	clbpt_property *property
	)
{
	int level = property->level;
	uintptr_t root = property->root;

	fprintf(stderr, "### Traversal of GPU-side tree ###\n");
	fprintf(stderr, "Level of tree: %d\n", level);
	if (level <= 1)
		_clbptPrintMirror(0, (clbpt_leafmirror *)root);
	else
		_clbptPrintNode(0, level, (clbpt_int_node *)root);
	fprintf(stderr, "### End of traversal ###\n");
}

void show_pkt_buf(clbpt_packet *pkt_buf, uint32_t buf_size)
{
	int i;

	fprintf(stderr, "===  buf_info  ===\n");
	for (i = 0; i < buf_size; i++)
	{
		if (isNopPacket(pkt_buf[i]))
		{
			fprintf(stderr, "%d\t", i);
			fprintf(stderr, "NOP\n");
		}
		else if (isSearchPacket(pkt_buf[i]))
		{
			fprintf(stderr, "%d\t", i);
			fprintf(stderr, "SEARCH\t%4d \t\t\n", getKeyFromPacket(pkt_buf[i]));
		}
		else if (isRangePacket(pkt_buf[i]))
		{
			fprintf(stderr, "%d\t", i);
			fprintf(stderr, "RANGE\t%4d %4d\t\n", getKeyFromPacket(pkt_buf[i]), getUpperKeyFromRangePacket(pkt_buf[i]));
		}
		else if (isInsertPacket(pkt_buf[i]))
		{
			fprintf(stderr, "%d\t", i);
			fprintf(stderr, "INSERT\t%4d \t\t\n", getKeyFromPacket(pkt_buf[i]));
		}
		else if (isDeletePacket(pkt_buf[i]))
		{
			fprintf(stderr, "%d\t", i);
			fprintf(stderr, "DELETE\t%4d \t\t\n", getKeyFromPacket(pkt_buf[i]));
		}
	}
	fprintf(stderr, "==================\n");
}

void show_leaves(clbpt_leaf_node *leaf)	// function for testing
{
	int count;
	clbpt_leaf_node *temp = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
	temp->next_node = leaf;
	clbpt_leaf_entry *entry;

	if (temp == NULL || leaf == NULL || temp->next_node->head == NULL)
	{
		fprintf(stderr, "Leaf is EMPTY\n");
		return;
	}

	while(1)
	{
		//
		//fprintf(stderr, "\nmirror: %p", temp->next_node->mirror);
		//fprintf(stderr, "\tparent: %p   ", temp->next_node->mirror->parent);
		//		
		for(count = temp->next_node->num_entry, entry = temp->next_node->head;
			count > 0; count--)
		{
			fprintf(stderr, "|(key: %d, rec: %d)", entry->key, *((CLBPT_RECORD_TYPE *)entry->record_ptr));
			entry = entry->next;
		}
		
		if ((temp->next_node = temp->next_node->next_node) != NULL)
		{
			fprintf(stderr, "|---");
		}
		else
		{
			fprintf(stderr, "|");
			break;
		}
	}
	fprintf(stderr, "\n");

	free(temp);
}
