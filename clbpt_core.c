/**
 * @file The back-end source file
 */
 
#include "clbpt_core.h"
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>

// Static Global Variables
static cl_int err;
static size_t cb;

static cl_mem wait_buf_d, execute_buf_d, result_buf_d;
static cl_mem execute_buf_d_temp, result_buf_d_temp;

static cl_mem property_d;

// Insert and Delete Packet (to internal node)
static clbpt_ins_pkt *ins;
static uint32_t num_ins;
static clbpt_del_pkt *del;
static uint32_t num_del;
static void **addr;
static void **leafmirror_addr;

// Size and Order
static size_t global_work_size;
static size_t local_work_size;
static size_t max_local_work_size;		// get this value in _clbptInitialize
static uint32_t order  = CLBPT_ORDER;	// get this value in _clbptInitialize
static uint32_t buf_size = CLBPT_BUF_SIZE;


int handle_node(void *node_addr);
int handle_leftmost_node(clbpt_leaf_node *node);
int search_leaf(int32_t key, void *node_addr, void *result_addr);
int range_leaf(int32_t key, int32_t key_upper, void *node_addr, void *result_addr);
int insert_leaf(int32_t key, void *node_addr);
int delete_leaf(int32_t key, void *node_addr);
//void print_tree(clbpt_tree tree);
void print_leaves(clbpt_leaf_node *leaf);	// function for testing
void _clbptPrintTree(clbpt_property *property);

int _clbptGetDevices(clbpt_platform platform)
{
	cl_uint num_devices;
	cl_context context = platform->context;
	cl_device_id *devices;
	char *devName;
	char *devVer;
	int i;

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
		devName = (char*)malloc(sizeof(char) * cb);
		clGetDeviceInfo(devices[i], CL_DEVICE_NAME, cb, &devName[0], NULL);
		devName[cb] = 0;
		fprintf(stderr, "Device: %s", devName);
		free(devName);
		
		// get device supports version
		clGetDeviceInfo(devices[i], CL_DEVICE_VERSION, 0, NULL, &cb);
		devVer = (char*)malloc(sizeof(char) * cb);
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
	cl_kernel *kernels = (cl_kernel *)malloc(sizeof(cl_kernel) * NUM_KERNELS);
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
	clbpt_int_node	*root = tree->root;	///// NEED TO CHANGE ?
	clbpt_property	property = tree->property;
	cl_device_id	device = tree->platform->devices[0];
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_program		program = tree->platform->program;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	// Allocate SVM memory
	size_t heap_size = sizeof(void*) * 2048;

	// Create a buffer object using the SVM memory for KMA
	fprintf(stderr, "kma create START\n");
	err = kma_create_svm(device, context, queue, program, heap_size, &(tree->heap));
	assert(err == CL_SUCCESS);
	fprintf(stderr, "kma create SUCCESS\n");

	// Create leaf node
	tree->leaf = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
	tree->leaf->head = NULL;
	tree->leaf->num_entry = 0;
	tree->leaf->next_node = NULL;
	tree->leaf->prev_node = NULL;
	tree->leaf->mirror = NULL;
	tree->leaf->parent_key = 0;

	// Initialize root
	root = (void *)tree->leaf;

	// Create node_addr buffer
	tree->node_addr_buf = (void **)malloc(sizeof(void *) * buf_size);

	// Create ins, del pkt buffer
	ins = (clbpt_ins_pkt *)malloc(sizeof(clbpt_ins_pkt) * buf_size/2);
	addr = (void **)malloc(sizeof(void *) * buf_size/2);
	leafmirror_addr = (void **)malloc(sizeof(void *) * buf_size/2);
	num_ins = 0;
	del = (clbpt_del_pkt *)malloc(sizeof(clbpt_del_pkt) * buf_size/2);
	num_del = 0;

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

	// clmem initialize

	// clmem allocation
	property_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, sizeof(clbpt_property), &tree->property, &err);

	// kernel _clbptInitialize
	kernel = kernels[CLBPT_INITIALIZE];
	err = clSetKernelArg(kernel, 0, sizeof(root), (void *)&root);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(property_d), (void *)&property_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArgSVMPointer(kernel, 2, tree->heap);
	//err = clSetKernelArg(kernel, 2, sizeof(tree->heap), (void *)&(tree->heap));
	assert(err == CL_SUCCESS);

	// Initialize
	global_work_size = local_work_size = 1;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, property_d, CL_TRUE, 0, sizeof(clbpt_property), &(tree->property), 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	tree->leaf->mirror = (clbpt_leafmirror *)tree->property.root;

	return CL_SUCCESS;
}

int _clbptSelectFromWaitBuffer(clbpt_tree tree)
{
	int i;
	unsigned int isEmpty = 1;

	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	// Fill end of wait_buf with NOPs
	for(i = tree->wait_buf_index; i < buf_size; i++)
	{
		tree->wait_buf[i] = PACKET_NOP;
	}

	// clmem initialize
	cl_mem isEmpty_d;

	// clmem allocation
	wait_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(clbpt_packet), tree->wait_buf, &err);
	assert(err == CL_SUCCESS);
	execute_buf_d = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	assert(err == CL_SUCCESS);
	result_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(void *), (void *)tree->result_buf, &err);
	assert(err == CL_SUCCESS);
	execute_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	assert(err == CL_SUCCESS);
	result_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(void *), NULL, &err);
	assert(err == CL_SUCCESS);
	isEmpty_d = clCreateBuffer(context, 0, sizeof(uint8_t), NULL, &err);
	assert(err == CL_SUCCESS);

	// kernel _clbptPacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	err = clSetKernelArg(kernel, 0, sizeof(isEmpty_d), (void *)&isEmpty_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(wait_buf_d), (void *)&wait_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(buf_size), (void *)&buf_size);
	assert(err == CL_SUCCESS);

	// kernel _clbptPacketSort
	kernel = kernels[CLBPT_PACKET_SORT];
	err = clSetKernelArg(kernel, 0, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(result_buf_d), (void *)&result_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(execute_buf_d_temp), (void *)&execute_buf_d_temp);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(result_buf_d_temp), (void *)&result_buf_d_temp);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 4, sizeof(buf_size), (void *)&buf_size);
	assert(err == CL_SUCCESS);

	// <DEBUG>
	/*
	fprintf(stderr, "Original\n");
	fprintf(stderr, "_wait_buf_\n");
	for (i = 0; i < buf_size; i++) {
		if (isNopPacket((clbpt_packet)tree->wait_buf[i])) {
			fprintf(stderr, "NOP\n");
			fprintf(stderr, "i = %d\n", i);
		} else if (isSearchPacket((clbpt_packet)tree->wait_buf[i])) {
			fprintf(stderr, "SEARCH\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->wait_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isRangePacket((clbpt_packet)tree->wait_buf[i])) {
			fprintf(stderr, "RANGE\t%4d %4d\t\n", getKeyFromPacket((clbpt_packet)tree->wait_buf[i]), getUpperKeyFromRangePacket((clbpt_packet)tree->wait_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isInsertPacket((clbpt_packet)tree->wait_buf[i])) {
			fprintf(stderr, "INSERT\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->wait_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isDeletePacket((clbpt_packet)tree->wait_buf[i])) {
			fprintf(stderr, "DELETE\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->wait_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		}
	}
	fprintf(stderr, "size = %d\n", i);
	fprintf(stderr, "\n");
	*/
	// </DEBUG>

	// PacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	isEmpty = 1;
	global_work_size = buf_size;
	local_work_size = max_local_work_size;
	err = clEnqueueWriteBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, wait_buf_d, CL_TRUE, 0, buf_size * sizeof(clbpt_packet), tree->wait_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	if (isEmpty)	// Return if the wait_buf is empty
		return isEmpty;

	// <DEBUG>
	/*
	tree->execute_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, execute_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	assert(err == CL_SUCCESS);

	
	fprintf(stderr, "Before sort\n");
	fprintf(stderr, "_Execute_buf_\n");
	for (i = 0; i < buf_size; i++) {
		if (isNopPacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "NOP\n");
			fprintf(stderr, "i = %d\n", i);
		} else if (isSearchPacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "SEARCH\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isRangePacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "RANGE\t%4d %4d\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]), getUpperKeyFromRangePacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isInsertPacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "INSERT\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isDeletePacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "DELETE\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		}
	}
	fprintf(stderr, "size = %d\n", i);
	fprintf(stderr, "\n");
	*/

	// PacketSort
	kernel = kernels[CLBPT_PACKET_SORT];
	global_work_size = local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, execute_buf_d, CL_TRUE, 0, buf_size * sizeof(clbpt_packet),tree->execute_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, result_buf_d, CL_TRUE, 0, buf_size * sizeof(void *), tree->execute_result_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	// <DEBUG>
	/*
	fprintf(stderr, "After sort\n");
	fprintf(stderr, "_Execute_buf_\n");
	for (i = 0; i < buf_size; i++) {
		if (isNopPacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "NOP\n");
			fprintf(stderr, "i = %d\n", i);
			break;
		} else if (isSearchPacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "SEARCH\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isRangePacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "RANGE\t%4d %4d\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]), getUpperKeyFromRangePacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isInsertPacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "INSERT\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		} else if (isDeletePacket((clbpt_packet)tree->execute_buf[i])) {
			fprintf(stderr, "DELETE\t%4d \t\t\n", getKeyFromPacket((clbpt_packet)tree->execute_buf[i]));
			fprintf(stderr, "i = %d\n", i);
		}
	}
	fprintf(stderr, "size = %d\n", i);
	fprintf(stderr, "\n");
	*/

	return isEmpty;
}

int _clbptHandleExecuteBuffer(clbpt_tree tree)
{
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	// clmem initialize

	// clmem allocation

	// Get the number of non-NOP packets in execute buffer
	int num_packets;
	for(num_packets = 0; num_packets < buf_size; num_packets++)
	{
		if (isNopPacket((clbpt_packet)tree->execute_buf[num_packets]))
			break;
	}
	fprintf(stderr, "Host: num_pkt=%d\n", num_packets);

	// kernel _clbptSearch
	kernel = kernels[CLBPT_SEARCH];
	err = clSetKernelArg(kernel, 0, sizeof(result_buf_d), (void *)&result_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(property_d), (void *)&property_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(num_packets), (void *)&num_packets);
	assert(err == CL_SUCCESS);

	// Search
	global_work_size = buf_size;
	local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, result_buf_d, CL_TRUE, 0, buf_size * sizeof(void *), tree->node_addr_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	// Handle leaf nodes
	int i, j, k;
	int *instr_result = (int *)calloc(buf_size, sizeof(int));
	int node_result = 0;
	clbpt_packet pkt;
	int32_t key, key_upper;
	void *node_addr;
	void *leftmost_node_addr = NULL;
	num_ins = 0;
	num_del = 0;

	node_addr = tree->node_addr_buf[0];

	// Scan through the execute buffer
	for(i = 0, j = 0; i < buf_size; i++)
	{
		pkt = tree->execute_buf[i];
		key = getKeyFromPacket(pkt);

		if (isNopPacket(pkt))	// Reach the end of the execute buffer
		{
			if (i == 0)	// Do nothing if the execute buffer is empty
				break;

			node_result = handle_node(node_addr);	// Handle the last operated node

			if (node_result > 0)	// Need to rollback insertion packets to waiting buffer
			{
				//<DEBUG>
				fprintf(stderr, "Insert ROLLBACK\n");
				fprintf(stderr, "node_result = %d\n", node_result);
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
					print_leaves(tree->leaf);
					//</DEBUG>

					if (instr_result[k] == 0)	// Delete the entry if the rollback insertion packet did insert
					{
						delete_leaf(getKeyFromPacket(tree->execute_buf[k]), node_addr);
						fprintf(stderr, "roll back delete\n");
					}
						
					k--;
				}
				node_result = handle_node(node_addr);	// After rollback, re-handle the node
			}
			if (node_result == leftMostNodeBorrowMerge)
			{
				leftmost_node_addr = node_addr;
			}

			//if (leftmost_node_addr != NULL ||
			//	((clbpt_leaf_node *)node_addr)->mirror->parent !=
			//	((clbpt_leaf_node *)leftmost_node_addr)->mirror->parent)	// node's parent is different with leftmost node's, handle leftmost node
			//{
			//	handle_leftmost_node(leftmost_node_addr);
			//	leftmost_node_addr = NULL;
			//}

			break;
		}

		if (node_addr != tree->node_addr_buf[i])	// Accessed node changed
		{
			node_result = handle_node(node_addr);	// Handle the previous node

			if (node_result > 0)	// Need to rollback insertion packets to waiting buffer
			{
				//<DEBUG>
				fprintf(stderr, "Insert ROLLBACK\n");
				fprintf(stderr, "node_result = %d\n", node_result);
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
					print_leaves(tree->leaf);
					//</DEBUG>

					if (instr_result[k] == 0)	// Delete the entry if the rollback insertion packet did insert
						delete_leaf(getKeyFromPacket(tree->execute_buf[k]), node_addr);
					k--;
				}
				node_result = handle_node(node_addr);	// After rollback, re-handle the node
			}
			if (node_result == leftMostNodeBorrowMerge)
			{
				leftmost_node_addr = node_addr;
			}

			if (leftmost_node_addr != NULL ||
				((clbpt_leaf_node *)node_addr)->mirror->parent !=
				((clbpt_leaf_node *)leftmost_node_addr)->mirror->parent)	// node's parent is different with leftmost node's, handle leftmost node
			{
				handle_leftmost_node(leftmost_node_addr);
				leftmost_node_addr = NULL;
			}

			node_addr = tree->node_addr_buf[i];
		}

		if (isSearchPacket(pkt))
		{
			//<DEBUG>
			fprintf(stderr, "search goes to node with head = %d\n", *(int32_t *)(((clbpt_leaf_node *)node_addr)->head->record_ptr));
			//</DEBUG>
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
				fprintf(stderr, "insert to node with head = %d\n", *(int32_t *)(((clbpt_leaf_node *)node_addr)->head->record_ptr));
			//</DEBUG>
			instr_result[i] = insert_leaf(key, node_addr);
			//<DEBUG>
			fprintf(stderr, "After insert\n");
			print_leaves(tree->leaf);
			//</DEBUG>
		}
		else if (isDeletePacket(pkt))
		{
			instr_result[i] = delete_leaf(key, node_addr);
			//<DEBUG>
			fprintf(stderr, "After delete\n");
			print_leaves(tree->leaf);
			//</DEBUG>
		}
	}

	//<DEBUG>
	fprintf(stderr, "num of pkts in buf = %d\n", i);
	print_leaves(tree->leaf);
	//</DEBUG>


	if (num_ins == 0 && num_del == 0)
	{
		return CL_SUCCESS;
	}

	// clmem initialize
	static cl_mem ins_d;
	static cl_mem del_d;
	static cl_mem addr_d;
	static cl_mem leafmirror_addr_d;

	// clmem allocation
	if (num_ins > 0)
	{
		ins_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_ins * sizeof(clbpt_ins_pkt), ins, &err);
		assert(err == CL_SUCCESS);
		addr_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_ins * sizeof(void *), (void *)addr, &err);
		assert(err == CL_SUCCESS);
		leafmirror_addr_d = clCreateBuffer(context, 0, num_ins * sizeof(void *), NULL, &err);
		assert(err == CL_SUCCESS);

		err = clEnqueueReadBuffer(queue, ins_d, CL_TRUE, 0, num_ins * sizeof(clbpt_ins_pkt), ins, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		fprintf(stderr, "insert packets to internal node:\n");
		for(i = 0; i < num_ins; i++)
		{
			fprintf(stderr, "insert %d, target: %p\n", ins[i].entry.key, ins[i].target);
		}
	}
	if (num_del > 0)
	{
		del_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_del * sizeof(clbpt_del_pkt), del, &err);
		assert(err == CL_SUCCESS);

		err = clEnqueueReadBuffer(queue, del_d, CL_TRUE, 0, num_del * sizeof(clbpt_del_pkt), del, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		fprintf(stderr, "delete packets to internal node:\n");
		for(i = 0; i < num_del; i++)
		{
			fprintf(stderr, "delete %d, target: %p\n", del[i].key, del[i].target);
		}
	}

	// kernel _clbptWPacketInit
	kernel = kernels[CLBPT_WPACKET_INIT];
	err = clSetKernelArg(kernel, 0, sizeof(ins_d), (void *)&ins_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(addr_d), (void *)&addr_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(leafmirror_addr_d), (void *)&leafmirror_addr_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(num_ins), (void *)&num_ins);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 4, sizeof(del_d), (void *)&del_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 5, sizeof(num_del), (void *)&num_del);
	assert(err == CL_SUCCESS);
	err = clSetKernelArgSVMPointer(kernel, 6, tree->heap);
	//err = clSetKernelArg(kernel, 6, sizeof(tree->heap), (void *)&(tree->heap));
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 7, sizeof(property_d), (void *)&property_d);
	assert(err == CL_SUCCESS);

	// WPacketInit
	global_work_size = num_ins > num_del ? num_ins : num_del;
	local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	fprintf(stderr, "enqueue wpacket init SUCCESS\n");
	err = clEnqueueReadBuffer(queue, property_d, CL_TRUE, 0, sizeof(clbpt_property), &(tree->property), 0, NULL, NULL);

	// assign leafmirror_addr to node_sibling's parent
	if (num_ins > 0)
	{
		fprintf(stderr, "leafmirror with num_ins = %d\n", num_ins);
		err = clEnqueueReadBuffer(queue, leafmirror_addr_d, CL_TRUE, 0, num_ins * sizeof(void *), leafmirror_addr, 0, NULL, NULL);
		assert(err == CL_SUCCESS);

		for(i = 0; i < num_ins; i++)
		{
			((clbpt_leaf_node *)addr[i])->mirror = leafmirror_addr[i];
			fprintf(stderr, "%p ", leafmirror_addr[i]);
		}
		fprintf(stderr, "\n");
	}

	fprintf(stderr, "leafmirror_addr assign SUCCESS\n");

	_clbptPrintTree(&tree->property);

	return CL_SUCCESS;
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

	// free ins_pkt, del_pkt, node_addr buffers
	free(ins);
	free(del);
	free(addr);
	free(leafmirror_addr);
	free(tree->node_addr_buf);
	clSVMFree(tree->platform->context, tree->heap);

	return CL_SUCCESS;
}

int handle_node(void *node_addr)
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
		node_sibling->parent_key = *((int32_t *)entry_head->record_ptr);
		node->next_node = node_sibling;

		// After splitting, insert parent_key to internal node
		clbpt_entry entry_d;
		entry_d.key = node_sibling->parent_key;
		entry_d.child = NULL;
		ins[num_ins].target = node->mirror;
		ins[num_ins].entry = entry_d;
		addr[num_ins] = (void *)node_sibling;
		fprintf(stderr, "insert to internal with head = %d\n", *((int32_t *)node_sibling->head->record_ptr));
		num_ins++;
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
				del[num_del].target = node_sibling->mirror;
				del[num_del].key = node_sibling->parent_key;
				num_del++;

				node_sibling->head = NULL;
				node_sibling->num_entry = 0;
				node_sibling->next_node = NULL;
				node_sibling->prev_node = NULL;
				node_sibling->mirror = NULL;
				node_sibling->parent_key = 0;
				free(node_sibling);
			}
			else	// Borrow (from left sibling)
			{
				node_sibling = node->prev_node;

				fprintf(stderr, "node with head %d\n", *(int32_t *)node->head->record_ptr);

				// Delete old parent_key to internal node
				del[num_del].target = node->mirror;
				del[num_del].key = node->parent_key;
				num_del++;

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
				node->parent_key = *((int32_t *)entry_head->record_ptr);

				// Insert new parent_key to internal node
				clbpt_entry entry_d;
				entry_d.key = node->parent_key;
				entry_d.child = NULL;
				ins[num_ins].target = node->mirror;
				ins[num_ins].entry = entry_d;
				addr[num_ins] = (void *)node;
				num_ins++;
			}
		}
		else	// Leftmost node borrows/merges from/with right sibling later
		{
			return leftMostNodeBorrowMerge;
		}
	}

	return 0;
}

int handle_leftmost_node(clbpt_leaf_node *node)
{
	int m, parent_key_update = 0;
	clbpt_leaf_node *node_sibling;
	clbpt_leaf_entry *entry_head;

	fprintf(stderr, "handle leftmost_node START with num_entry = %d", node->num_entry);
	if (node->num_entry >= half_f(order)) return 0;
	if (node->next_node != NULL)
	{
		if (node->num_entry + node->next_node->num_entry < order)	// Merge (with right sibling)
		{
			node_sibling = node->next_node;
			node->num_entry += node_sibling->num_entry;
			node->next_node = node_sibling->next_node;
			if (node_sibling->next_node != NULL)
			{
				node_sibling->next_node->prev_node = node;
			}

			// Delete parent_key to internal node
			del[num_del].target = node_sibling->mirror;
			del[num_del].key = node_sibling->parent_key;
			num_del++;

			node_sibling->head = NULL;
			node_sibling->num_entry = 0;
			node_sibling->next_node = NULL;
			node_sibling->prev_node = NULL;
			node_sibling->mirror = NULL;
			node_sibling->parent_key = 0;
			free(node_sibling);
		}
		else	// Borrow (from right sibling)
		{
			node_sibling = node->next_node;
			entry_head = node_sibling->head;
			if (*((int32_t *)entry_head->record_ptr) == node_sibling->parent_key)
			{
				parent_key_update = 1;

				// Delete old parent_key to internal node
				del[num_del].target = node_sibling->mirror;
				del[num_del].key = node_sibling->parent_key;
				num_del++;
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
				node_sibling->parent_key = *((int32_t *)entry_head->record_ptr);

				// Insert new parent_key to internal node
				clbpt_entry entry_d;
				entry_d.key = node_sibling->parent_key;
				entry_d.child = NULL;
				ins[num_ins].target = node_sibling->mirror;
				ins[num_ins].entry = entry_d;
				addr[num_ins] = (void *)node_sibling;
				num_ins++;
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
		if (*((int32_t *)entry->next->record_ptr) == key)
		{
			existed = 1;
			result_addr = entry->next->record_ptr;
			break;
		}
		if (*((int32_t *)entry->next->record_ptr) > key)
		{
			result_addr = NULL;
			break;
		}
		entry = entry->next;
	}

	free(entry_free);

	if (existed)
	{
		fprintf(stderr, "FOUND: record: %d is in the B+ Tree\n", key);
		num_records_found = 1;
	}
	else
	{
		fprintf(stderr, "NOT FOUND: record: %d is NOT in the B+ Tree\n", key);
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
		result_addr = (void *)malloc(sizeof(void *) * (num_records+1));
		((int *)result_addr)[0] = num_records;
		fprintf(stderr, "RANGE FOUND: records: \n");
		for(entry = start, i = num_records, j = 1; i > 0; entry = entry->next, i--, j++)
		{
			((void **)result_addr)[j] = entry->record_ptr;
			fprintf(stderr, "%d\n", *((CLBPT_RECORD_TYPE *)entry->record_ptr));
		}
		fprintf(stderr, "are inside the range[%d, %d]\n", *((CLBPT_RECORD_TYPE *)start->record_ptr), *((CLBPT_RECORD_TYPE *)end->record_ptr));
	}
	else
	{
		result_addr = NULL;
		fprintf(stderr, "RANGE NOT FOUND: nothing is inside the range[%d, %d]\n", *((CLBPT_RECORD_TYPE *)start->record_ptr), *((CLBPT_RECORD_TYPE *)end->record_ptr));
	}

	return num_records;
}

int insert_leaf(int32_t key, void *node_addr)
{
	int existed = 0;
	clbpt_leaf_node *node;
	clbpt_leaf_entry *entry_temp, *entry, *entry_free;

	node = node_addr;
	entry = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
	entry->next = node->head;
	entry_free = entry;

	// Scan through entries
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
		node->num_entry++;
	}
	else	// No Insert
	{
		fprintf(stderr, "INSERT FAILED: record: %d is already in the B+ Tree\n", key);
	}

	free(entry_free);

	return existed;
}

int delete_leaf(int32_t key, void *node_addr)
{
	int existed = 0;
	clbpt_leaf_node *node = node_addr;
	clbpt_leaf_entry *entry_temp, *entry, *entry_free;

	entry = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
	entry->next = node->head;
	entry_free = entry;

	// Scan through entries
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
	}
	else	// Nothing to Delete
	{
		fprintf(stderr, "DELETE FAILED: record: %d was not in the B+ Tree\n", key);
	}

	free(entry_free);

	return !existed;
}

int _clbptDisplayTree(clbpt_tree tree)
{
	return CL_SUCCESS;
}

/*
void print_tree(clbpt_tree tree)	// function for testing
{
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	// kernel _clbptPrintTreeKernelWrapper
	kernel = kernels[CLBPT_PRINT_TREE_KERNEL_WRAPPER];
	err = clSetKernelArg(kernel, 0, sizeof(property_d), (void *)&property_d);
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
		fprintf(stderr, "|%d", *((int *)entry->record_ptr));
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


void print_leaves(clbpt_leaf_node *leaf)	// function for testing
{
	int count;
	clbpt_leaf_node *temp = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
	temp->next_node = leaf;
	clbpt_leaf_entry *entry;

	if (temp->next_node->head == NULL)
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
			fprintf(stderr, "|%d", *((int *)entry->record_ptr));
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
