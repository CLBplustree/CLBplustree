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

//static clbpt_property property;
static cl_mem property_d;

// Insert and Delete Packet (to internal node)
static clbpt_ins_pkt *ins;
static uint32_t num_ins;
static clbpt_del_pkt *del;
static uint32_t num_del;
static void **addr;

// Size and Order
static size_t global_work_size;
static size_t local_work_size;
static size_t max_local_work_size;		// get this value in _clbptInitialize
static uint32_t order  = CLBPT_ORDER;	// get this value in _clbptInitialize
static uint32_t buf_size = CLBPT_BUF_SIZE;


int handle_node(void *node_addr);
int haldle_leftmost_node(clbpt_leaf_node *node);
int search_leaf(int32_t key, void *node_addr, void *result_addr);
int range_leaf(int32_t key, int32_t key_upper, void *node_addr, void *result_addr);
int insert_leaf(int32_t key, void *node_addr);
int delete_leaf(int32_t key, void *node_addr);
void show_leaf(clbpt_leaf_node *leaf);	// function for testing

int _clbptGetDevices(clbpt_platform platform)
{
	cl_uint num_devices;
	cl_context context = platform->context;
	cl_device_id *devices;
	char *devName;
	char *devVer;
	int i;

	// get a list of devices
	clGetContextInfo(context, CL_CONTEXT_DEVICES, 0, NULL, &cb);
	devices = (cl_device_id *)malloc(sizeof(cl_device_id) * cb);
	platform->devices = devices;
	clGetContextInfo(context, CL_CONTEXT_DEVICES, cb, &devices[0], 0);

	// get the number of devices
	clGetContextInfo(context, CL_CONTEXT_NUM_DEVICES, 0, NULL, &cb);
	clGetContextInfo(context, CL_CONTEXT_NUM_DEVICES, cb, &num_devices, 0);
	fprintf(stderr, "There are %d device(s) in the context\n", num_devices);
	platform->num_devices = num_devices;

	// show devices info
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
		"_clbptWPacketSuperGroupHandler",
	};

	for(i = 0; i < NUM_KERNELS; i++)
	{
		kernels[i] = clCreateKernel(platform->program, kernels_name[i], &err);
		if(err != CL_SUCCESS)
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
	clbpt_int_node	*root = tree->root;
	clbpt_property	property = tree->property;
	cl_device_id	device = tree->platform->devices[0];
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_program		program = tree->platform->program;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	// create heap for kma
	tree->heap = kma_create(device, context, queue, program, 2048);

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
	err = clGetDeviceInfo(device, CL_DEVICE_MAX_WORK_ITEM_SIZES, 0, NULL, &cb);
	assert(err == 0);
	err = clGetDeviceInfo(device, CL_DEVICE_MAX_WORK_ITEM_SIZES, sizeof(max_work_item_sizes), &max_work_item_sizes[0], NULL);
	assert(err == 0);
	//fprintf(stderr, "Device Maximum Work Item Sizes = %zu x %zu x %zu\n", max_work_item_sizes[0], max_work_item_sizes[1], max_work_item_sizes[2]);
	//max_local_work_size = max_work_item_sizes[0];	// one dimension
	max_local_work_size = 256;	// one dimension
	//order = max_local_work_size/2;
	order = 4;
	fprintf(stderr, "Tree Order = %d\n", order);

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
	global_work_size = local_work_size = 1;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);	
	//tree->property = (clbpt_property)clEnqueueMapBuffer(queue, property_d, CL_TRUE, CL_MAP_READ, 0, sizeof(clbpt_property), 0, NULL, NULL, &err);
	//assert(err == 0);

	// DEBUG used
	//fprintf(stderr, "Initialize SUCCESS\n");

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

	// fill wait_buf with NOPs
	for(i = tree->wait_buf_index; i < buf_size; i++)
	{
		tree->wait_buf[i] = PACKET_NOP;
	}

	// clmem initialize
	cl_mem isEmpty_d;

	// clmem allocation
	wait_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(clbpt_packet), tree->wait_buf, &err);
	assert(err == 0);
	execute_buf_d = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	assert(err == 0);
	result_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(void *), (void *)tree->result_buf, &err);
	assert(err == 0);
	execute_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	assert(err == 0);
	result_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(void *), NULL, &err);
	assert(err == 0);
	isEmpty_d = clCreateBuffer(context, 0, sizeof(uint8_t), NULL, &err);
	assert(err == 0);

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
	assert(err == 0);
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	err = clEnqueueReadBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == 0);
	tree->wait_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, wait_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	assert(err == 0);

	// <DEBUG>
	//tree->execute_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, execute_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	//assert(err == 0);

	/*
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
	assert(err == 0);
	tree->execute_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, execute_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	assert(err == 0);

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

	tree->execute_result_buf = (void **)clEnqueueMapBuffer(queue, result_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(void *), 0, NULL, NULL, &err);
	assert(err == 0);

	return isEmpty;
}

int _clbptHandleExecuteBuffer(clbpt_tree tree)
{
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;
	//clbpt_property property = tree->property;

	// clmem initialize

	// clmem allocation

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
	local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	tree->node_addr_buf = (void **)clEnqueueMapBuffer(queue, result_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(void *), 0, NULL, NULL, &err);
	assert(err == 0);

	// handle leaf nodes
	int i, j, k;
	int instr_result, node_result;
	clbpt_packet pkt;
	int32_t key, key_upper;
	void *node_addr;
	num_ins = 0;
	num_del = 0;

	node_addr = tree->node_addr_buf[0];
	for(i = 0, j = 0; i < buf_size; i++)
	{
		pkt = tree->execute_buf[i];
		key = getKeyFromPacket(pkt);
		if (node_addr != tree->node_addr_buf[i])	// accessed node changed
		{
			node_result = handle_node(node_addr);

			// DEBUG
			if (node_result > 0)	// insertion pkts rollback to waiting buffer
			{
				// DEBUG
				fprintf(stderr, "Insert ROLLBACK\n");
				fprintf(stderr, "node_result = %d\n", node_result);
				k = i-1;
				for (; node_result > 0; node_result--)
				{
					while (!isInsertPacket(tree->execute_buf[k]))
						k--;
					while(tree->wait_buf[j] != PACKET_NOP) 
						j++;
					tree->wait_buf[j] = tree->execute_buf[k];
					fprintf(stderr, "before delete\n");
					show_leaf(tree->leaf);
					delete_leaf(getKeyFromPacket(tree->execute_buf[k]), node_addr);
					k--;
				}
				node_result = handle_node(node_addr);	// re-handle the node
			}

			node_addr = tree->node_addr_buf[i];
		}

		if (isNopPacket(pkt))
		{
			node_result = handle_node(node_addr);
			
			if (node_result > 0)	// insertion pkts rollback to waiting buffer
			{
				// DEBUG
				fprintf(stderr, "Insert ROLLBACK\n");
				fprintf(stderr, "node_result = %d\n", node_result);
				k = i-1;
				for (; node_result > 0; node_result--)
				{
					while (!isInsertPacket(tree->execute_buf[k]))
						k--;
					while(tree->wait_buf[j] != PACKET_NOP) 
						j++;
					tree->wait_buf[j] = tree->execute_buf[k];
					fprintf(stderr, "before delete\n");
					show_leaf(tree->leaf);
					delete_leaf(getKeyFromPacket(tree->execute_buf[k]), node_addr);
					k--;
				}
				node_result = handle_node(node_addr);	// re-handle the node
			}
			break;
		}
		if (isSearchPacket(pkt))
		{
			fprintf(stderr, "search goes to node with head = %d\n", *(int32_t *)(((clbpt_leaf_node *)node_addr)->head->record_ptr));
			instr_result = search_leaf(key, node_addr, tree->execute_result_buf[i]);
		}
		else if (isRangePacket(pkt))
		{
			key_upper = getUpperKeyFromRangePacket(pkt);
			instr_result = range_leaf(key, key_upper, node_addr, tree->execute_result_buf[i]);
		}
		else if (isInsertPacket(pkt))
		{
			// DEBUG
			fprintf(stderr, "Before insert\n");
			fprintf(stderr, "insert key = %d\n", key);
			show_leaf(tree->leaf);
			instr_result = insert_leaf(key, node_addr);
			fprintf(stderr, "After insert\n");
			show_leaf(tree->leaf);
		}
		else if (isDeletePacket(pkt))
		{
			instr_result = delete_leaf(key, node_addr);
		}
	}
	fprintf(stderr, "num of pkts in buf = %d\n", i);
	if (node_result == leftMostNodeBorrowMerge)
	{
		haldle_leftmost_node(tree->leaf);
	}

	show_leaf(tree->leaf);		

	if (num_ins == 0 && num_del == 0)
	{
		return CL_SUCCESS;
	}

	
	err = clEnqueueUnmapMemObject(queue, result_buf_d, tree->node_addr_buf, 0, NULL, NULL);

	// clmem initialize
	static cl_mem ins_d;
	static cl_mem del_d;
	static cl_mem addr_d;

	// clmem allocation
	if (num_ins != 0)
	{
		ins_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_ins * sizeof(clbpt_ins_pkt), ins, &err);
		assert(err == 0);
		addr_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_ins * sizeof(void *), (void *)addr, &err);
		assert(err == 0);
	}
	if (num_del != 0)
	{
		del_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, num_del * sizeof(clbpt_del_pkt), del, &err);
		assert(err == 0);
	}

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
	local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);	

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
	free(tree->node_addr_buf);

	return CL_SUCCESS;
}

int handle_node(void *node_addr)
{
	int m;
	clbpt_leaf_node *node_sibling, *node = node_addr;
	clbpt_leaf_entry *entry_head;

	fprintf(stderr, "handle node START\n");
	if (node->num_entry >= order)	// Need Split
	{
		if (node->num_entry > 2*(order-1))
		{
			// insertion pkts rollback to waiting buffer
			return node->num_entry - 2*(order-1);
		}
		node_sibling = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
		m = half_f(node->num_entry);
		node_sibling->num_entry = node->num_entry - m;
		node->num_entry = m;

		entry_head = node->head;
		while(m-- > 0)
		{
			entry_head = entry_head->next;
		}
		node_sibling->head = entry_head;
		node_sibling->prev_node = node;
		node_sibling->next_node = node->next_node;
		node_sibling->parent = node->parent;
		node_sibling->parent_key = *((int32_t *)entry_head->record_ptr);
		node->next_node = node_sibling;

		// insert parent_key to internal node
		clbpt_entry entry_d;
		entry_d.key = node_sibling->parent_key;
		entry_d.child = NULL;
		ins[num_ins].target = node->parent;
		ins[num_ins].entry = entry_d;
		addr[num_ins] = (void *)node_sibling;
		fprintf(stderr, "insert to internal with head = %d\n", *((int32_t *)node_sibling->head->record_ptr));
		num_ins++;
	}
	else if (node->num_entry < half_f(order))	// Need Borrow or Merge
	{
		if (node->prev_node != NULL)
		{
			//if (node->prev_node->num_entry - 1 < half_f(order))	// Merge
			if (node->num_entry + node->prev_node->num_entry < order)	// Merge
			{
				node = node->prev_node;
				node_sibling = node->next_node;
				node->num_entry += node_sibling->num_entry;
				node->next_node = node_sibling->next_node;
				if (node_sibling->next_node != NULL)
				{
					node_sibling->next_node->prev_node = node;
				}

				// delete parent_key to internal node
				del[num_del].target = node_sibling->parent;
				del[num_del].key = node_sibling->parent_key;
				num_del++;

				node_sibling->head = NULL;
				node_sibling->num_entry = 0;
				node_sibling->next_node = NULL;
				node_sibling->prev_node = NULL;
				node_sibling->parent = NULL;
				node_sibling->parent_key = 0;
				free(node_sibling);
			}
			else	// Borrow
			{
				node_sibling = node->prev_node;

				// delete old parent_key to internal node
				del[num_del].target = node->parent;
				del[num_del].key = node->parent_key;
				num_del++;

				m = half_f(node_sibling->num_entry + node->num_entry);
				node->num_entry = node_sibling->num_entry + node->num_entry - m;
				node_sibling->num_entry = m;
				entry_head = node_sibling->head;
				while(m-- > 0)
				{
					entry_head = entry_head->next;
				}
				node->head = entry_head;
				node->parent_key = *((int32_t *)entry_head->record_ptr);

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
			return leftMostNodeBorrowMerge;
		}
	}

	return 0;
}

int haldle_leftmost_node(clbpt_leaf_node *node)
{
	int m, parent_key_update = 0;
	clbpt_leaf_node *node_sibling;
	clbpt_leaf_entry *entry_head;

	if (node->num_entry >= half_f(order)) return 0;
	if (node->next_node != NULL)
	{
		//if (node->next_node->num_entry - 1 < half_f(order))	// Merge
		if (node->num_entry + node->next_node->num_entry < order)	// Merge (with right sibling)
		{
			node_sibling = node->next_node;
			node->num_entry += node_sibling->num_entry;
			node->next_node = node_sibling->next_node;
			if (node_sibling->next_node != NULL)
			{
				node_sibling->next_node->prev_node = node;
			}

			// delete parent_key to internal node
			del[num_del].target = node_sibling->parent;
			del[num_del].key = node_sibling->parent_key;
			num_del++;

			node_sibling->head = NULL;
			node_sibling->num_entry = 0;
			node_sibling->next_node = NULL;
			node_sibling->prev_node = NULL;
			node_sibling->parent = NULL;
			node_sibling->parent_key = 0;
			free(node_sibling);
		}
		else	// borrow (from right sibling)
		{
			node_sibling = node->next_node;
			entry_head = node_sibling->head;
			if (*((int32_t *)entry_head->record_ptr) == node_sibling->parent_key)
			{
				parent_key_update = 1;

				// delete old parent_key to internal node
				del[num_del].target = node_sibling->parent;
				del[num_del].key = node_sibling->parent_key;
				num_del++;
			}
			m = half_f(node->num_entry + node_sibling->num_entry);
			node_sibling->num_entry = node->num_entry + node_sibling->num_entry - m;
			node->num_entry = m;
			entry_head = node->head;
			while(m-- > 0)
			{
				entry_head = entry_head->next;
			}
			node_sibling->head = entry_head;
			if (parent_key_update)
			{
				node_sibling->parent_key = *((int32_t *)entry_head->record_ptr);

				// insert new parent_key to internal node
				clbpt_entry entry_d;
				entry_d.key = node_sibling->parent_key;
				entry_d.child = NULL;
				ins[num_ins].target = node_sibling->parent;
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

	// scan through
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
	int m, existed = 0;
	clbpt_leaf_node *node;
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
		node->num_entry++;
	}
	else	// No Insert
	{
		fprintf(stderr, "INSERT FAILED: record: %d is already in the B+ Tree\n", key);
	}

	free(entry_free);

	return existed;
}

int delete_leaf(int32_t key, void *node_addr)	// not sure is it able to borrow yet
{
	int m, existed = 0;
	clbpt_leaf_node *node = node_addr;
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

void show_leaf(clbpt_leaf_node *leaf)	// function for testing
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
	while(temp->next_node != NULL)
	{
		count = temp->next_node->num_entry;
		entry = temp->next_node->head;
		while(count-- > 0)
		{
			fprintf(stderr, "|%d", *((int *)entry->record_ptr));
			entry = entry->next;
		}
		fprintf(stderr, "|   ");
		temp->next_node = temp->next_node->next_node;
	}
	fprintf(stderr, "\n");

	free(temp);
}
