/**
 * @file The back-end source file
 */
 
#include "clbpt_core.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>

// Size and Order
static size_t max_local_work_size;		// get this value in _clbptInitialize
static uint32_t order = 4;	// get this value in _clbptInitialize

/*
void insertionSort(clbpt_packet *execute_buf, void **result_buf, int l, int h)
{
	for (int m = l + 1; m <= h; m++) {
		clbpt_packet m_pkt = execute_buf[m];
		void *m_res = result_buf[m];	
		int mKey = getKeyFromPacket(m_pkt);

		int n;
		for (n = m - 1; n >= l; n--) {
			if (getKeyFromPacket(execute_buf[n]) > mKey) {
				execute_buf[n + 1] = execute_buf[n];
				result_buf[n + 1] = result_buf[n];
			}
			else
				break;
		}
		execute_buf[n + 1] = m_pkt;
		result_buf[n + 1] = m_res;
	}
}

int partition(clbpt_packet *execute_buf, void **result_buf, int l, int h)
{
	int pivot = rand() % (h - l + 1) + l;
	clbpt_packet pivot_pkt = execute_buf[pivot];
	void *pivot_res = result_buf[pivot];
	int pivotKey = getKeyFromPacket(pivot_pkt);

	execute_buf[pivot] = execute_buf[l];
	result_buf[pivot] = result_buf[l];
	for (;;) {
		while (l != h && getKeyFromPacket(execute_buf[h]) >= pivotKey)
			h--;
		if (l == h)
			break;
		execute_buf[l] = execute_buf[h];
		result_buf[l] = result_buf[h];
		l++;

		while (l != h && pivotKey >= getKeyFromPacket(execute_buf[l]))
			l++;
		if (l == h)
			break;
		execute_buf[h] = execute_buf[l];
		result_buf[h] = result_buf[l];
		h--;
	}	

	execute_buf[l] = pivot_pkt;
	result_buf[l] = pivot_res;

	return l;
}

void quick_sort(clbpt_packet *execute_buf, void **result_buf, int l, int h)
{
	if (l >= h) return;
	if (h - l > 512) {
		int m = partition(execute_buf, result_buf, l, h);
		quick_sort(execute_buf, result_buf, l, m-1);
		quick_sort(execute_buf, result_buf, m+1, h);
	}
	else
		insertionSort(execute_buf, result_buf, l, h);
}
*/

const clbpt_packet *execute_buf_hook;
int permListComp(const void *a, const void *b)
{
	int aKey = getKeyFromPacket(execute_buf_hook[*(const int *)a]);
	int bKey = getKeyFromPacket(execute_buf_hook[*(const int *)b]);
	if (aKey < bKey)
		return -1;
	else if (aKey == bKey)
		return 0;
	else
		return 1;
}

void _clbptPacketSort(clbpt_packet *execute_buf, void **execute_result_buf, uint32_t buf_size)
{
	// Initialize permutation list
	int *perm = (int *)malloc(buf_size * sizeof(int));
	for (int i = 0; i < buf_size; i++)
		perm[i] = i;
	
	// Sort it
	execute_buf_hook = execute_buf;
	qsort(perm, buf_size, sizeof(int), permListComp);

	// Sort execute_buf and result_buf by the permutation list
	clbpt_packet *execute_buf_temp = (clbpt_packet *)
		malloc(buf_size * sizeof(clbpt_packet));
	void **result_buf_temp = (void **)malloc(buf_size * sizeof(void *));
	memcpy(execute_buf_temp, execute_buf, buf_size * sizeof(clbpt_packet));
	memcpy(result_buf_temp, execute_result_buf, buf_size * sizeof(void *));
	for (int i = 0; i < buf_size; i++) {
		int index = perm[i];
		execute_buf[i] = execute_buf_temp[index];
		execute_result_buf[i] = result_buf_temp[index];
	}

	// Free allocations
	free(perm);
	free(execute_buf_temp);
	free(result_buf_temp);
}

int handle_node(clbpt_tree tree, void *node_addr, void *leftmost_node_addr);
int handle_leftmost_node(clbpt_tree tree, clbpt_leaf_node *node);

int search_leaf(int32_t key, void *node_addr, void *result_addr, size_t record_size);
int range_leaf(int32_t key, int32_t key_upper, void *node_addr, void *result_addr, size_t record_size);
int insert_leaf(int32_t key, void *node_addr, void *record_addr, size_t record_size, int order);
int delete_leaf(int32_t key, void *node_addr);

void show_pkt_buf(clbpt_packet *pkt_buf, uint32_t buf_size);	// function for testing
void show_leaves(clbpt_leaf_node *leaf, size_t record_size);	// function for testing
void _clbptPrintTree(clbpt_property *property, size_t record_size);
//void show_tree(clbpt_tree tree);

int clbpt_debug = 0;

va_list ap;

//#define _clbptDebug printf
void _clbptDebug(const char *Format, ...)
{
	if (clbpt_debug)
	{
		va_start(ap,Format);
		vfprintf(stderr, Format, ap);
		va_end(ap);
	}
}

int _clbptGetDevices(clbpt_platform platform)
{
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
	_clbptDebug( "There are %d device(s) in the context\n", num_devices);
	platform->num_devices = num_devices;

	// Show devices info
	/*
	for(int i = 0; i < num_devices; i++)
	{
		// get device name
		clGetDeviceInfo(devices[i], CL_DEVICE_NAME, 0, NULL, &cb);
		devName = (char*)malloc(cb * sizeof(char));
		clGetDeviceInfo(devices[i], CL_DEVICE_NAME, cb, &devName[0], NULL);
		devName[cb] = 0;
		_clbptDebug( "Device: %s", devName);
		free(devName);
		
		// get device supports version
		clGetDeviceInfo(devices[i], CL_DEVICE_VERSION, 0, NULL, &cb);
		devVer = (char*)malloc(cb * sizeof(char));
		clGetDeviceInfo(devices[i], CL_DEVICE_VERSION, cb, &devVer[0], NULL);
		devVer[cb] = 0;
		_clbptDebug( " (supports %s)\n", devVer);
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
		_clbptDebug( "Error occurs when creating commandQueues\n");

		return err;
	}
	else
	{
		return CL_SUCCESS;
	}
}

int _clbptCreateKernels(clbpt_platform platform)
{
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

	for(int i = 0; i < NUM_KERNELS; i++)
	{
		kernels[i] = clCreateKernel(platform->program, kernels_name[i], &err);
		if (err != CL_SUCCESS)
		{
			_clbptDebug( "kernel error %d\n", err);
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
	_clbptDebug( "kma create START\n");
	tree->heap_size = 128 * 1024 * 1024;
	err = kma_create_svm(device, context, queue, tree->heap_size, &(tree->heap));
	assert(err == CL_SUCCESS);
	_clbptDebug( "kma create SUCCESS\n");

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

	// Create buffers
	tree->node_addr_buf = (void **)malloc(tree->buf_size * sizeof(void *));
	tree->instr_result_buf = (int *)calloc(tree->buf_size, sizeof(int));

	// Create insert, delete packets buffers
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
	//_clbptDebug( "Device Maximum Work Item Sizes = %zu x %zu x %zu\n", max_work_item_sizes[0], max_work_item_sizes[1], max_work_item_sizes[2]);
	max_local_work_size = max_work_item_sizes[0];	// one dimension
	if (tree->order <= 0)
		tree->order = order;
		//tree->order = max_local_work_size/2;
	_clbptDebug( "Tree Order = %d\n", tree->order);

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
	tree->isEmpty_d = clCreateBuffer(context, 0, sizeof(uint8_t), NULL, &err);
	assert(err == CL_SUCCESS);

	tree->property_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, sizeof(clbpt_property), tree->property, &err);
	assert(err == CL_SUCCESS);

	tree->ins_d = clCreateBuffer(context, 0, tree->buf_size * sizeof(clbpt_ins_pkt), NULL, &err);
	assert(err == CL_SUCCESS);
	tree->del_d = clCreateBuffer(context, 0, tree->buf_size * sizeof(clbpt_del_pkt), NULL, &err);
	assert(err == CL_SUCCESS);
	tree->leafmirror_addr_d = clCreateBuffer(context, 0, tree->buf_size * sizeof(void *), NULL, &err);
	assert(err == CL_SUCCESS);

	// kernel _clbptInitialize
	kernel = kernels[CLBPT_INITIALIZE];
	err = clSetKernelArg(kernel, 0, sizeof(tree->property->root), (void *)&tree->property->root);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(tree->property_d), (void *)&tree->property_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArgSVMPointer(kernel, 2, tree->heap);
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
	uint8_t isEmpty = 1;

	size_t global_work_size;
	size_t local_work_size;

	cl_int err;
	cl_context		context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel		*kernels = tree->platform->kernels;
	cl_kernel		kernel;

	// Fill end of wait_buf with NOPs
	for(int i = tree->wait_buf_index; i < tree->buf_size; i++)
	{
		tree->wait_buf[i] = PACKET_NOP;
	}

	err = clEnqueueWriteBuffer(queue, tree->wait_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->wait_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueWriteBuffer(queue, tree->result_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(void *), tree->wait_result_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	// kernel _clbptPacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	err = clSetKernelArg(kernel, 0, sizeof(tree->isEmpty_d), (void *)&tree->isEmpty_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(tree->wait_buf_d), (void *)&tree->wait_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(tree->execute_buf_d), (void *)&tree->execute_buf_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(tree->buf_size), (void *)&tree->buf_size);
	assert(err == CL_SUCCESS);

	// kernel _clbptPacketSort
	/*
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
	*/

	// PacketSelect
	kernel = kernels[CLBPT_PACKET_SELECT];
	isEmpty = 1;
	global_work_size = tree->buf_size;
	local_work_size = max_local_work_size;
	err = clEnqueueWriteBuffer(queue, tree->isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	err = clEnqueueReadBuffer(queue, tree->isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->wait_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->wait_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	if (isEmpty)	// Return if the wait_buf is empty
		return isEmpty;

	// PacketSort
	/*
	kernel = kernels[CLBPT_PACKET_SORT];
	global_work_size = local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	err = clEnqueueReadBuffer(queue, tree->execute_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->execute_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->result_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(void *), tree->execute_result_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	*/
	err = clEnqueueReadBuffer(queue, tree->execute_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->execute_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueReadBuffer(queue, tree->result_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(void *), tree->execute_result_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	_clbptPacketSort(tree->execute_buf, tree->execute_result_buf, tree->buf_size);

	err = clEnqueueWriteBuffer(queue, tree->execute_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(clbpt_packet), tree->execute_buf, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
	err = clEnqueueWriteBuffer(queue, tree->result_buf_d, CL_TRUE, 0, tree->buf_size * sizeof(void *), tree->execute_result_buf, 0, NULL, NULL);
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

	// Get the number of non-NOP packets in execute buffer
	int num_packets;
	for(num_packets = 0; num_packets < tree->buf_size; num_packets++)
	{
		if (isNopPacket((clbpt_packet)tree->execute_buf[num_packets]))
			break;
	}
	_clbptDebug( "Host: num_pkt=%d\n", num_packets);

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
	int node_result = 0;
	clbpt_packet pkt;
	int32_t key, key_upper;
	void *node_addr;
	void *leftmost_node_addr = NULL;
	tree->num_ins = 0;
	tree->num_del = 0;

	node_addr = tree->node_addr_buf[0];

	// Scan through the execute buffer
	for(int i = 0, j = 0; i < tree->buf_size; i++)
	{
		//fprintf(stderr, "node_addr = %p\n", tree->node_addr_buf[i]);
		pkt = tree->execute_buf[i];
		key = getKeyFromPacket(pkt);

		if (isNopPacket(pkt) ||						// Reach the end of the execute buffer
			i == tree->buf_size-1 ||
			node_addr != tree->node_addr_buf[i])	// Accessed node changed	
		{
			if (i == 0)	// Do nothing if the execute buffer is empty
				break;

			node_result = handle_node(tree, node_addr, leftmost_node_addr);	// Handle the last operated node

			if (node_result == leftMostNodeBorrowMerge)
			{
				leftmost_node_addr = node_addr;
			}
			else if (node_result == mergeWithLeftMostNode)
			{
				leftmost_node_addr = NULL;
			}

			if (leftmost_node_addr != NULL &&
				((clbpt_leaf_node *)node_addr)->mirror->parent !=
				((clbpt_leaf_node *)leftmost_node_addr)->mirror->parent)	// node's parent is different with leftmost node's, handle leftmost node
			{
				handle_leftmost_node(tree, leftmost_node_addr);
				leftmost_node_addr = NULL;
			}

			if (isNopPacket(pkt) || i == tree->buf_size-1)	// Reach the end of the execute buffer
				break;
			else
				node_addr = tree->node_addr_buf[i];
		}

		if (isSearchPacket(pkt))
		{
			tree->instr_result_buf[i] = search_leaf(key, node_addr, tree->execute_result_buf[i], tree->record_size);
		}
		else if (isRangePacket(pkt))
		{
			key_upper = getUpperKeyFromRangePacket(pkt);
			tree->instr_result_buf[i] = range_leaf(key, key_upper, node_addr, tree->execute_result_buf[i], tree->record_size);
		}
		else if (isInsertPacket(pkt))
		{
			_clbptDebug( "insert key = %d\n", key);
			tree->instr_result_buf[i] = insert_leaf(key, node_addr, tree->execute_result_buf[i], tree->record_size, tree->order);
			if (tree->instr_result_buf[i] == needInsertionRollback)
			{
				_clbptDebug( "Insertion ROLLBACK with key = %d\n", key);
				while(tree->wait_buf[j] != PACKET_NOP)	// Search forward for empty space in wait buffer
				{
					j++;
				}
				tree->wait_buf[j] = tree->execute_buf[i];	// Rollback the insertion packet to wait buffer
				tree->wait_result_buf[j] = tree->execute_result_buf[i];
			}
			//<DEBUG>
			_clbptDebug( "After insert\n");
			//show_leaves(tree->leaf, tree->record_size);
			//</DEBUG>
		}
		else if (isDeletePacket(pkt))
		{
			//<DEBUG>
			_clbptDebug( "delete key = %d\n", key);
			//</DEBUG>
			tree->instr_result_buf[i] = delete_leaf(key, node_addr);
			//<DEBUG>
			_clbptDebug( "After delete\n");
			//show_leaves(tree->leaf, tree->record_size);
			//</DEBUG>
		}

	}
	_clbptDebug( "Final result\n");
	//show_leaves(tree->leaf, tree->record_size);

	if (tree->num_ins == 0 && tree->num_del == 0)
	{
		return CL_SUCCESS;
	}

	if (tree->num_ins > 0)
	{
		for (int i = 0; i < tree->num_ins; i++) {
			//printf("Insert packet %d\n", tree->ins[i].entry.key);
		}


		err = clEnqueueWriteBuffer(queue, tree->ins_d, CL_TRUE, 0, tree->num_ins * sizeof(clbpt_ins_pkt), tree->ins, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		//<DEBUG>
		/*
		err = clEnqueueReadBuffer(queue, tree->ins_d, CL_TRUE, 0, tree->num_ins * sizeof(clbpt_ins_pkt), tree->ins, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		printf( "insert packets to internal node:\n");
		for(int i = 0; i < tree->num_ins; i++)
		{
			_clbptDebug( "insert %d, target: %p\n", tree->ins[i].entry.key, tree->ins[i].target);
		}
		*/
		//</DEBUG>
	}
	if (tree->num_del > 0)
	{
		err = clEnqueueWriteBuffer(queue, tree->del_d, CL_TRUE, 0, tree->num_del * sizeof(clbpt_del_pkt), tree->del, 0, NULL, NULL);
		assert(err == CL_SUCCESS);

		//<DEBUG>
		err = clEnqueueReadBuffer(queue, tree->del_d, CL_TRUE, 0, tree->num_del * sizeof(clbpt_del_pkt), tree->del, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		_clbptDebug( "delete packets to internal node:\n");
		for(int i = 0; i < tree->num_del; i++)
		{
			_clbptDebug( "delete %d, target: %p\n", tree->del[i].key, tree->del[i].target);
		}
		//</DEBUG>
	}

	// kernel _clbptWPacketInit
	kernel = kernels[CLBPT_WPACKET_INIT];
	err = clSetKernelArg(kernel, 0, sizeof(tree->ins_d), (void *)&tree->ins_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 1, sizeof(tree->leafmirror_addr_d), (void *)&tree->leafmirror_addr_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 2, sizeof(tree->num_ins), (void *)&tree->num_ins);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 3, sizeof(tree->del_d), (void *)&tree->del_d);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 4, sizeof(tree->num_del), (void *)&tree->num_del);
	assert(err == CL_SUCCESS);
	err = clSetKernelArgSVMPointer(kernel, 5, tree->heap);
	assert(err == CL_SUCCESS);
	err = clSetKernelArg(kernel, 6, sizeof(tree->property_d), (void *)&tree->property_d);
	assert(err == CL_SUCCESS);

	// WPacketInit
	global_work_size = tree->num_ins > tree->num_del ? tree->num_ins : tree->num_del;
	local_work_size = max_local_work_size;
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);

	_clbptDebug( "enqueue wpacket init SUCCESS\n");
	err = clEnqueueReadBuffer(queue, tree->property_d, CL_TRUE, 0, sizeof(clbpt_property), (tree->property), 0, NULL, NULL);
	
	// Assign leafmirror_addr to node_sibling's parent
	if (tree->num_ins > 0)
	{
		err = clEnqueueReadBuffer(queue, tree->leafmirror_addr_d, CL_TRUE, 0, tree->num_ins * sizeof(void *), tree->leafmirror_addr, 0, NULL, NULL);
		assert(err == CL_SUCCESS);

		for(int i = 0; i < tree->num_ins; i++)
		{
			((clbpt_leaf_node *)tree->leafnode_addr[i])->mirror = tree->leafmirror_addr[i];
			((clbpt_leafmirror *)tree->leafmirror_addr[i])->leaf = 
				tree->leafnode_addr[i];
		}
	}

	_clbptDebug( "leafmirror_addr assign SUCCESS\n");
	
	//_clbptPrintTree(tree->property, tree->record_size);

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

	// Free buffers
	free(tree->node_addr_buf);
	free(tree->instr_result_buf);

	// Free insert, delete packets
	free(tree->ins);
	free(tree->del);
	free(tree->leafnode_addr);
	free(tree->leafmirror_addr);

	// Free device-side memory
	clReleaseMemObject(tree->wait_buf_d);
	clReleaseMemObject(tree->execute_buf_d);
	clReleaseMemObject(tree->result_buf_d);
	clReleaseMemObject(tree->execute_buf_d_temp);
	clReleaseMemObject(tree->result_buf_d_temp);
	clReleaseMemObject(tree->isEmpty_d);

	clReleaseMemObject(tree->property_d);

	clReleaseMemObject(tree->ins_d);
	clReleaseMemObject(tree->del_d);
	clReleaseMemObject(tree->leafmirror_addr_d);

	clSVMFree(tree->platform->context, tree->heap);

	return CL_SUCCESS;
}

int handle_node(clbpt_tree tree, void *node_addr, void *leftmost_node_addr)
{
	int m;
	clbpt_leaf_node *node_sibling, *node = node_addr;
	clbpt_leaf_entry *entry_head;

	_clbptDebug( "handle node START\n");
	if (node->num_entry >= tree->order)	// Need to Split
	{
		node_sibling = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
		m = half_f(node->num_entry);
		node_sibling->num_entry = node->num_entry - m;
		node->num_entry = m;
		//printf("split result: %d, %d\n", node->num_entry, node_sibling->num_entry);

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
		entry_d.child = (uintptr_t)NULL;
		tree->ins[tree->num_ins].target = node->mirror;
		tree->ins[tree->num_ins].entry = entry_d;
		tree->leafnode_addr[tree->num_ins] = (void *)node_sibling;
		tree->num_ins++;
	}
	else if (node->num_entry < half_f(tree->order))	// Need Borrow or Merge
	{
		if (node->prev_node != NULL && node->prev_node->mirror->parent == node->mirror->parent)
		{
			if (node->num_entry + node->prev_node->num_entry < tree->order)	// Merge (with left sibling)
			{
				_clbptDebug( "Merging...\n");

				node = node->prev_node;
				node_sibling = node->next_node;
				node->num_entry += node_sibling->num_entry;
				node->next_node = node_sibling->next_node;
				if (node_sibling->next_node != NULL)
				{
					node_sibling->next_node->prev_node = node;
				}

				// Delete parent_key to internal node
				tree->del[tree->num_del].target = node_sibling->mirror;
				tree->del[tree->num_del].key = node_sibling->parent_key;
				tree->num_del++;

				node_sibling->head = NULL;
				node_sibling->num_entry = 0;
				node_sibling->next_node = NULL;
				node_sibling->prev_node = NULL;
				node_sibling->mirror = NULL;
				node_sibling->parent_key = 0;
				free(node_sibling);

				if (node == leftmost_node_addr)
					return mergeWithLeftMostNode;
			}
			else	// Borrow (from left sibling)
			{
				node_sibling = node->prev_node;

				// Delete old parent_key to internal node
				tree->del[tree->num_del].target = node->mirror;
				tree->del[tree->num_del].key = node->parent_key;
				tree->num_del++;

				m = half_f(node_sibling->num_entry + node->num_entry);
				node->num_entry = node_sibling->num_entry + node->num_entry - m;
				node_sibling->num_entry = m;
				_clbptDebug( "left node with %d entries, right node with %d entries\n", node_sibling->num_entry, node->num_entry);
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
				entry_d.child = (uintptr_t)NULL;
				tree->ins[tree->num_ins].target = node->mirror;
				tree->ins[tree->num_ins].entry = entry_d;
				tree->leafnode_addr[tree->num_ins] = (void *)node;
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

	_clbptDebug( "handle leftmost_node START with num_entry = %d\n", node->num_entry);
	if (node->num_entry >= half_f(tree->order)) return 0;
	if (node->next_node != NULL)
	{
		
		if (node->num_entry + node->next_node->num_entry < tree->order)	// Merge (with right sibling)
		{
			_clbptDebug( "Merge\n");
			node_sibling = node->next_node;
			node->num_entry += node_sibling->num_entry;
			node->next_node = node_sibling->next_node;
			if (node_sibling->next_node != NULL)
			{
				node_sibling->next_node->prev_node = node;
			}

			// Delete parent_key to internal node
			tree->del[tree->num_del].target = node_sibling->mirror;
			tree->del[tree->num_del].key = node_sibling->parent_key;
			tree->num_del++;

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
			if (entry_head->key == node_sibling->parent_key)
			{
				parent_key_update = 1;

				// Delete old parent_key to internal node
				tree->del[tree->num_del].target = node_sibling->mirror;
				tree->del[tree->num_del].key = node_sibling->parent_key;
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
				entry_d.child = (uintptr_t)NULL;
				tree->ins[tree->num_ins].target = node_sibling->mirror;
				tree->ins[tree->num_ins].entry = entry_d;
				tree->leafnode_addr[tree->num_ins] = (void *)node_sibling;
				tree->num_ins++;
			}
		}
	}

	return 0;
}

int search_leaf(int32_t key, void *node_addr, void *result_addr, size_t record_size)
{
	int existed = 0;
	clbpt_leaf_node *node;
	clbpt_leaf_entry *entry;

	node = node_addr;
	entry = node->head;

	// Scan through entries
	while (entry != NULL)
	{
		if (entry->key == key)
		{
			existed = 1;
			memcpy(result_addr, entry->record_ptr, record_size);
			break;
		}
		if (entry->key > key)
		{
			result_addr = NULL;
			break;
		}
		entry = entry->next;
	}

	if (existed)
	{
		switch (record_size)
		{
			case sizeof(char):
				_clbptDebug("FOUND: (key: %d, record: %c) is in the B+ Tree\n", key, *((char *)result_addr));
				break;
			case sizeof(int):
				_clbptDebug("FOUND: (key: %d, record: %d) is in the B+ Tree\n", key, *((int *)result_addr));
				break;
			case sizeof(double):
				_clbptDebug("FOUND: (key: %d, record: %lf) is in the B+ Tree\n", key, *((double *)result_addr));
				break;
			default:
				break;
		}
	}
	else
	{
		_clbptDebug( "NOT FOUND: key: %d is NOT in the B+ Tree\n", key);
	}

	return existed;
}

int range_leaf(int32_t key, int32_t key_upper, void *node_addr, void *result_addr, size_t record_size)
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
		{
			num_records += 1;
			entry = entry->next;
		}
		end = entry;
	}

	if (num_records > 0)
	{
		((clbpt_pair_group *)result_addr)->num_pairs = num_records;

		_clbptDebug( "RANGE [%d, %d] FOUND:\n", key, key_upper);
		for(entry = start, i = 0; i < num_records; entry = entry->next, i++)
		{
			((clbpt_pair_group *)result_addr)->pairs[i].key = entry->key;
			memcpy(&((clbpt_pair_group_list)result_addr)->pairs[i].record, entry->record_ptr, record_size);
			switch (record_size)
			{
				case sizeof(char):
					_clbptDebug( "\t(key: %d, record: %c)\n", entry->key, *((char *)entry->record_ptr));
					break;
				case sizeof(int):
					_clbptDebug( "\t(key: %d, record: %d)\n", entry->key, *((int *)entry->record_ptr));
					break;
				case sizeof(double):
					_clbptDebug( "\t(key: %d, record: %lf)\n", entry->key, *((double *)entry->record_ptr));
					break;
				default:
					break;
			}
		}
		/*
		result_addr = (void *)malloc(sizeof(void *) * (num_records+1));
		((int *)result_addr)[0] = num_records;
		_clbptDebug( "RANGE FOUND:\n");
		for(entry = start, i = num_records, j = 1; i > 0; entry = entry->next, i--, j++)
		{
			((void **)result_addr)[j] = entry->record_ptr;
			_clbptDebug( "\t(key: %d, record: %d)\n", entry->key, *((CLBPT_RECORD_TYPE *)entry->record_ptr));
		}
		_clbptDebug( "are inside the range[%d, %d]\n", start->key, end->key);
		*/
	}
	else
	{
		((clbpt_pair_group *)result_addr)->num_pairs = 0;
		_clbptDebug( "RANGE [%d, %d] NOT FOUND:\n", key, key_upper);
	}

	return num_records;
}

int insert_leaf(int32_t key, void *node_addr, void *record_addr, size_t record_size, int order)
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
		if (node->num_entry+1 > 2*(order-1))
		{
			return needInsertionRollback;
		}

		entry_new = (clbpt_leaf_entry *)malloc(sizeof(clbpt_leaf_entry));
		entry_new->key = key;
		entry_new->record_ptr = (void *)malloc(record_size);
		//memcpy(entry_new->record_ptr, &record, record_size);
		memcpy(entry_new->record_ptr, record_addr, record_size);
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
		_clbptDebug( "INSERT FAILED: key: %d is already in the B+ Tree\n", key);
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

	_clbptDebug( "node_head with key = %d\n", node->head->key);

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
			_clbptDebug( "node_head with key = %d\n", node->head->key);
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
		_clbptDebug( "DELETE FAILED: key: %d was not in the B+ Tree\n", key);
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
	_clbptDebug( "Internal node:\n");
	err = clEnqueueNDRangeKernel(queue, kernel, 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == CL_SUCCESS);
}
*/

void _clbptPrintLeaf(int level, clbpt_leaf_node *leaf, size_t record_size)	// function for testing
{
	int count;
	clbpt_leaf_entry *entry;

	for (int i = 0; i < level; i++) {
		printf( " ");
	}
	printf( "Leaf:%p (mirror=%p) ", leaf, leaf->mirror);
	count = leaf->num_entry;
	entry = leaf->head;
	while (count-- > 0)
	{
		switch (record_size)
		{
			case sizeof(char):
				printf( "|(key: %d, rec: %c)", entry->key, *((char *)entry->record_ptr));
				break;
			case sizeof(int):
				printf( "|(key: %d, rec: %d)", entry->key, *((int *)entry->record_ptr));
				break;
			case sizeof(double):
				printf( "|(key: %d, rec: %lf)", entry->key, *((double *)entry->record_ptr));
				break;
			default:
				break;
		}
		entry = entry->next;
	}
	printf( "|   ");
	printf( "\n");

}

void
_clbptPrintMirror(
	int level,
	clbpt_leafmirror *mirror,
	size_t record_size
	)
{
	for (int i = 0; i < level; i++)
		printf( " ");
	printf( "Mirror:%p (parent=%p, leaf=%p)\n",
		(void *)mirror, (void *)mirror->parent, mirror->leaf);
	_clbptPrintLeaf(level + 1, mirror->leaf, record_size);
}

void
_clbptPrintNode(
	int level_proc,
	int level_tree,
	clbpt_int_node *node,
	size_t record_size
	)
{
	for (int i = 0; i < level_proc; i++)
		printf( " ");
	printf( "Node:%p (num_entry=%u, parent=%p, parent_key=%d)\n",
		(void *)node, node->num_entry, (void *)node->parent,
		getKey(node->parent_key));
	if (level_proc < level_tree - 2)
	for (int i = 0; i < node->num_entry; i++) {
		for (int j = 0; j < level_proc; j++)
			printf( " ");
		printf( ">Entry #%d (key=%d, child=%p)\n",
			i, getKey(node->entry[i].key), (void *)node->entry[i].child);
		_clbptPrintNode(level_proc + 1, level_tree,
			(clbpt_int_node *)node->entry[i].child,
			record_size);
	}
	else
	for (int i = 0; i < node->num_entry; i++) {
		for (int j = 0; j < level_proc; j++)
			printf( " ");
		printf( ">Entry #%d (key=%d, child=%p)\n",
			i, getKey(node->entry[i].key), (void *)node->entry[i].child);
		_clbptPrintMirror(level_proc + 1,
			(clbpt_leafmirror *)node->entry[i].child,
			record_size);
	}

}

void
_clbptPrintTree(
	clbpt_property *property,
	size_t record_size
	)
{
	int level = property->level;
	uintptr_t root = (uintptr_t)property->root;

	printf( "### Traversal of GPU-side tree ###\n");
	printf( "Level of tree: %d\n", level);
	if (level <= 1)
		_clbptPrintMirror(0, (clbpt_leafmirror *)root, record_size);
	else
		_clbptPrintNode(0, level, (clbpt_int_node *)root, record_size);
	printf( "### End of traversal ###\n");
}

void show_pkt_buf(clbpt_packet *pkt_buf, uint32_t buf_size)
{
	int i;

	_clbptDebug( "===  buf_info  ===\n");
	for (i = 0; i < buf_size; i++)
	{
		if (isNopPacket(pkt_buf[i]))
		{
			_clbptDebug( "%d\t", i);
			_clbptDebug( "NOP\n");
		}
		else if (isSearchPacket(pkt_buf[i]))
		{
			_clbptDebug( "%d\t", i);
			_clbptDebug( "SEARCH\t%4d \t\t\n", getKeyFromPacket(pkt_buf[i]));
		}
		else if (isRangePacket(pkt_buf[i]))
		{
			_clbptDebug( "%d\t", i);
			_clbptDebug( "RANGE\t%4d %4d\t\n", getKeyFromPacket(pkt_buf[i]), getUpperKeyFromRangePacket(pkt_buf[i]));
		}
		else if (isInsertPacket(pkt_buf[i]))
		{
			_clbptDebug( "%d\t", i);
			_clbptDebug( "INSERT\t%4d \t\t\n", getKeyFromPacket(pkt_buf[i]));
		}
		else if (isDeletePacket(pkt_buf[i]))
		{
			_clbptDebug( "%d\t", i);
			_clbptDebug( "DELETE\t%4d \t\t\n", getKeyFromPacket(pkt_buf[i]));
		}
	}
	_clbptDebug( "==================\n");
}

void show_leaves(clbpt_leaf_node *leaf, size_t record_size)	// function for testing
{
	int count;
	clbpt_leaf_node *temp = (clbpt_leaf_node *)malloc(sizeof(clbpt_leaf_node));
	temp->next_node = leaf;
	clbpt_leaf_entry *entry;

	if (temp == NULL || leaf == NULL || temp->next_node->head == NULL)
	{
		_clbptDebug( "Leaf is EMPTY\n");
		return;
	}

	while(1)
	{
		//
		//_clbptDebug( "\nmirror: %p", temp->next_node->mirror);
		//_clbptDebug( "\tparent: %p   ", temp->next_node->mirror->parent);
		//		
		for(count = temp->next_node->num_entry, entry = temp->next_node->head;
			count > 0; count--)
		{
			switch (record_size)
			{
				case sizeof(char):
					_clbptDebug( "|(%d -> %c)", entry->key, *((char *)entry->record_ptr));
					break;
				case sizeof(int):
					_clbptDebug( "|(%d -> %d)", entry->key, *((int *)entry->record_ptr));
					break;
				case sizeof(double):
					_clbptDebug( "|(%d -> %lf)", entry->key, *((double *)entry->record_ptr));
					break;
				default:
					break;
			}
			entry = entry->next;
		}
		
		if ((temp->next_node = temp->next_node->next_node) != NULL)
		{
			_clbptDebug( "|---");
		}
		else
		{
			_clbptDebug( "|");
			break;
		}
	}
	_clbptDebug( "\n");

	free(temp);
}
