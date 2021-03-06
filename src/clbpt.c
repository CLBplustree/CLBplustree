/**
 * @file The front-end source file
 */

#define _CRT_SECURE_NO_WARNINGS
#include "clbpt.h"
#include "clbpt_core.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <time.h>
//#include <unistd.h>

#define CLBPT_PACKET_SEARCH(x) ((( (clbpt_packet)(x) << 32 ) & 0x7FFFFFFF00000000 ) | 0x7FFFFFFF )
#define CLBPT_PACKET_RANGE(x,y) ((( (clbpt_packet)(x)  << 32 ) & 0xFFFFFFFF00000000 ) | ( (uint32_t)(y) | 0x80000000 ) )
#define CLBPT_PACKET_INSERT(x,y) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ) | (uint32_t)(y) )
#define CLBPT_PACKET_DELETE(x) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ))

void _clbptLoadProgram(clbpt_platform platform, char *filename)
{
	FILE *f = fopen(filename, "r");
	cl_int err;
	if (f == NULL) exit(-1);
	fseek(f, 0, SEEK_END);
	long fsize = ftell(f);
	fseek(f, 0, SEEK_SET);
	char *string = malloc(fsize + 1);
	fread(string, fsize, 1, f);
	string[fsize] = 0;
	fclose(f);
	const char *source = &string[0];
	platform->program = clCreateProgramWithSource(platform->context, 1, &source, 0, &err);
	if (err != 0)
		_clbptDebug( "error load program : %d\n", err);
	if (platform->program == NULL)
		_clbptDebug( "error load program\n");
	if ((err = clBuildProgram(platform->program, 1, platform->devices, "-cl-std=CL2.0 -I .", 0, 0)) != CL_SUCCESS)
	{
		size_t len;
		char *buffer;
		clGetProgramBuildInfo(platform->program, platform->devices[0], CL_PROGRAM_BUILD_LOG, 0, NULL, &len);
		buffer = calloc(sizeof(char), len);
		clGetProgramBuildInfo(platform->program, platform->devices[0], CL_PROGRAM_BUILD_LOG, len, buffer, NULL);
		printf( "Error Build Program %d: %s\n", err, buffer);
		exit(-1);
	}
}

int _clbptLockWaitBuffer(clbpt_tree tree);	///
int _clbptUnlockWaitBuffer(clbpt_tree tree);	///

void * _clbptHandler(void *tree)
{
	int isEmpty;

	while (1)
	{
		if(((clbpt_tree)tree)->close_thread) pthread_exit(0);
		_clbptLockWaitBuffer((clbpt_tree)tree);
		if (((clbpt_tree)tree)->is_Complete == 1)
		{
			_clbptUnlockWaitBuffer((clbpt_tree)tree);
			continue;
		}
		_clbptUnlockWaitBuffer((clbpt_tree)tree);
		do {
			_clbptDebug( "select START\n");
			isEmpty = _clbptSelectFromWaitBuffer((clbpt_tree)tree);
			if (isEmpty)
				break;
			_clbptDebug( "isEmpty = %d\n", isEmpty);
			_clbptDebug( "select COMPLETE\n");
			_clbptDebug( "handle START\n");
			_clbptHandleExecuteBuffer((clbpt_tree)tree);
			_clbptDebug( "handle COMPLETE\n");
		} while (isEmpty != 1);
		_clbptLockWaitBuffer((clbpt_tree)tree);
		((clbpt_tree)tree)->is_Complete = 1;
		_clbptUnlockWaitBuffer((clbpt_tree)tree);
		_clbptDebug( "==========================================\n");
	}
}

int _clbptLockWaitBuffer(clbpt_tree tree)
{
	int err = pthread_mutex_lock(&(tree->buffer_mutex));
	if (err != CLBPT_SUCCESS) return err;
	return CLBPT_SUCCESS;
}

int _clbptUnlockWaitBuffer(clbpt_tree tree)
{
	int err = pthread_mutex_unlock(&(tree->buffer_mutex));
	if (err != CLBPT_SUCCESS) return err;
	return CLBPT_SUCCESS;
}

int _clbptBufferExchange(clbpt_tree tree)
{
	clbpt_packet *fetch_buf_temp = tree->fetch_buf;
	tree->fetch_buf = tree->wait_buf;
	tree->wait_buf = fetch_buf_temp;
	tree->wait_buf_index = tree->fetch_buf_index;
	tree->fetch_buf_index = 0;
	void **result_buf_temp = tree->fetch_result_buf;
	tree->fetch_result_buf = tree->wait_result_buf;
	tree->wait_result_buf = result_buf_temp;
	_clbptDebug( "buffer exchange COMPLETE\n");
	//while (pthread_mutex_trylock(&tree->buffer_mutex) == 0)_clbptUnlockWaitBuffer(tree);
	return CLBPT_SUCCESS;
}

int clbptEnqueueFetchBuffer(
	clbpt_tree tree,
	clbpt_packet packet,
	void *record_addr)
{
	if (tree->fetch_buf_index >= CLBPT_BUF_SIZE)
	{
		//_clbptBufferExchange(tree);
		clbptFinish(tree);
	}
	tree->fetch_buf[tree->fetch_buf_index] = packet;
	tree->fetch_result_buf[tree->fetch_buf_index++] = record_addr;
	return CLBPT_SUCCESS;
}

int clbptCreatePlatform(
	clbpt_platform *dst_platform_ptr,
	cl_context context) 
{
	cl_int err;
	size_t cb;
	clbpt_platform dst_platform;

	dst_platform = malloc(sizeof(struct _clbpt_platform));
	dst_platform->context = context;

	_clbptGetDevices(dst_platform);
	_clbptLoadProgram(dst_platform, "clbpt.cl");
	_clbptCreateKernels(dst_platform);
	_clbptCreateQueues(dst_platform);

	*dst_platform_ptr = dst_platform;
	return CLBPT_SUCCESS;
}

int clbptReleasePlatform(
	clbpt_platform platform)
{
	cl_int err = 0;

	// Release command queues
	err |= clReleaseCommandQueue(platform->queue);
	err |= clReleaseCommandQueue(platform->queue_device);
	assert(err == 0);

	// Release kernels
	for (int i = 0; i < NUM_KERNELS; i++) {
		err |= clReleaseKernel(platform->kernels[i]);
	}
	free(platform->kernels);
	assert(err == 0);

	// Release program
	err |= clReleaseProgram(platform->program);
	assert(err == 0);

	return CLBPT_SUCCESS;	
}

int clbptCreateTree(
	clbpt_tree *dst_tree_ptr,
	clbpt_platform platform,
	const int order,
	const size_t record_size)
{
	int err;
	clbpt_tree dst_tree;
	dst_tree = malloc(sizeof(struct _clbpt_tree));
	dst_tree->platform = platform;
	dst_tree->order = order;
	dst_tree->record_size = record_size;
	dst_tree->buf_status = CLBPT_STATUS_DONE;
	dst_tree->fetch_buf = calloc(sizeof(clbpt_packet), CLBPT_BUF_SIZE);
	dst_tree->wait_buf = calloc(sizeof(clbpt_packet), CLBPT_BUF_SIZE);
	dst_tree->execute_buf = calloc(sizeof(clbpt_packet), CLBPT_BUF_SIZE);
	dst_tree->fetch_result_buf = calloc(sizeof(void *), CLBPT_BUF_SIZE);
	dst_tree->wait_result_buf = calloc(sizeof(void *), CLBPT_BUF_SIZE);
	dst_tree->execute_result_buf = calloc(sizeof(void *), CLBPT_BUF_SIZE);
	dst_tree->fetch_buf_index = 0;
	dst_tree->close_thread = 0;
	dst_tree->is_Complete = 1;
	dst_tree->readOnlyMode = 1;
	if ((err = pthread_mutex_init(&(dst_tree->buffer_mutex), NULL)) != 0)
		return err;
	if ((err = pthread_mutex_init(&(dst_tree->loop_mutex), NULL)) != 0)
		return err;
	_clbptInitialize(dst_tree);
	pthread_create(&dst_tree->handler, NULL, _clbptHandler, dst_tree);
	*dst_tree_ptr = dst_tree;
	return CLBPT_SUCCESS;
}

int clbptReleaseTree(clbpt_tree tree)
{
	tree->close_thread = 1;
	_clbptUnlockWaitBuffer(tree);
	pthread_join(tree->handler, NULL);
	if (tree == NULL)
		return CLBPT_SUCCESS;
	_clbptReleaseLeaf(tree);
	// wait_buf is corrupted by clbptSelectFromWaitBuffer
	if (tree->fetch_buf != NULL) 
		free(tree->fetch_buf);
	if (tree->wait_buf != NULL) 
		free(tree->wait_buf);
	if (tree->execute_buf != NULL)
		free(tree->execute_buf);
	if (tree->fetch_result_buf != NULL)
		free(tree->fetch_result_buf);
	if (tree->wait_result_buf != NULL)
		free(tree->wait_result_buf);
	if (tree->execute_result_buf != NULL)
		free(tree->execute_result_buf);
	free(tree);

	return CLBPT_SUCCESS;
}

int clbptCreatePairGroupList(
	clbpt_pair_group_list *pair_group_list_ptr,
	int num_pair_groups,
	CLBPT_KEY_TYPE *l_keys,
	CLBPT_KEY_TYPE *u_keys)
{
	int i;
	clbpt_pair_group_list pair_group_list;

	pair_group_list = calloc(sizeof(clbpt_pair_group), num_pair_groups);
	for(i = 0; i < num_pair_groups; i++)
	{
		pair_group_list[i].pairs = calloc(sizeof(clbpt_pair), u_keys[i]-l_keys[i]+2);
	}

	*pair_group_list_ptr = pair_group_list;

	return CLBPT_SUCCESS;
}

int clbptReleasePairGroupList(clbpt_pair_group_list *pair_group_list_ptr, int num_pair_groups)
{
	int i;

	for(i = num_pair_groups-1; i >= 0; i--)
	{
		free((*pair_group_list_ptr)[i].pairs);
	}
	free(*pair_group_list_ptr);
}

int clbptEnqueueSearches(
	clbpt_tree tree,
	int num_keys,
	CLBPT_KEY_TYPE *keys,
	void **record_list)
{
	int i, err;
	tree->readOnlyMode = 1;
	for (i = 0; i < num_keys; i++)
	{
		/*
		err = clbptEnqueueFetchBuffer(
			tree,
			CLBPT_PACKET_SEARCH(keys[i]),
			record_list[i]);
		*/
		if (tree->record_size == sizeof(char))
			err = clbptEnqueueFetchBuffer(
				tree,
				CLBPT_PACKET_SEARCH(keys[i]),
				(void *)((char *)record_list + i));
		else if (tree->record_size == sizeof(int))
			err = clbptEnqueueFetchBuffer(
				tree,
				CLBPT_PACKET_SEARCH(keys[i]),
				(void *)((int *)record_list + i));
		else if (tree->record_size == sizeof(double))
			err = clbptEnqueueFetchBuffer(
				tree,
				CLBPT_PACKET_SEARCH(keys[i]),
				(void *)((double *)record_list + i));
		if (err != CLBPT_SUCCESS) return err;
	}
	return CLBPT_SUCCESS;
}

int clbptEnqueueRangeSearches(
	clbpt_tree tree,
	int num_keys,
	CLBPT_KEY_TYPE *l_keys,
	CLBPT_KEY_TYPE *u_keys,
	clbpt_pair_group_list pair_group_list) 
{
	int i, err;
	tree->readOnlyMode = 1;
	for (i = 0; i < num_keys; i++)
	{
		err = clbptEnqueueFetchBuffer(
			tree,
			CLBPT_PACKET_RANGE(l_keys[i], u_keys[i]),
			(void *)(pair_group_list+i));
		if (err != CLBPT_SUCCESS) return err;
	}
	return CLBPT_SUCCESS;
}

int clbptEnqueueInsertions(
	clbpt_tree tree,
	int num_inserts,
	CLBPT_KEY_TYPE *keys,
	void **records)
{
	int i, err;
	tree->readOnlyMode = 0;
	for (i = 0; i < num_inserts; i++)
	{
		/*
		err = clbptEnqueueFetchBuffer(
			tree,
			CLBPT_PACKET_INSERT(keys[i], 1),
			records[i]);
		*/
		if (tree->record_size == sizeof(char))
			err = clbptEnqueueFetchBuffer(
				tree,
				CLBPT_PACKET_INSERT(keys[i], 1),
				(void *)((char *)records+ i));
		else if (tree->record_size == sizeof(int))
			err = clbptEnqueueFetchBuffer(
				tree,
				CLBPT_PACKET_INSERT(keys[i], 1),
				(void *)((int *)records+ i));
		else if (tree->record_size == sizeof(double))
			err = clbptEnqueueFetchBuffer(
				tree,
				CLBPT_PACKET_INSERT(keys[i], 1),
				(void *)((double *)records+ i));
		if (err != CLBPT_SUCCESS) return err;
	}
	return CLBPT_SUCCESS;
}

int clbptEnqueueDeletions(
	clbpt_tree tree,
	int num_deletes,
	CLBPT_KEY_TYPE *keys) 
{
	int i, err;
	tree->readOnlyMode = 0;
	for (i = 0; i < num_deletes; i++)
	{
		err = clbptEnqueueFetchBuffer(
			tree,
			CLBPT_PACKET_DELETE(keys[i]),
			NULL);
		if (err != CLBPT_SUCCESS) return err;
	}
	return CLBPT_SUCCESS;
}

int clbptFinish(clbpt_tree tree)
{
	_clbptDebug("Enter finish\n");
	for (;;) {
		_clbptLockWaitBuffer((clbpt_tree)tree);
		if (tree->is_Complete) {
			int err = _clbptBufferExchange(tree);
			if (err != CLBPT_SUCCESS) return err;
			tree->is_Complete = 0;
			break;
		}
		_clbptUnlockWaitBuffer((clbpt_tree)tree);
	}
	_clbptUnlockWaitBuffer((clbpt_tree)tree);
	for (;;) {
		_clbptLockWaitBuffer((clbpt_tree)tree);
		if (tree->is_Complete)
			break;
		_clbptUnlockWaitBuffer((clbpt_tree)tree);
	}
	_clbptUnlockWaitBuffer((clbpt_tree)tree);
	return CLBPT_SUCCESS;
}
