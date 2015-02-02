/**
 * @file The back-end source file
 */
 
#include "clbpt_core.h"
#include "clbpt_type.h"
#include <assert.h>

void _clbptSelectFromWaitBuffer(clbpt_tree tree)
{
	cl_int err;
	size_t global_work_size;
	size_t local_work_size = 256;
	unsigned int isEmpty = 1;

	cl_context context = tree->platform->context;
	cl_command_queue queue = tree->platform->queue;
	cl_kernel *kernels = tree->platform->kernels;
	size_t buf_size = tree->buf_size;

	// clmem initialize
	static cl_mem wait_buf_d, execute_buf_d, result_buf_d;
	static cl_mem execute_buf_d_temp, result_buf_d_temp;
	static cl_mem isEmpty_d;

	// clmem allocation
	wait_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(clbpt_packet), wait_buf, &err);
	execute_buf_d = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	result_buf_d = clCreateBuffer(context, CL_MEM_COPY_HOST_PTR, buf_size * sizeof(void *), (void *)result_buf, &err);
	execute_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(clbpt_packet), NULL, &err);
	result_buf_d_temp = clCreateBuffer(context, 0, buf_size * sizeof(void *), NULL, &err);
	isEmpty_d = clCreateBuffer(context, 0, sizeof(uint8_t), NULL, &err);

	// kernel PacketSelect
	err = clSetKernelArg(kernels[0], 0, sizeof(isEmpty_d), (void *)&isEmpty_d);
	assert(err == 0);
	err = clSetKernelArg(kernels[0], 1, sizeof(wait_buf_d), (void *)&wait_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernels[0], 2, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernels[0], 3, sizeof(buf_size), (void *)&buf_size);
	assert(err == 0);

	// kernel PacketSort
	err = clSetKernelArg(kernels[1], 0, sizeof(execute_buf_d), (void *)&execute_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernels[1], 1, sizeof(result_buf_d), (void *)&result_buf_d);
	assert(err == 0);
	err = clSetKernelArg(kernels[1], 2, sizeof(execute_buf_d_temp), (void *)&execute_buf_d_temp);
	assert(err == 0);
	err = clSetKernelArg(kernels[1], 3, sizeof(result_buf_d_temp), (void *)&result_buf_d_temp);
	assert(err == 0);
	err = clSetKernelArg(kernels[1], 4, sizeof(buf_size), (void *)&buf_size);
	assert(err == 0);

	// PacketSelect
	isEmpty = 1;
	global_work_size = buf_size;
	err = clEnqueueWriteBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == 0);
	err = clEnqueueNDRangeKernel(queue, kernels[0], 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	err = clEnqueueReadBuffer(queue, isEmpty_d, CL_TRUE, 0, sizeof(uint8_t), &isEmpty, 0, NULL, NULL);
	assert(err == 0);
	if (isEmpty)
	{
		tree->buf_status = CLBPT_STATUS_DONE;
		return isEmpty;
	}

	// PacketSort
	global_work_size = 256;
	err = clEnqueueNDRangeKernel(queue, kernels[1], 1, NULL, &global_work_size, &local_work_size, 0, NULL, NULL);
	assert(err == 0);
	execute_buf = (clbpt_packet *)clEnqueueMapBuffer(queue, execute_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(clbpt_packet), 0, NULL, NULL, &err);
	assert(err == 0);
	result_buf = (void **)clEnqueueMapBuffer(queue, result_buf_d, CL_TRUE, CL_MAP_READ, 0, buf_size * sizeof(void *), 0, NULL, NULL, &err);
	assert(err == 0);
	tree->buf_status = CLBPT_STATUS_WAIT;
	return isEmpty;	/* not done */
}