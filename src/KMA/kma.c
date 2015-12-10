#include "kma.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <CL/cl.h>

// Macros
#define PROGRAM_FILENAME "kma.cl"
#define NUM_KERNELS 1

char kernels_name[][128] = {
	"initializeClheap"
};

cl_program clLoadProgram(cl_context context, cl_device_id device, const char* filename);

int clCreateKernels(cl_program program, cl_kernel **kernels_ptr);

int clReleaseKernels(cl_kernel *kernels);

int kma_create_svm(cl_device_id device, cl_context context, cl_command_queue queue, uint64_t size, void **clheap)
{
	// OpenCL Variables
	cl_program program;
	cl_kernel *kernels;
	size_t cb;
	cl_int err;
	cl_uint num;
	size_t global_work_size[1] = {1};
	size_t local_work_size[1] = {1};

	// Create and compile the program object
	program = clLoadProgram(context, device, PROGRAM_FILENAME);
	if (program == NULL)
	{
		perror("Error, can't load or build program\n");
		exit(1);
	}
	// Create kernel objects from program
	err = clCreateKernels(program, &kernels);
	if (err)
	{
		return 1;
	}
	
	// Allocate clheap space 
	void *heap;
	{
		heap = clSVMAlloc(context, CL_MEM_READ_WRITE | 
			CL_MEM_SVM_FINE_GRAIN_BUFFER | CL_MEM_SVM_ATOMICS, size, 0);
		assert(heap != NULL);

		err = clSetKernelArgSVMPointer(kernels[0], 0, heap);
		assert(err == CL_SUCCESS);
		err = clSetKernelArg(kernels[0], 1, sizeof(uint64_t), (void *)&size);
		assert(err == CL_SUCCESS);

		err = clEnqueueNDRangeKernel(queue, kernels[0], 1, NULL, global_work_size, 
			local_work_size, 0, NULL, NULL);
		assert(err == CL_SUCCESS);
		clFinish(queue);
	}
	*clheap = heap;

	// Release cl objects
	clReleaseKernels(kernels);
	clReleaseProgram(program);

	return 0;
}

cl_program clLoadProgram(cl_context context, cl_device_id device, const char* filename)
{
	FILE *fp;
	size_t length;
	char *data;
	const char* source;
	size_t ret;

	// Open file
	fp = fopen(filename, "rb");
	if (fp == NULL)
		fprintf(stderr, "Error opening file\n");

	// Get length of file
	fseek(fp, 0, SEEK_END);
	length = ftell(fp);
	fseek(fp, 0, SEEK_SET);

	// Read program source
	data = (char*)malloc((length+1) * sizeof(char));
	ret = fread(data, sizeof(char), length, fp);
	if (ret != length)
		fprintf(stderr, "Error reading file\n");
	data[length] = 0;

	// Create and build program object
	source = &data[0];
	cl_program program = clCreateProgramWithSource(context, 1, &source, NULL, NULL);
	if (program == NULL) {
		fprintf(stderr, "Error creating program\n");
		return NULL;
	}

	// Compile program
	if (clBuildProgram(program, 0, NULL, "-I . -cl-std=CL2.0", NULL, NULL) != CL_SUCCESS)
	{
		cl_int err;
		size_t len;
		char *buffer;

		clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG, 0, NULL, &len);
		buffer = calloc(sizeof(char), len);
		clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG, len, buffer, NULL);
		fprintf(stderr, "Error building program %d: %s\n", err, buffer);
		return NULL;
	}

	free(data);
	fclose(fp);

	return program;
}

int clCreateKernels(cl_program program, cl_kernel **kernels_ptr)
{
	cl_int err;
	cl_kernel *kernels = (cl_kernel *)malloc(NUM_KERNELS * sizeof(cl_kernel));

	for(int i = 0; i < NUM_KERNELS; i++)
	{
		kernels[i] = clCreateKernel(program, kernels_name[i], &err);
		if (err != CL_SUCCESS)
		{
			fprintf(stderr, "Error creating kernels\n");
			return err;
		}
	}

	*kernels_ptr = kernels;

	return CL_SUCCESS;
}

int clReleaseKernels(cl_kernel *kernels)
{
	for (int i = 0; i < NUM_KERNELS; i++)
	{
		clReleaseKernel(kernels[i]);
	}

	return CL_SUCCESS;
}
