#include <stdio.h>
#include <stdlib.h>
#include <CL/cl.h>
#include "clbpt.h"
 
#define MAT_SIZE 15
 
void err_check( int err, char *err_code ) {
	if ( err != CL_SUCCESS ) {
		printf("Error: %d\n",err);
		exit(-1);
	}
}
 
int main()
{
	cl_platform_id platform_id = NULL;
	cl_device_id device_id = NULL;
	cl_context context = NULL;
	//cl_command_queue command_queue = NULL, queue_device;
	//const cl_queue_properties queue_device_prop[] = {CL_QUEUE_PROPERTIES, (cl_command_queue_properties)(CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE | CL_QUEUE_ON_DEVICE | CL_QUEUE_ON_DEVICE_DEFAULT), 0};
	cl_program program = NULL;
	cl_kernel kernel = NULL;
	cl_uint ret_num_devices;
	cl_uint ret_num_platforms;
	cl_int err;
	clbpt_platform p; 
	clbpt_tree t; 
	

	cl_device_id arr[5];
	printf("size of device id: %lu\n",sizeof(cl_device_id));
	printf("size of cl_int: %lu\n",sizeof(cl_int));
 
	float mat_a[ MAT_SIZE ];
	for ( cl_int i = 0; i < MAT_SIZE; i++ ) {
		mat_a[i] = i;
	}
 
	err = clGetPlatformIDs( 1, &platform_id, &ret_num_platforms );
	err_check( err, "clGetPlatformIDs" );
 
	err = clGetDeviceIDs( platform_id, CL_DEVICE_TYPE_DEFAULT, 1, &device_id, &ret_num_devices );
	err_check( err, "clGetDeviceIDs" );

	context = clCreateContext( NULL, 1, &device_id, NULL, NULL, &err );
	err_check( err, "clCreateContext" );

	//queue_device = clCreateCommandQueueWithProperties(context, device_id, queue_device_prop, &err);
 
	printf("data information about devices are: %d\n", (int)arr[0]);

	p = malloc(sizeof(struct _clbpt_platform));
	clbptCreatePlatform(&p, context);
	fprintf(stderr, "CreatePlatform SUCCESS\n");
	//cl_kernel kern = p->kernels[0];
	clbptCreateTree(&t, p, 256, 64);
	fprintf(stderr, "CreateTree SUCCESS\n");

	int d[1] = { 7 };
	err = clbptEnqueueInsertions(t, 1, d, NULL);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);

	printf("============");
	int a[4] = { 10,3,5,6};
	err = clbptEnqueueInsertions(t, 4, a, NULL);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t); 
	///*
	printf("============");

	int b[2] = { 4,11 };
	err = clbptEnqueueInsertions(t, 2, b, NULL);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueDeletions ERROR\n");
	}
	clbptFinish(t);
	printf("============");

	int c[2] = { 1,9 };
	err = clbptEnqueueInsertions(t, 2, c, NULL);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);
	printf("============");

	
	//*/
/*
	int b[3] = {2,4,0};
	err = clbptEnqueueSearches(t, 3, b, NULL);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueSearches ERROR\n");
	}
	clbptFlush(t);
*/
	printf("haha\n"); 
	//err = clFlush( command_queue );
	//err = clFinish( command_queue );
	//err = clReleaseKernel( kernel );
//	err = clReleaseProgram( program );
	//err = clReleaseCommandQueue( command_queue );
//	err = clReleaseContext( context );
	getchar();
	clbptReleaseTree(t);
 
	return 0;
 
}
