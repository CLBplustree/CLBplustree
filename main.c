#include <stdio.h>
#include <stdlib.h>
#include <time.h>
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
 
	err = clGetDeviceIDs( platform_id, CL_DEVICE_TYPE_CPU, 1, &device_id, &ret_num_devices );
	err_check(err, "clGetDeviceIDs");
	{
		cl_device_svm_capabilities caps;

		cl_int err = clGetDeviceInfo(
			device_id,
			CL_DEVICE_SVM_CAPABILITIES,
			sizeof(cl_device_svm_capabilities),
			&caps,
			0
			);

		if (err != CL_SUCCESS)
			fprintf(stderr, "No info!\n");
		if (!(caps & CL_DEVICE_SVM_COARSE_GRAIN_BUFFER))
			fprintf(stderr, "No support for svm!\n");
	}


	context = clCreateContext( NULL, 1, &device_id, NULL, NULL, &err );
	err_check( err, "clCreateContext" );

	printf("data information about devices are: %d\n", (int)arr[0]);

	p = malloc(sizeof(struct _clbpt_platform));
	clbptCreatePlatform(&p, context);
	fprintf(stderr, "CreatePlatform SUCCESS\n");
	clbptCreateTree(&t, p, 256, sizeof(int));
	fprintf(stderr, "CreateTree SUCCESS\n");
	
	int d[10] = { 7,21,24,23,14,15,16,17,18,19 };
<<<<<<< HEAD
	int d_rec[10] = { 8,22,25,24,15,16,17,18,20 };
	err = clbptEnqueueInsertions(t, 1, d, d_rec);
=======
	int d_rec[10] = { 8,22,25,24,15,16,17,18,19,20 };
	err = clbptEnqueueInsertions(t, 1, d, NULL);
>>>>>>> origin/master
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);
	//getchar();
	printf("============\n");

	
	int a[10] = { 10,3,5,6,2,8,11,12,13,1 };
	int a_rec[10] = { 11,4,6,7,3,9,12,13,14,2 };
	err = clbptEnqueueInsertions(t, 9, a, a_rec);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t); 

	err = clbptEnqueueDeletions(t, 3, a, NULL);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);
	getchar();
	///*
	printf("============\n");

	int b[2] = { 4,11 };
	int b_rec[2] = { 5, 12 };
	err = clbptEnqueueInsertions(t, 2, b, b_rec);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueDeletions ERROR\n");
	}
	clbptFinish(t);
	printf("============\n");

	int c[2] = { 1,9 };
	int c_rec[2] = { 2, 10 };
	err = clbptEnqueueInsertions(t, 2, c, c_rec);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);
	printf("============\n/");

	
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
