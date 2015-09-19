#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <CL/cl.h>
#include "clbpt.h"
 
#define MAT_SIZE 15

extern void
_clbptPrintTree(
	clbpt_property *property,
	size_t record_size
	);
 
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
 
	err = clGetDeviceIDs( platform_id, CL_DEVICE_TYPE_GPU, 1, &device_id, &ret_num_devices );
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

	//printf("data information about devices are: %d\n", (int)arr[0]);

	p = malloc(sizeof(struct _clbpt_platform));
	clbptCreatePlatform(&p, context);
	fprintf(stderr, "CreatePlatform SUCCESS\n");
	clbptCreateTree(&t, p, 4, sizeof(int));
	fprintf(stderr, "CreateTree SUCCESS\n");
/*	
	// read test data
#define CLBPT_STOP_TYPE 0
#define CLBPT_INSERT_TYPE 1
#define CLBPT_SEARCH_TYPE 2
#define CLBPT_DELETE_TYPE 3
#define CLBPT_RANGE_TYPE 4
	FILE *input_data ;
	if(input_data=fopen("input","r+b"))printf("fuck\n");
	int bfsize = 128;
	int *data_buffer = calloc(128, sizeof(int));
	int *rec_buffer = calloc(128, sizeof(int));
	clbpt_pair_group_list pg_list;
	time_t start_time;
	time_t end_time;
	while(1){
		int i;
		int err = CL_SUCCESS;
		int data_info[2] = {0,0};
		fread(data_info, 2, sizeof(int), input_data);
		printf("Info %d %d\n",data_info[0],data_info[1]);
		getchar();
		time(&start_time);
		if(!data_info[0] ) break;
		if (data_info[1] > bfsize){
			bfsize = data_info[1];
			data_buffer = realloc(data_buffer, bfsize*sizeof(int));
			rec_buffer = realloc(rec_buffer, bfsize*sizeof(int));
		}
		switch (data_info[0]){
		case  CLBPT_SEARCH_TYPE:
			fread(data_buffer, data_info[1], sizeof(int), input_data);
			for (i = 0; i < data_info[1]; i++){
				printf("%d key : %d.\n", i, data_buffer[i]);
			}
			clbptEnqueueSearches(t, data_info[1], data_buffer, rec_buffer);
			break;
		case  CLBPT_INSERT_TYPE:
			fread(data_buffer, data_info[1], sizeof(int), input_data);
			fread(rec_buffer, data_info[1], sizeof(int), input_data);
			//for (i = 0; i < data_info[1]; i++){
				//printf("%d key : %d.\n", i, data_buffer[i]);
			//}
			//getchar();
			clbptEnqueueInsertions(t, data_info[1], data_buffer, rec_buffer);
			break;
		case  CLBPT_DELETE_TYPE:
			fread(data_buffer, data_info[1], sizeof(int), input_data);
			for (i = 0; i < data_info[1]; i++){
				printf("%d key : %d.\n", i, data_buffer[i]);
			}
			clbptEnqueueDeletions(t, data_info[1], data_buffer);
			break;
		case  CLBPT_RANGE_TYPE:
			//clbptCreatePairGroupList(&pg_list, data_info[1], l_keys, u_keys);
			//clbptEnqueueRangeSearches(t, data_info[1], l_keys, u_keys, pg_list);
			//clbptReleasePairGroupList(&pg_list, data_info[1]);
			break;
		default: exit(-1);
		}
		if (err != CL_SUCCESS)
		{
			fprintf(stderr, "EnqueInsertions ERROR\n");
			exit(err);
		}
		clbptFinish(t);
		fflush(0);
		if (data_info[0] == CLBPT_SEARCH_TYPE)
			for (i = 0; i < data_info[1]; i++)
				printf("key(%d) : %d\n", i, rec_buffer[i]);
		time(&end_time);
		int delay = difftime(end_time, start_time);
		_clbptPrintTree(t->property, t->record_size);
		printf("Cost time : %d seconds\n",delay);
	}
	return 0;
*/
	//===========================
	///*
	int d[10] = { 7,21,24,23,14,15,16,17,18,19 };
	int d_rec[10] = { 8,22,25,24,15,16,17,18,19,20 };
	err = clbptEnqueueInsertions(t, 10, d, (void **)d_rec);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);
	getchar();
	printf("============\n");
/*
	
	int a[10] = { 10,3,5,6,2,8,11,12,13,1 };
	int a_rec[10] = { 11,4,6,7,3,9,12,13,14,2 };
	err = clbptEnqueueInsertions(t, 10, a, (void **)a_rec);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);
	getchar();

	err = clbptEnqueueDeletions(t, 3, a);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueInsertions ERROR\n");
	}
	clbptFinish(t);
	getchar();
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

	int l_keys[10] = { 1, 1, 2, 3, 4, 5, 5, 3, 2, 1 };
	int u_keys[10] = { 5, 5, 7, 9, 5, 7, 8, 4, 3, 2 };
	clbpt_pair_group_list pg_list;
	clbptCreatePairGroupList(&pg_list, 10, l_keys, u_keys);
	err = clbptEnqueueRangeSearches(t, 10, l_keys, u_keys, pg_list);
	if (err != CL_SUCCESS)
	{
		fprintf(stderr, "EnqueRangeSearches ERROR\n");
	}
	clbptFinish(t);
	clbptReleasePairGroupList(&pg_list, 10);
	printf("============\n/");
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
