#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <CL/cl.h>
#include <assert.h>
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
	clbptCreateTree(&t, p, 32, sizeof(int));
	fprintf(stderr, "CreateTree SUCCESS\n");

	// read test data
#define CLBPT_STOP_TYPE 0
#define CLBPT_INSERT_TYPE 1
#define CLBPT_SEARCH_TYPE 2
#define CLBPT_DELETE_TYPE 3
#define CLBPT_RANGE_TYPE 4
	FILE *input_data ;
	input_data=fopen("input","r");
	assert(input_data != NULL);
	int bfsize = 128;
	int *data_buffer = calloc(128, sizeof(int));
	int *rec_buffer = calloc(128, sizeof(int));
	clbpt_pair_group_list pg_list;
	struct timeval start_time, end_time;
	//time_t start_time;
	//time_t end_time;
	while(1){
		int i;
		int err = CL_SUCCESS;
		int data_info[2] = {0,0};
		fscanf(input_data, "%d%d", &data_info[0], &data_info[1]);
		//fread(data_info, 2, sizeof(int), input_data);
		printf("Info %d %d\n",data_info[0],data_info[1]);
		getchar();
		if(!data_info[0] ) break;
		if (data_info[1] > bfsize){
			bfsize = data_info[1];
			data_buffer = realloc(data_buffer, bfsize*sizeof(int));
			rec_buffer = realloc(rec_buffer, bfsize*sizeof(int));
		}
		switch (data_info[0]){
		case  CLBPT_SEARCH_TYPE:
			//fread(data_buffer, data_info[1], sizeof(int), input_data);
			for (i = 0; i < data_info[1]; i++){
				fscanf(input_data, "%d", &data_buffer[i]);
				//printf("%d key : %d.\n", i, data_buffer[i]);
			}
			gettimeofday(&start_time, NULL);
			//time(&start_time);
			clbptEnqueueSearches(t, data_info[1], data_buffer, (void **)rec_buffer);
			break;
		case  CLBPT_INSERT_TYPE:
			//fread(data_buffer, data_info[1], sizeof(int), input_data);
			//fread(rec_buffer, data_info[1], sizeof(int), input_data);
			for (i = 0; i < data_info[1]; i++){
				fscanf(input_data, "%d", &data_buffer[i]);
				fscanf(input_data, "%d", &rec_buffer[i]);
				//printf("%d key : %d.\n", i, data_buffer[i]);
			}
			gettimeofday(&start_time, NULL);
			//time(&start_time);
			clbptEnqueueInsertions(t, data_info[1], data_buffer, (void **)rec_buffer);
			break;
		case  CLBPT_DELETE_TYPE:
			//fread(data_buffer, data_info[1], sizeof(int), input_data);
			for (i = 0; i < data_info[1]; i++){
				fscanf(input_data, "%d", &data_buffer[i]);
				//printf("%d key : %d.\n", i, data_buffer[i]);
			}
			gettimeofday(&start_time, NULL);
			//time(&start_time);
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
		//if (data_info[0] == CLBPT_SEARCH_TYPE)
		//	for (i = 0; i < data_info[1]; i++)
		//		printf("key(%d) : %d\n", data_buffer[i], rec_buffer[i]);
		gettimeofday(&end_time, NULL);
		int msec = (end_time.tv_sec-start_time.tv_sec)*1000;
		msec += (end_time.tv_usec-start_time.tv_usec)/1000;
		int delay = msec;
		//time(&end_time);
		//int delay = difftime(end_time, start_time);
		//_clbptPrintTree(t->property, t->record_size);
		printf("CLBPT Cost time : %d ms\n", delay);
	}

	clbptReleaseTree(t);
	clbptReleasePlatform(p);
	assert(clReleaseContext(context) == CL_SUCCESS);
 
	return 0;
 
}
