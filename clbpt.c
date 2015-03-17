/**
 * @file The front-end source file
 */
 
#include "clbpt.h"
#include "clbpt_core.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define CLBPT_PACKET_SEARCH(x) ((( (clbpt_packet)(x) << 32 ) & 0x7FFFFFFF00000000 ) | 0x7FFFFFFF )
#define CLBPT_PACKET_RANGE(x,y) ((( (clbpt_packet)(x)  << 32 ) & 0xFFFFFFFF00000000 ) | ( (uint32_t)(y) | 0x80000000 ) )
#define CLBPT_PACKET_INSERT(x,y) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ) | (uint32_t)(y) )
#define CLBPT_PACKET_DELETE(x) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ))

void _clbpt_load_program(clbpt_platform platform,char *filename)
{
   FILE *f = fopen(filename,"r");
   cl_int err;
   if(f==NULL)exit(-1);
   fseek(f, 0, SEEK_END);
   long fsize = ftell(f);
   fseek(f, 0, SEEK_SET);
   char *string = malloc(fsize + 1);
   fread(string, fsize, 1, f);
   string[fsize] = 0;
   fclose(f);
   const char *source = &string[0];
   platform->program = clCreateProgramWithSource(platform->context, 1, &source, 0, &err);
   if(err != 0)
      printf("error load program : %d\n",err);
   if(platform->program == NULL)
      printf("error load program\n");
   if((err = clBuildProgram( platform->program, 1, platform->devices, " -I /home/mangohot/KMA", 0, 0)) != CL_SUCCESS) 
   {
      printf("error build program : %d\n",err);
      size_t len;
      char *buffer;
      clGetProgramBuildInfo(platform->program, platform->devices[0], CL_PROGRAM_BUILD_LOG, 0, NULL, &len);
      buffer = calloc(sizeof(char),len);
      clGetProgramBuildInfo(platform->program, platform->devices[0], CL_PROGRAM_BUILD_LOG, len, buffer, NULL);
      printf("%s\n", buffer);
   }
}

void * _clbptHandler(void *tree)                                                {
    while( 1 )                                                                  {
        pthread_mutex_lock(&(((clbpt_tree)tree)->loop_mutex))                   ;
        _clbptSelectFromWaitBuffer((clbpt_tree)tree)                            ;
        _clbptHandleExecuteBuffer((clbpt_tree)tree)                             ;}}

int _clbptLockWaitBuffer(clbpt_tree tree)                                       {
    int err = pthread_mutex_lock(&(tree->buffer_mutex))                         ;
    if( err != CLBPT_SUCCESS )return err                                        ;
    return CLBPT_SUCCESS                                                        ;}

int _clbptUnlockWaitBuffer(clbpt_tree tree)                                     {
    int err = pthread_mutex_unlock(&(tree->buffer_mutex))                       ;
    if( err != CLBPT_SUCCESS )return err                                        ;
    return CLBPT_SUCCESS                                                        ;}

int _clbptBufferExchange(clbpt_tree tree)                                       {
    int err = _clbptLockWaitBuffer(tree)                                        ;
    if( err != CLBPT_SUCCESS ) return err                                       ;
    clbpt_packet *fetch_buf_temp = tree->fetch_buf                              ;
    tree->fetch_buf = tree->wait_buf                                            ;
    tree->wait_buf = fetch_buf_temp                                             ;
    tree->fetch_buf_index = 0                                                   ;
    void **result_buf_temp = tree->result_buf                                   ;
    tree->result_buf = tree->execute_result_buf                                 ;
    tree->execute_result_buf = result_buf_temp                                  ;
    tree->fetch_buf_index = 0                                                   ;
    err = _clbptUnlockWaitBuffer(tree)                                          ;
    if( err != CLBPT_SUCCESS ) return err                                       ;
    pthread_mutex_unlock(&(tree->loop_mutex))                                   ;
    return CLBPT_SUCCESS                                                        ;}

int clbptEnqueueFecthBuffer(
        clbpt_tree tree, 
        clbpt_packet packet, 
        void *records)                                                          {
    if( tree->fetch_buf_index >= CLBPT_BUF_SIZE )                               {
        _clbptBufferExchange(tree)                                              ;}
    tree->fetch_buf[ tree->fetch_buf_index ] = packet                           ;
    tree->result_buf[ tree->fetch_buf_index++ ] = records                       ;
    return CLBPT_SUCCESS                                                        ;}

int clbptCreatePlatform(
        clbpt_platform dst_platform, 
        cl_context context)                                                     {
    cl_int err                                                                  ;
    size_t cb                                                                   ;
    dst_platform = malloc(sizeof(struct _clbpt_platform))                       ;
    dst_platform->context = context                                             ;
    dst_platform->kernels = calloc(sizeof(cl_kernel),64)			;
    clGetContextInfo(context, CL_CONTEXT_DEVICES, 0, NULL, &cb)                 ;
    cl_device_id *devices = calloc( sizeof(cl_device_id),cb)                    ;
    dst_platform->devices = devices;
    clGetContextInfo(context, CL_CONTEXT_DEVICES, cb, &devices[0], 0)           ;
    clGetDeviceInfo(devices[0], CL_DEVICE_NAME, 0, NULL, &cb)                   ;
    _clbpt_load_program(dst_platform,"clbpt.cl")				;
    dst_platform->kernels[CLBPT_INITIALIZE] = clCreateKernel(dst_platform->program,"_clbptInitialize",&err);
    dst_platform->queue = clCreateCommandQueueWithProperties(
	context, 
	devices[0], 
	0, 
	&err)    								;
    if(err != CL_SUCCESS || dst_platform->queue == 0)                           {
        clReleaseContext(context)                                               ;
        return err                                                              ;}
	return CLBPT_SUCCESS                                                    ;}

int clbptCreateTree(	
            clbpt_tree dst_tree, 
			clbpt_platform platform, 
			const int degree, 
			const size_t record_size)				{
    int err                                                                     ;
    pthread_t thread                                                            ;
    dst_tree = malloc(sizeof(struct _clbpt_tree))                               ;
    dst_tree->platform = platform                                               ;
    dst_tree->degree = degree                                                   ;
    dst_tree->record_size = record_size                                         ;
    dst_tree->buf_status = CLBPT_STATUS_DONE                                    ;
    dst_tree->fetch_buf = calloc(sizeof(clbpt_packet),CLBPT_BUF_SIZE)           ;
    dst_tree->execute_buf = calloc(sizeof(clbpt_packet),CLBPT_BUF_SIZE)         ;
    dst_tree->result_buf = calloc(sizeof(void *),CLBPT_BUF_SIZE)                ;
    dst_tree->execute_result_buf = calloc(sizeof(void *),CLBPT_BUF_SIZE)        ;
    dst_tree->fetch_buf_index = 0                                               ;
    if( (err = pthread_mutex_init(&(dst_tree->buffer_mutex),NULL)) != 0)
	    return err								;
    if( (err = pthread_mutex_init(&(dst_tree->loop_mutex),NULL)) != 0) 
	    return err								;
    _clbptInitialize(dst_tree)                                                  ;
    pthread_create(&thread,NULL,_clbptHandler,dst_tree)                         ;
    return CLBPT_SUCCESS                                                        ;}

int clbptReleaseTree(clbpt_tree tree)                                           {
    if(tree == NULL ) return CLBPT_SUCCESS                                      ;
    if(tree->fetch_buf != NULL ) free(tree->fetch_buf)                          ;
    if(tree->wait_buf != NULL ) free(tree->wait_buf)                            ;
    if(tree->result_buf != NULL ) free(tree->result_buf)                        ;
    free(tree)                                                                  ;
    return CLBPT_SUCCESS                                                        ;}

int clbptEnqueueSearches(	
                clbpt_tree tree, 
				int num_keys, 
				CLBPT_KEY_TYPE *keys, 
				void *records)					{
	int i, err                                                              ;
	for( i = 0 ; i < num_keys ; i++ )                                       {
		err = clbptEnqueueFecthBuffer(	
                tree, 
		CLBPT_PACKET_SEARCH(keys[i]), 
		records)			                                ;
        if( err != CLBPT_SUCCESS ) return err                                   ;}
    return CLBPT_SUCCESS                                                        ;}

int clbptEnqueueRangeSearches(
                clbpt_tree tree, 
                int num_keys, 
                CLBPT_KEY_TYPE *l_keys, 
                CLBPT_KEY_TYPE *u_keys, 
                void **record_list)                                             {
	int i, err                                                              ;
	for( i = 0 ; i < num_keys ; i++ )                                       {
		err = clbptEnqueueFecthBuffer(
		tree, 
		CLBPT_PACKET_RANGE(l_keys[i],u_keys[i]), 
		(void *)record_list)		    	                        ;
        if( err != CLBPT_SUCCESS ) return err                                   ;}
    return CLBPT_SUCCESS                                                        ;}

int clbptEnqueueInsertions(	
                clbpt_tree tree, 
				int num_inserts, 
				CLBPT_KEY_TYPE *keys, 
				void *records)				        {
	int i, err                                                              ;
	for( i = 0 ; i < num_inserts ; i++ )                                    {
		err = clbptEnqueueFecthBuffer(	
                tree, 
		CLBPT_PACKET_INSERT(keys[i],0),
		records)        			                        ;
        if( err != CLBPT_SUCCESS ) return err                                   ;}
    return CLBPT_SUCCESS                                                        ;}

int clbptEnqueueDeletions(
        clbpt_tree tree, 
        int num_deletes, 
        CLBPT_KEY_TYPE *keys)                                                   {
	int i, err                                                              ;
	for( i = 0 ; i < num_deletes ; i++ )                                    {
		err = clbptEnqueueFecthBuffer(	
                tree, 
		CLBPT_PACKET_DELETE(keys[i]), 
		NULL)		        				        ;
        if( err != CLBPT_SUCCESS ) return err                                   ;}
    return CLBPT_SUCCESS                                                        ;}

int clbptFlush(clbpt_tree tree)                                                 {
    int err = _clbptBufferExchange(tree)                                        ;
    if( err != CLBPT_SUCCESS ) return err                                       ;
    return CLBPT_SUCCESS                                                        ;}

int clbptFinish(clbpt_tree tree)                                                {
    int err = clbptFlush(tree)                                                  ;
    if( err != CLBPT_SUCCESS ) return err                                       ;
    err = _clbptLockWaitBuffer(tree)                                            ;
    err = _clbptUnlockWaitBuffer(tree)                                          ;
    pthread_mutex_lock(&(tree->loop_mutex))                                     ;
    if( err != CLBPT_SUCCESS ) return err                                       ;
    return CLBPT_SUCCESS                                                        ;}
