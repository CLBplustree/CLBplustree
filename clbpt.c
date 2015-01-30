/**
 * @file The front-end source file
 */
 
#include "clbpt.h"
#include <stdlib.h>
#include <string.h>

#define CLBPT_PACKET_SEARCH(x) ((( (clbpt_packet)(x) << 32 ) & 0x7FFFFFFF00000000 ) | 0x7FFFFFFF )
#define CLBPT_PACKET_RANGE(x,y) ((( (clbpt_packet)(x)  << 32 ) & 0xFFFFFFFF00000000 ) | ( (uint32_t)(y) | 0x80000000 ) )
#define CLBPT_PACKET_INSERT(x,y) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ) | (uint32_t)(y) )
#define CLBPT_PACKET_DELETE(x) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ))

int clbptWaitExcuteBufferEmpty(clbpt_tree tree)
{
    while( tree->buf_status != CLBPT_STATUS_DONE )
    {
        /*Slow down*/
    }
    return CLBPT_SUCCESS;
}

int clbptBufferExchange(clbpt_tree tree)
{
    int err = clbptWaitExcuteBufferEmpty(tree);
    if( err != CLBPT_SUCCESS ) return err;
    memcpy( tree->execute_buf , tree->fetch_buf , sizeof(clbpt_packet)*tree->buf_size );
    memset( tree->fetch_buf , 0 , sizeof(clbpt_packet)*tree->buf_size );
    tree->buf_status = CLBPT_STATUS_WAIT;
    return CLBPT_SUCCESS;
}

int clbptEnqueueFecthBuffer(clbpt_tree tree, clbpt_packet packet, void *records)
{
    if( tree->fetch_buf_index >= tree->buf_size )
    {
        clbptBufferExchange(tree);
    }
    tree->fetch_buf[ tree->fetch_buf_index ] = packet;
    tree->result_buf[ tree->fetch_buf_index++ ] = records;
    return CLBPT_SUCCESS;
}

int clbptCreatePlatform(clbpt_platform dst_platform, cl_context context)
{
    int err = CLBPT_SUCCESS;
    dst_platform = malloc(sizeof(struct _clbpt_platform));
    dst_platform->context = context;
    dst_platform->queue = clCreateCommandQueue(context, /*TO DO*/ , /*TO DO*/ , &err );
    if( err != CLBPT_SUCCESS ) return err;
	return CLBPT_SUCCESS;
}

int clbptCreateTree(clbpt_tree dst_tree, clbpt_platform platform, const int degree, const size_t record_size)
{
    dst_tree = malloc(sizeof(struct _clbpt_tree));
    dst_tree->platform = platform;
    dst_tree->degree = degree;
    dst_tree->record_size = record_size;
    dst_tree->buf_status = CLBPT_STATUS_DONE;
    dst_tree->buf_size = buf_size;
    dst_tree->fetch_buf = calloc(sizeof(clbpt_packet),buf_size);
    dst_tree->execute_buf = calloc(sizeof(clbpt_packet),buf_size);
    dst_tree->result_buf = calloc(sizeof(void *),buf_size);
    return CLBPT_SUCCESS;
}

int clbptReleaseTree(clbpt_tree tree)
{
    if(tree == NULL ) return CLBPT_SUCCESS;
    if(tree->fetch_buf != NULL ) free(tree->fetch_buf);
    if(tree->execute_buf != NULL ) free(tree->execute_buf);
    if(tree->result_buf != NULL ) free(tree->result_buf);
    free(tree);
    return CLBPT_SUCCESS;
}

int clbptEnqueueSearches(clbpt_tree tree, int num_keys, CLBPT_KEY_TYPE *keys, void *records)
{
	int i, err;
	for( i = 0 ; i < num_keys ; i++ )
	{
		err = clEnqueueFetchBuffer(tree, CLBPT_PACKET_SEARCH(keys[i]), records);
        if( err != CLBPT_SUCCESS ) return err;
	}
    return CLBPT_SUCCESS;
}

int clbptEnqueueRangeSearches(clbpt_tree tree, int num_keys, CLBPT_KEY_TYPE *l_keys, CLBPT_KEY_TYPE *u_keys, void **record_list)
{
	int i, err;
	for( i = 0 ; i < num_keys ; i++ )
	{
		err = clEnqueueFetchBuffer(tree, CLBPT_PACKET_RANGE(l_keys[i],u_keys[i]), (void *)record_list);
        if( err != CLBPT_SUCCESS ) return err;
	}
    return CLBPT_SUCCESS;
}

int clbptEnqueueInsertions(clbpt_tree tree, int num_inserts, CLBPT_KEY_TYPE *keys, void *records)
{
	int i, err;
	for( i = 0 ; i < num_inserts ; i++ )
	{
		err = clEnqueueFetchBuffer(tree, CLBPT_PACKET_INSERT(keys[i],0),records);
        if( err != CLBPT_SUCCESS ) return err;
	}
    return CLBPT_SUCCESS;
}

int clbptEnqueueDeletions(clbpt_tree tree, int num_deletes, CLBPT_KEY_TYPE *keys)
{
	int i, err;
	for( i = 0 ; i < num_deletes ; i++ )
	{
		err = clEnqueueFetchBuffer(tree, CLBPT_PACKET_DELETE(keys[i]),NULL);
        if( err != CLBPT_SUCCESS ) return err;
	}
    return CLBPT_SUCCESS;
}

int clbptFlush(clbpt_tree tree)
{
    int err = clbptBufferExchange(tree);
    if( err != CLBPT_SUCCESS ) return err;
    return CLBPT_SUCCESS;
}

int clbptFinish(clbpt_tree tree)
{
    int err = clbptFlush(tree);
    if( err != CLBPT_SUCCESS ) return err;
    err = clbptWaitExcuteBufferEmpty(tree);
    if( err != CLBPT_SUCCESS ) return err;
    return CLBPT_SUCCESS;
}
