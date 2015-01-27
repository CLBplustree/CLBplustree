/**
 * @file The front-end source file
 */
 
#include "clbpt.h"

#define CLBPT_PACKET_SEARCH(x) ((( (clbpt_packet)(x) << 32 ) & 0x7FFFFFFF00000000 ) | 0x7FFFFFFF )
#define CLBPT_PACKET_RANGE(x,y) ((( (clbpt_packet)(x)  << 32 ) & 0xFFFFFFFF00000000 ) | ( (uint32_t)(y) | 0x80000000 ) )
#define CLBPT_PACKET_INSERT(x,y) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ) | (uint32_t)(y) )
#define CLBPT_PACKET_DELETE(x) (((( (clbpt_packet)(x) | 0x80000000 ) << 32 ) & 0xFFFFFFFF00000000 ))

int clbptBufferExchange(clbpt_tree tree)
{
    clbptWaitExcuteBufferEmpty(tree);
    memcpy( execute_buf , fetch_buf , sizeof(clbpt_packet)*buf_size );
    memset( fetch_buf , 0 , sizeof(clbpt_packet)*buf_size );
    return CLBPT_SUCCESS;
}

int clbptEnqueueFecthBuffer(clbpt_tree tree, clbpt_packet packet, void *records)
{
    if( tree->fecth_buf_index >= tree->buf_size )
    {
        clbptBufferExchange(tree);
    }
    tree->fetch_buf[ tree->fetch_buf_index ] = packet;
    tree->result_buf[ tree->fetch_buf_index++ ] = records;
    return CLBPT_SUCCESS;
}

int clbptCreatePlatform(clbpt_platform dst_platform, cl_context context)
{
	
}

int clbptCreateTree(clbpt_tree dst_tree, clbpt_platform platform, const int degree, const size_t record_size)
{
    dst_tree = malloc(sizeof(struct _clbpt_tree));
    dst_tree->platform = plantform;
    dst_tree->degree = degree;
    dst_tree->record_size = record_size;
    dst_tree->buf_size = buf_size;
    dst_tree->fecth_buf = calloc(sizeof(clbpt_pakcet),buf_size);
    dst_tree->execute_buf = calloc(sizeof(clbpt_pakcet),buf_size);
    dst_tree->result_buf = calloc(sizeof(void *),buf_size);
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
	for( i = 0 ; i < num_keys ; i++ )
	{
		err = clEnqueueFetchBuffer(tree, CLBPT_PACKET_SEARCH(keys[i],records),NULL);
        if( err != CLBPT_SUCCESS ) return err;
	}
    return CLBPT_SUCCESS;
}

int clbptEnqueueDeletions(clbpt_tree tree, int num_deletes, CLBPT_KEY_TYPE *keys)
{
	int i, err;
	for( i = 0 ; i < num_keys ; i++ )
	{
		err = clEnqueueFetchBuffer(tree, CLBPT_PACKET_DELETE(keys[i]),NULL);
        if( err != CLBPT_SUCCESS ) return err;
	}
    return CLBPT_SUCCESS;
}
