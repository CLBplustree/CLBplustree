/**
 * @file Type definitions of CLBPT.
 */
 
#ifndef __CLBPT_TYPE_H_INCLUDED
#define __CLBPT_TYPE_H_INCLUDED

#include <stdint.h>
#include <pthread.h>
#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#define CLBPT_KEY_TYPE int

#define CLBPT_BUF_SIZE 65536

#define CLBPT_STATUS_DONE 0
#define CLBPT_STATUS_WAIT 1
#define CLBPT_PACKET_SELECT 0
#define CLBPT_PACKET_SORT 1
#define CLBPT_INITIALIZE 2
#define CLBPT_SEARCH 3
#define CLBPT_INIT_WPACKET_BUFFER 4
#define CLBPT_WPACKET_BUFFER_HANDLER 5
#define CLBPT_WPACKET_SUPER_GROUP_HANDLER 6

struct _clbpt_platform {
	cl_context context;
	cl_command_queue queue;
	cl_program program;
	cl_kernel *kernels;
	cl_device_id *devices;
};

typedef struct _clbpt_property {
	void *root;
	uint32_t level;
} clbpt_property;

typedef uint64_t clbpt_entry;
 
typedef struct _clbpt_int_node {
	clbpt_entry *entry;
	uint32_t num_entry;
} clbpt_node;

typedef struct _clbpt_leaf_node {
	struct _entry {
		uint8_t enable;
		void *record_ptr;
		struct _entry *next;
	} *entry;
	struct _entry *head;
	uint32_t num_entry;
} clbpt_leaf_node;

typedef	uint64_t clbpt_packet;

typedef struct _clbpt_platform * clbpt_platform;
 
struct _clbpt_tree {
	clbpt_platform platform;
	pthread_t *handler;
    pthread_mutex_t buffer_mutex;
    pthread_mutex_t loop_mutex;

    int buf_status;
    int fetch_buf_index;
	clbpt_packet *fetch_buf;
	clbpt_packet *wait_buf;
	clbpt_packet *execute_buf;
	void **result_buf;
	void **execute_result_buf;

	int degree;
	size_t record_size;
	cl_command_queue queue;

	clbpt_property property;
	cl_mem heap;
	clbpt_leaf_node *leaf;
	clbpt_node *root;
};

typedef struct _clbpt_tree * clbpt_tree;

#endif
