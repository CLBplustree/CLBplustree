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
#define CLBPT_RECORD_TYPE int

// Temporary. Replace this by compiler option later.
#define CLBPT_ORDER 128	// Should be less than or equal to half
						// of MAX_LOCAL_SIZE

//#define CLBPT_BUF_SIZE 65536
#define CLBPT_BUF_SIZE 20

#define CLBPT_STATUS_DONE 0
#define CLBPT_STATUS_WAIT 1

// KERNEL
#define NUM_KERNELS 9
#define CLBPT_PACKET_SELECT 0
#define CLBPT_PACKET_SORT 1
#define CLBPT_INITIALIZE 2
#define CLBPT_SEARCH 3
#define CLBPT_WPACKET_BUFFER_HANDLER 4
#define CLBPT_WPACKET_BUFFER_ROOT_HANDLER 5
#define CLBPT_WPACKET_INIT 6
#define CLBPT_WPACKET_COMPACT 7
#define CLBPT_WPACKET_SUPER_GROUP_HANDLER 8

struct _clbpt_platform {
	cl_context context;
	cl_command_queue queue;
	cl_command_queue queue_device;
	cl_program program;
	cl_kernel *kernels;
	cl_device_id *devices;
	int num_devices;
};

typedef struct _clbpt_property {
	void *root;
	uint32_t level;
} clbpt_property;

//typedef uint64_t clbpt_entry;
typedef struct _clbpt_entry {
	int32_t key;
	void *child;
} clbpt_entry;
 
typedef struct _clbpt_int_node {
	clbpt_entry entry[CLBPT_ORDER];
	uint32_t num_entry;
	uint32_t *parent;
	uint32_t parent_key;
} clbpt_int_node;

typedef struct _clbpt_leafmirror {
	clbpt_int_node *parent;
	void *leaf;
} clbpt_leafmirror;

typedef struct _clbpt_leaf_entry {
	void *record_ptr;
	struct _clbpt_leaf_entry *next;
} clbpt_leaf_entry;

typedef struct _clbpt_leaf_node {
	clbpt_leaf_entry *head;
	uint32_t num_entry;
	struct _clbpt_leaf_node *prev_node;
	struct _clbpt_leaf_node *next_node;
	clbpt_leafmirror *mirror;
	uint32_t parent_key;
} clbpt_leaf_node;

typedef struct _clbpt_ins_pkt {
	clbpt_int_node *target;
	clbpt_entry entry;
} clbpt_ins_pkt;

typedef struct _clbpt_del_pkt {
	clbpt_int_node *target;
	int32_t key;
} clbpt_del_pkt;

typedef	uint64_t clbpt_packet;

typedef struct _clbpt_platform *clbpt_platform;
 
struct _clbpt_tree {
	clbpt_platform platform;
	pthread_t handler;
	pthread_mutex_t buffer_mutex;
	pthread_mutex_t loop_mutex;

	int buf_status;
	int fetch_buf_index;
	int wait_buf_index;
	clbpt_packet *fetch_buf;
	clbpt_packet *wait_buf;
	clbpt_packet *execute_buf;
	void **result_buf;
	void **execute_result_buf;
	void **node_addr_buf;

	int close_thread;
	int is_Complete;

	int degree;
	size_t record_size;

	clbpt_property property;
	cl_mem heap;
	void **heap_svm_ptr;
	clbpt_leaf_node *leaf;
	void *root;
};

typedef struct _clbpt_tree *clbpt_tree;

#endif
