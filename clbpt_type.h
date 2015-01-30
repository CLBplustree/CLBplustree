/**
 * @file Type definitions of CLBPT.
 */
 
#include <stdint.h>
#include <pthread.h>
#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#define CLBPT_KEY_TYPE int

struct _clbpt_platform {
	cl_context context;
	cl_command_queue queue;
	cl_program program;
};
	
typedef uint64_t clbpt_entry;
 
typedef struct _clbpt_node {
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
	
    int buf_status;
    int fetch_buf_index;
	size_t buf_size;
	clbpt_packet *fetch_buf;
	clbpt_packet *execute_buf;
	void **result_buf;
	
	int degree;
	size_t record_size;
	cl_command_queue queue;
	cl_mem heap;
	clbpt_leaf_node *leaf;
	clbpt_node *root;
};

typedef struct _clbpt_tree * clbpt_tree;
