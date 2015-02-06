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

#define CLBPT_BUF_SIZE 65536

#define CLBPT_STATUS_DONE 0
#define CLBPT_STATUS_WAIT 1

struct _clbpt_platform {
	cl_context context;
	cl_command_queue queue;
	cl_program program;
	cl_kernel *kernels;
};
	
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
    pthread_mutex_t mutex;
	
    int buf_status;
<<<<<<< HEAD
    int fetch_buf_index;
	//const int buf_size = 65536;
=======
	size_t buf_size;
	int fetch_buf_index;
>>>>>>> 1b076f3bcea84bc0b7f923be8ae11989eaf8fa87
	clbpt_packet *fetch_buf;
	clbpt_packet *wait_buf;
	clbpt_packet *execute_buf;
	void **result_buf;
	void **wait_result_buf;
	
	int degree;
	size_t record_size;
	cl_command_queue queue;
	cl_mem heap;
	clbpt_leaf_node *leaf;
	clbpt_node *root;
};

typedef struct _clbpt_tree * clbpt_tree;
