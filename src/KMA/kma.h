#ifndef KMA_H
#define KMA_H

#include <CL/cl.h>

int kma_create_svm(cl_device_id device, cl_context context, cl_command_queue queue, uint64_t size, void **clheap);

#endif
