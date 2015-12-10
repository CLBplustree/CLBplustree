#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable

struct clheap {
	ulong size;
	uintptr_t head;
	uintptr_t free;
};

__kernel void
initializeClheap(__global void *heap, ulong size)
{
	struct clheap *header = heap;

	header->size = size;
	header->head = header->free = (uintptr_t)(heap + sizeof(struct clheap));
}

void *
malloc(__global void *heap, ulong size)
{
	struct clheap *header = heap;

	if ((ulong)(header->free) - (ulong)heap + size > header->size)
		return NULL;

	void *result;
	result = (void *)atom_add((volatile __global ulong *)(&(header->free)), 
		size);

	return result;
}

void
free(__global void *heap, __global void *target)
{

}
