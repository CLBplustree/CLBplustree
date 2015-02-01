/*
	Issues:		1. Packet select: isOver
*/

#include "kma.cl"

// Temporary. Replace this by compiler option latter.
#define CLBPT_ORDER 8
#define CPU_BITNESS 64

#if CPU_BITNESS == 32
typedef uint cpu_address_t;
#else
typedef ulong cpu_address_t;
#endif

typedef struct _clbpt_entry {
	uint key;
	uintptr_t child;
} clbpt_entry;
 
typedef struct _clbpt_int_node {
	clbpt_entry entry[CLBPT_ORDER];
	uint num_entry;
	uintptr_t parent;
} clbpt_int_node;

typedef	ulong clbpt_packet;

#define getKeyFromPacket(X) (int)(((X) >> 31) & 0x80000000 | ((X) >> 32) & 0x7FFFFFFF)
#define PACKET_NOP (0x3FFFFFFF00000000L)
#define isReadPacket(X) (!((uchar)((X) >> 63) & 0x1))
#define isWritePacket(X) ((uchar)((X) >> 63) & 0x1)
#define isNopPacket(X) ((X) == PACKET_NOP)
#define isRangePacket(X) ((!((uchar)((X) >> 63) & 0x1)) && ((uchar)((X) >> 31) & 0x1))
#define isSearchPacket(X) ((!((uchar)((X) >> 63) & 0x1)) && ((uint)(X) == 0x7FFFFFFF))
#define isInsertPacket(X) (((uchar)((X) >> 63) & 0x1) && ((uint)(X) != 0))
#define isDeletePacket(X) (((uchar)((X) >> 63) & 0x1) && ((uint)(X) == 0))
#define getUpperKeyFromRangePacket(X) (int)(((X) << 1) & 0x80000000 | (X) & 0x7FFFFFFF)
#define getKeyFromEntry(X) (int)(((X.key) << 1) & 0x80000000 | (X.key) & 0x7FFFFFFF)
#define getChildFromEntry(X) (X.child)

__kernel void
clbptPacketSort(
			__global clbpt_packet *execute,
			__global cpu_address_t *result_addr,
			__global clbpt_packet *execute_temp,
			__global cpu_address_t *result_addr_temp,
			uint num_execute
			)
{
	
	const size_t gid = get_global_id(0);
	const size_t lid = get_local_id(0);
	const size_t l_size = get_local_size(0);
	//__local clbpt_packet execute_local[MAX_LOCAL_SIZE];
	//__local cpu_address_t result_addr_local[MAX_LOCAL_SIZE];
	event_t copy_event[2];
	uint less_count = 0, greater_count = 0, equal_count = 0;
	uint less_index, greater_index, equal_index;
	uint equal_index_init;
	
	if (num_execute < l_size) {	// Bitonic Sort
		uint bitonic_max_level, offset, local_offset;
		size_t exchg_index;
		clbpt_packet temp1, temp2;
		cpu_address_t temp_addr;
		
		/*
		// Copy to local
		copy_event[0] = async_work_group_copy(execute_local, execute, num_execute, 0);
		copy_event[1] = async_work_group_copy(result_addr_local, result_addr, num_execute, 0);
		wait_group_events(2, copy_event);
		*/
		
		// Sort
		for (bitonic_max_level = 1; bitonic_max_level < num_execute; bitonic_max_level <<= 1) {
			if ((local_offset = gid & ((bitonic_max_level << 1) - 1)) < bitonic_max_level) {
			
				if (getKeyFromPacket(temp1 = execute[gid]) > 
					getKeyFromPacket(temp2 = execute[exchg_index = gid + ((bitonic_max_level - local_offset) << 1) - 1]) &&
					exchg_index < num_execute) 
				{
					execute[gid] = temp2;
					execute[exchg_index] = temp1;
					temp_addr = result_addr[gid];
					result_addr[gid] = result_addr[exchg_index];
					result_addr[exchg_index] = temp_addr;
				}
			}
			work_group_barrier(CLK_LOCAL_MEM_FENCE);
			for (offset = (bitonic_max_level >> 1); offset != 0; offset >>= 1) {
				if ((gid & ((offset << 1) - 1)) < offset) {
					if (getKeyFromPacket(temp1 = execute[gid]) > getKeyFromPacket(temp2 = execute[exchg_index = gid + offset]) &&
						exchg_index < num_execute)
					{
						execute[gid] = temp2;
						execute[exchg_index] = temp1;
						temp_addr = result_addr[gid];
						result_addr[gid] = result_addr[exchg_index];
						result_addr[exchg_index] = temp_addr;
					}
				}
				work_group_barrier(CLK_LOCAL_MEM_FENCE);
			}
		}
		
		/*
		// Copy to global
		copy_event[0] = async_work_group_copy(execute, execute_local, num_execute, 0);
		copy_event[1] = async_work_group_copy(result_addr, result_addr_local, num_execute, 0);
		wait_group_events(2, copy_event);
		*/
		
	}
	else {							// Quick Sort Partition
	
		const int pivot = getKeyFromPacket(execute[num_execute >> 1]);
		
		for (int i = (num_execute * lid) / l_size; i != (num_execute * (lid + 1)) / l_size; i++) {
			if (getKeyFromPacket(execute[i]) < pivot) {
				less_count++;
			} 
			if (getKeyFromPacket(execute[i]) > pivot) {
				greater_count++;
			}
			if (getKeyFromPacket(execute[i]) == pivot) {
				equal_count++;
			}
		}
		
		less_index = work_group_scan_exclusive_add(less_count);
		equal_index = work_group_scan_exclusive_add(equal_count);
		greater_index = work_group_scan_inclusive_add(greater_count);
		if (gid == l_size - 1) {
			equal_index_init = less_index + less_count;
		}
		work_group_barrier(CLK_LOCAL_MEM_FENCE);
		greater_index = num_execute - greater_index;
		equal_index_init = work_group_broadcast(equal_index_init, l_size - 1);
		work_group_barrier(CLK_LOCAL_MEM_FENCE);
		equal_index += equal_index_init;
		work_group_barrier(CLK_LOCAL_MEM_FENCE);
		
		for (int i = (num_execute * lid) / l_size; i != (num_execute * (lid + 1)) / l_size; i++) {
			if (getKeyFromPacket(execute[i]) < pivot) {
				execute_temp[less_index] = execute[i];
				result_addr_temp[less_index++] = result_addr[i];
			} 
			if (getKeyFromPacket(execute[i]) > pivot) {
				execute_temp[greater_index] = execute[i];
				result_addr_temp[greater_index++] = result_addr[i];
			}
			if (getKeyFromPacket(execute[i]) == pivot) {
				execute_temp[equal_index] = execute[i];
				result_addr_temp[equal_index++] = result_addr[i];
			}
		}
		work_group_barrier(CLK_LOCAL_MEM_FENCE);
		
		/*
		for (int i = 0; i <= num_execute - MAX_LOCAL_SIZE; i += MAX_LOCAL_SIZE) {
			copy_event[0] = async_work_group_copy(execute_local, execute_temp, MAX_LOCAL_SIZE, 0);
			copy_event[1] = async_work_group_copy(result_addr_local, result_addr_temp, MAX_LOCAL_SIZE, 0);
			wait_group_events(2, copy_event);
			copy_event[0] = async_work_group_copy(execute, execute_local, MAX_LOCAL_SIZE, 0);
			copy_event[1] = async_work_group_copy(result_addr, result_addr_local, MAX_LOCAL_SIZE, 0);
			wait_group_events(2, copy_event);
		}
		copy_event[0] = async_work_group_copy(execute_local, execute_temp, num_execute % MAX_LOCAL_SIZE, 0);
		copy_event[1] = async_work_group_copy(result_addr_local, result_addr_temp, num_execute % MAX_LOCAL_SIZE, 0);
		wait_group_events(2, copy_event);
		copy_event[0] = async_work_group_copy(execute, execute_local, num_execute % MAX_LOCAL_SIZE, 0);
		copy_event[1] = async_work_group_copy(result_addr, result_addr_local, num_execute % MAX_LOCAL_SIZE, 0);
		wait_group_events(2, copy_event);
		*/
		
		for (int i = (num_execute * lid) / l_size; i != (num_execute * (lid + 1)) / l_size; i++) {
			execute[i] = execute_temp[i];
			result_addr[i] = result_addr_temp[i];
		}
		if (gid == l_size - 1) {
			if (less_index > 0) {
				enqueue_kernel(
					get_default_queue(),
					CLK_ENQUEUE_FLAGS_NO_WAIT,
					ndrange_1D(l_size, l_size),
					^{
						clbptPacketSort(execute, result_addr, execute_temp, result_addr_temp, less_index);
					 }
				);
			}
			if (num_execute - equal_index > 0) {
				enqueue_kernel(
					get_default_queue(),
					CLK_ENQUEUE_FLAGS_NO_WAIT,
					ndrange_1D(l_size, l_size),
					^{
						clbptPacketSort(execute + equal_index, result_addr + equal_index, execute_temp + equal_index, result_addr_temp + equal_index, num_execute - equal_index);
					 }
				);
			}
		}
	}
}

__kernel void
clbptPacketSelect(
	__global uchar *isOver,
	__global clbpt_packet *wait,
	__global clbpt_packet *execute,
	__const uint buffer_size
	)
{
	const uint gid = get_global_id(0);
	const uint grid = get_group_id(0);
	const uint lsize = get_local_size(0);
	
	if (gid >= buffer_size) return;
	
	clbpt_packet pkt = wait[gid];
	
	if (isNopPacket(pkt)) {
		execute[gid] = PACKET_NOP;
		return;
	}
	
	uchar isRange = isRangePacket(pkt);
	int key = getKeyFromPacket(pkt);
	int ukey = getUpperKeyFromRangePacket(pkt);
	clbpt_packet prev_pkt;
	int i;
	
	*isOver = 0;
	for (i = gid - 1; i >= grid * lsize; i--) {
		if (isWritePacket(prev_pkt = wait[i])) {
			int prev_key = getKeyFromPacket(prev_pkt);
			if (isRange) {
				if (prev_key >= key && prev_key <= ukey) {
					break;
				}
			}
			else {
				if (prev_key == key) {
					break;
				}
			}
		}
	}
	work_group_barrier(0);
	if (i == grid * lsize - 1) {
		for (; i >= 0; i--) {
			if (isWritePacket(prev_pkt = wait[i])) {
				int prev_key = getKeyFromPacket(prev_pkt);
				if (isRange) {
					if (prev_key >= key && prev_key <= ukey) {
						break;
					}
				}
				else {
					if (prev_key == key) {
						break;
					}
				}
			}
		}
	}
	if (i < 0) {
		execute[gid] = pkt;
		wait[gid] = PACKET_NOP;
	}
	else {
		execute[gid] = PACKET_NOP;
	}
}


/*
__kernel void
clbptPacketSelect(
	__global uchar *isOver,
	__global clbpt_packet *wait,
	__global clbpt_packet *select,
	__const uint buffer_size
)
{
	const uint lid = get_local_id(0);
	const uint lsize = get_local_size(0);
	clbpt_packet pkt;
	int pkt_key;
	uint i, j;
	int isOver_private = 1;
	
	for (i = 0; i < buffer_size; i += lsize) {
		if (i + lid < buffer_size) {
			select[i + lid] = 1;
		}
	}
	work_group_barrier(0);
	for (i = 0; i < buffer_size; i += lsize) {
		if (i + lid < buffer_size) {
			pkt = wait[i + lid];
			if (isNopPacket(pkt)) {
				select[i + lid] = 0;
			}
			else if (isWritePacket(pkt)) {
				pkt_key = getKeyFromPacket(pkt);
				for (j = i + lid + 1; j < i + lsize; j++) {
					clbpt_packet latter_pkt = wait[j];
					if (isRangePacket(latter_pkt)) {
						if (pkt_key >= getKeyFromPacket(latter_pkt) && pkt_key <= getUpperKeyFromRangePacket(latter_pkt)) {
							select[j] = 0;
						}
					}
					else {
						if (pkt_key == getKeyFromPacket(latter_pkt)) {
							select[j] = 0;
						}
					}
				}
				work_group_barrier(0);
				for (j = i + lsize; j < buffer_size; j++) {
					clbpt_packet latter_pkt = wait[j];
					if (isRangePacket(latter_pkt)) {
						if (pkt_key >= getKeyFromPacket(latter_pkt) && pkt_key <= getUpperKeyFromRangePacket(latter_pkt)) {
							select[j] = 0;
						}
					}
					else {
						if (pkt_key == getKeyFromPacket(latter_pkt)) {
							select[j] = 0;
							if (isWritePacket(latter_pkt))
								break;
						}
					}
				}
			}
		}
	}
	work_group_barrier(CLK_GLOBAL_MEM_FENCE);
	for (i = 0; i < buffer_size; i += lsize) {
		if (i + lid < buffer_size) {
			if (select[i + lid]) {
				select[i + lid] = wait[i + lid];
				wait[i + lid] = PACKET_NOP;
				isOver_private = 0;
			}
			else {
				select[i + lid] = PACKET_NOP;
			}
		}
	}
	work_group_barrier(0);
	*isOver = (uchar)work_group_all(isOver_private);
}*/

__kernel void
_clbptInitialize (
	__global uint *level
)
{
	*level = 1;
}

__kernel void
_clbptSearch(
	__global cpu_address_t *result,
	__global uintptr_t *root_node,
	__global uint *level,
	__global clbpt_packet *execute,
	uint buffer_size)
{
	uint gid = get_global_id(0);
	int key;
	uint index_entry_high, index_entry_low, index_entry_mid;
	clbpt_int_node *node;
	
	if (gid >= buffer_size) return;
	if (*level == 1) {
		result[gid] = 0;	// Root node at host side
		return;
	}
	
	key = getKeyFromPacket(execute[gid]);
	node = (clbpt_int_node *)(*root_node);
	for (int i = 0; i < *level - 1; i++) {
		index_entry_low = 0;
		index_entry_high = node->num_entry;
		for (;;) {
			index_entry_mid = (index_entry_low + index_entry_high) / 2;
			if (key < getKeyFromEntry(node->entry[index_entry_mid])) {
				index_entry_high = index_entry_mid - 1;
			}
			else if (key >= getKeyFromEntry(node->entry[index_entry_mid + 1])) {
				index_entry_low = index_entry_mid + 1;
			}
			else {
				break;
			}
		}
		node = (clbpt_int_node *)getChildFromEntry(node->entry[index_entry_mid]);
	}
	result[gid] = *((cpu_address_t *)node);
}
	
	