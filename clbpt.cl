#include "kma.cl"

typedef ulong clbpt_entry;
 
typedef struct _clbpt_node {
	clbpt_entry *entry;
	uint num_entry;
} clbpt_node;

typedef struct _clbpt_leaf_node {
	struct _entry {
		uchar enable;
		void *record_ptr;
		struct _entry *next;
	} *entry;
	struct _entry *head;
	uint num_entry;
} clbpt_leaf_node;

typedef	ulong clbpt_packet;

#define getKeyFromPacket(X) (int)(((X) >> 31) & 0x80000000 | ((X) >> 32) & 0x7FFFFFFF)

__kernel void
clbptPacketSort(
			__global clbpt_packet *query,
			__global ulong *result_addr
			)
{
	size_t gid = get_global_id(0);
	size_t l_size = get_local_size(0);
	uint bitonic_max_level, offset, local_offset;
	size_t exchg_index;
	clbpt_packet temp1, temp2;
	ulong temp_addr;
	__local clbpt_packet query_local[MAX_LOCAL_SIZE];
	__local ulong result_addr_local[MAX_LOCAL_SIZE];
	event_t copy_event[2];
	
	if (get_num_groups(0) == 1) {	// Bitonic Sort
		// Copy to local
		copy_event[0] = async_work_group_copy(query_local, query, l_size, 0);
		copy_event[1] = async_work_group_copy(result_addr_local, result_addr, l_size, 0);
		wait_group_events(2, copy_event);
		
		// Sort
		for (bitonic_max_level = 1; bitonic_max_level < l_size; bitonic_max_level <<= 1) {
			if ((local_offset = gid & ((bitonic_max_level << 1) - 1)) < bitonic_max_level) {
			
				if (getKeyFromPacket(temp1 = query_local[gid]) > 
					getKeyFromPacket(temp2 = query_local[exchg_index = gid + ((bitonic_max_level - local_offset) << 1) - 1]) &&
					exchg_index < l_size) 
				{
					query_local[gid] = temp2;
					query_local[exchg_index] = temp1;
					temp_addr = result_addr_local[gid];
					result_addr_local[gid] = result_addr_local[exchg_index];
					result_addr_local[exchg_index] = temp_addr;
				}
			}
			barrier(CLK_LOCAL_MEM_FENCE);
			for (offset = (bitonic_max_level >> 1); offset != 0; offset >>= 1) {
				if ((gid & ((offset << 1) - 1)) < offset) {
					if (getKeyFromPacket(temp1 = query_local[gid]) > getKeyFromPacket(temp2 = query_local[exchg_index = gid + offset]) &&
						exchg_index < l_size)
					{
						query_local[gid] = temp2;
						query_local[exchg_index] = temp1;
						temp_addr = result_addr_local[gid];
						result_addr_local[gid] = result_addr_local[exchg_index];
						result_addr_local[exchg_index] = temp_addr;
					}
				}
				barrier(CLK_LOCAL_MEM_FENCE);
			}
		}
		
		// Copy to global
		copy_event[0] = async_work_group_copy(query, query_local, l_size, 0);
		copy_event[1] = async_work_group_copy(result_addr, result_addr_local, l_size, 0);
		wait_group_events(2, copy_event);
	}
	else {							// Quick Sort Partition
		
	}
}