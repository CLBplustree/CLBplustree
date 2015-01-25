//#include "kma.cl"

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
			__global ulong *result_addr,
			__global clbpt_packet *query_temp,
			__global ulong *result_addr_temp,
			uint num_query
			)
{
	
	const size_t gid = get_global_id(0);
	const size_t lid = get_local_id(0);
	const size_t l_size = get_local_size(0);
	//__local clbpt_packet query_local[MAX_LOCAL_SIZE];
	//__local ulong result_addr_local[MAX_LOCAL_SIZE];
	event_t copy_event[2];
	uint less_count = 0, greater_count = 0, equal_count = 0;
	uint less_index, greater_index, equal_index;
	uint equal_index_init;
	
	if (num_query < l_size) {	// Bitonic Sort
		uint bitonic_max_level, offset, local_offset;
		size_t exchg_index;
		clbpt_packet temp1, temp2;
		ulong temp_addr;
		
		/*
		// Copy to local
		copy_event[0] = async_work_group_copy(query_local, query, num_query, 0);
		copy_event[1] = async_work_group_copy(result_addr_local, result_addr, num_query, 0);
		wait_group_events(2, copy_event);
		*/
		
		// Sort
		for (bitonic_max_level = 1; bitonic_max_level < num_query; bitonic_max_level <<= 1) {
			if ((local_offset = gid & ((bitonic_max_level << 1) - 1)) < bitonic_max_level) {
			
				if (getKeyFromPacket(temp1 = query[gid]) > 
					getKeyFromPacket(temp2 = query[exchg_index = gid + ((bitonic_max_level - local_offset) << 1) - 1]) &&
					exchg_index < num_query) 
				{
					query[gid] = temp2;
					query[exchg_index] = temp1;
					temp_addr = result_addr[gid];
					result_addr[gid] = result_addr[exchg_index];
					result_addr[exchg_index] = temp_addr;
				}
			}
			work_group_barrier(CLK_LOCAL_MEM_FENCE);
			for (offset = (bitonic_max_level >> 1); offset != 0; offset >>= 1) {
				if ((gid & ((offset << 1) - 1)) < offset) {
					if (getKeyFromPacket(temp1 = query[gid]) > getKeyFromPacket(temp2 = query[exchg_index = gid + offset]) &&
						exchg_index < num_query)
					{
						query[gid] = temp2;
						query[exchg_index] = temp1;
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
		copy_event[0] = async_work_group_copy(query, query_local, num_query, 0);
		copy_event[1] = async_work_group_copy(result_addr, result_addr_local, num_query, 0);
		wait_group_events(2, copy_event);
		*/
		
	}
	else {							// Quick Sort Partition
	
		const int pivot = getKeyFromPacket(query[num_query >> 1]);
		
		for (int i = (num_query * lid) / l_size; i != (num_query * (lid + 1)) / l_size; i++) {
			if (query[i] < pivot) {
				less_count++;
			} 
			if (query[i] > pivot) {
				greater_count++;
			}
			if (query[i] == pivot) {
				equal_count++;
			}
		}
		
		less_index = work_group_scan_exclusive_add(less_count);
		greater_index = work_group_scan_inclusive_add(greater_count);
		if (gid == l_size - 1) {
			equal_index_init = greater_index + less_count;
		}
		work_group_barrier(CLK_LOCAL_MEM_FENCE);
		equal_index_init = work_group_broadcast(equal_index_init, l_size -1);
		equal_index = work_group_scan_exclusive_add(equal_count);
		work_group_barrier(CLK_LOCAL_MEM_FENCE);
		equal_index += equal_index_init;
		
		for (int i = (num_query * lid) / l_size; i != (num_query * (lid + 1)) / l_size; i++) {
			if (query[i] < pivot) {
				query_temp[less_index] = query[i];
				result_addr_temp[less_index++] = result_addr[i];
			} 
			if (query[i] > pivot) {
				query_temp[greater_index] = query[i];
				result_addr_temp[greater_index++] = result_addr[i];
			}
			if (query[i] == pivot) {
				query_temp[equal_index] = query[i];
				result_addr_temp[equal_index++] = result_addr[i];
			}
		}
		work_group_barrier(CLK_LOCAL_MEM_FENCE);
		
		/*
		for (int i = 0; i <= num_query - MAX_LOCAL_SIZE; i += MAX_LOCAL_SIZE) {
			copy_event[0] = async_work_group_copy(query_local, query_temp, MAX_LOCAL_SIZE, 0);
			copy_event[1] = async_work_group_copy(result_addr_local, result_addr_temp, MAX_LOCAL_SIZE, 0);
			wait_group_events(2, copy_event);
			copy_event[0] = async_work_group_copy(query, query_local, MAX_LOCAL_SIZE, 0);
			copy_event[1] = async_work_group_copy(result_addr, result_addr_local, MAX_LOCAL_SIZE, 0);
			wait_group_events(2, copy_event);
		}
		copy_event[0] = async_work_group_copy(query_local, query_temp, num_query % MAX_LOCAL_SIZE, 0);
		copy_event[1] = async_work_group_copy(result_addr_local, result_addr_temp, num_query % MAX_LOCAL_SIZE, 0);
		wait_group_events(2, copy_event);
		copy_event[0] = async_work_group_copy(query, query_local, num_query % MAX_LOCAL_SIZE, 0);
		copy_event[1] = async_work_group_copy(result_addr, result_addr_local, num_query % MAX_LOCAL_SIZE, 0);
		wait_group_events(2, copy_event);
		*/
		
		for (int i = (num_query * lid) / l_size; i != (num_query * (lid + 1)) / l_size; i++) {
			query[i] = query_temp[i];
			result_addr[i] = result_addr_temp[i];
		}
		if (gid == l_size - 1) {
		
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_NO_WAIT,
				ndrange_1D(l_size, l_size),
				^{
					clbptPacketSort(query, result_addr, query_temp, result_addr_temp, less_index);
				 }
			);
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_NO_WAIT,
				ndrange_1D(l_size, l_size),
				^{
					clbptPacketSort(query + equal_index, result_addr + equal_index, query_temp + equal_index, result_addr_temp + equal_index, greater_index - equal_index);
				 }
			);
		}
		
	}
}