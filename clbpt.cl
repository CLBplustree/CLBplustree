/*
	Issues:		1. Packet select: isOver
*/

#include "kma.cl"

// Temporary. Replace this by compiler option later.
#define CLBPT_ORDER 8
#define CPU_BITNESS 64

#if CPU_BITNESS == 32
typedef uint cpu_address_t;
#else
typedef ulong cpu_address_t;
#endif

typedef uint clbpt_key;

typedef struct _clbpt_entry {
	clbpt_key key;
	uintptr_t child;
} clbpt_entry;
 
typedef struct _clbpt_int_node {
	clbpt_entry entry[CLBPT_ORDER];
	uint num_entry;
	uintptr_t parent;
	uint parent_key;
} clbpt_int_node;

typedef	ulong clbpt_packet;

/*
typedef struct _clbpt_wpacket {
	uintptr_t target;		// 0 for nop w_packet
	uint key;
	uintptr_t new_addr;		// 0 for delete w_packet
} clbpt_wpacket;
*/

typedef clbpt_entry clbpt_ins_pkt;

typedef clbpt_key clbpt_del_pkt;

typedef struct _clbpt_property {
	uintptr_t root;
	uint level;
} clbpt_property;

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
#define ENTRY_NULL ((clbpt_entry){.child = 0})
#define isNullEntry(X) ((X).child == 0)

#define getParentKeyFromNode(X) (int)((((X).parent_key) << 1) & 0x80000000 | ((X).parent_key) & 0x7FFFFFFF)

/*
#define initDeleteWPacket(X) ((X).new_addr = 0)
#define initInsertWPacket(X, ADDR) ((X).new_addr = ADDR)
#define WPACKET_NOP ((clbpt_wpacket){.target = 0})
#define isInsertWPacket(X) ((X).new_addr != 0)
#define isDeleteWPacket(X) ((X).new_addr == 0)
#define isNopWPacket(X) ((X).target == 0)
#define getKeyFromWPacket(X) (int)(((X).key << 1) & 0x80000000 | (X).key & 0x7FFFFFFF)
*/

__kernel void
_clbptPacketSort(
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
_clbptPacketSelect(
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
_clbptPacketSelect(
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
_clbptInitialize(
	cpu_address_t host_root,
	__global clbpt_property *property,
	__global struct clheap *heap
	)
{
	property->level = 1;
	property->root = (uintptr_t)malloc(heap, sizeof(cpu_address_t));
	*(cpu_address_t *)(property->root) = host_root;
}

__kernel void
_clbptSearch(
	__global cpu_address_t *result,
	__global clbpt_property *property,
	__global clbpt_packet *execute,
	__const uint buffer_size
	)
{
	uint gid = get_global_id(0);
	int key;
	uint index_entry_high, index_entry_low, index_entry_mid;
	clbpt_int_node *node;
	
	if (gid >= buffer_size) return;
	
	key = getKeyFromPacket(execute[gid]);
	node = (clbpt_int_node *)(property->root);
	for (int i = 0; i < property->level - 1; i++) {
		index_entry_low = 0;
		index_entry_high = node->num_entry - 1;
		for (;;) {
			index_entry_mid = (index_entry_low + index_entry_high) / 2;
			if (index_entry_mid == node->num_entry - 1) {
				break;
			}
			else if (key < getKeyFromEntry(node->entry[index_entry_mid])) {
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
	
__kernel void
_clbptInitWPacketBuffer(
	__global clbpt_wpacket *wpacket,
	__global cpu_address_t *leaf_addr,	// 0 for delete w_packet
	uint buffer_size,
	__global struct clheap *heap
	)
{
	uint gid = get_global_id(0);
	int key;
	cpu_address_t addr;
	cpu_address_t *new_alloc;
	
	if (gid >= buffer_size) return;
	if ((addr = leaf_addr[gid]) == 0) {	// Delete W_packet
		initDeleteWPacket(wpacket[gid]);
	}
	else {								// Insert W_packet
		new_alloc = (cpu_address_t *)malloc(heap, sizeof(cpu_address_t));
		*new_alloc = addr;
		initInsertWPacket(wpacket[gid], (uintptr_t)new_alloc);
	}
}

void
_clbptWPacketGroupHandler(
	clbpt_wpacket *wpacket,
	uint num_wpacket_in_group,
	clbpt_wpacket *propagate,
	uint *top_propagate,
	struct clheap *heap,
	clbpt_property *property,
	uint level_proc
);
void
_clbptMerge(
	clbpt_int_node *target,
	clbpt_wpacket *propagate,
	uint *top_propagate,
	struct clheap *heap,
	clbpt_property *property,
	uint level_proc
);
void
_clbptSplit(
	clbpt_int_node *target,
	clbpt_wpacket *propagate,
	uint *top_propagate,
	struct clheap *heap,
	clbpt_property *property,
	uint level_proc
);

__kernel void
_clbptWPacketBufferHandler(
	__global clbpt_wpacket *wpacket,
	uint num_wpacket,
	__global struct clheap *heap,
	__global clbpt_property *property,
	uint level_proc
	)
{
	int wpacket_sgroup_start;
	uintptr_t prev_target, cur_target;
	uintptr_t prev_parent, cur_parent;
	uint wpacket_alloc = 0;
	uint wpacket_group_count = 1;
	
	wpacket_sgroup_start = 0;
	prev_target = wpacket[0].target;
	prev_parent = ((clbpt_int_node *)(wpacket[0].target))->parent;
	for (int i = 1; i < num_wpacket; i++) {
		cur_target = wpacket[i].target;
		cur_parent = ((clbpt_int_node *)(wpacket[i].target))->parent;
		if (cur_parent != prev_parent) {
			for (uint j = 0; j < 2 * wpacket_group_count; j++) {
				wpacket[num_wpacket + wpacket_alloc + j] = WPACKET_NOP;
			}
			// enqueue_kernel
			wpacket_sgroup_start = i;
			wpacket_alloc += 2 * wpacket_group_count;
			wpacket_group_count = 1;
			prev_target = cur_target;
			prev_parent = cur_parent;
		}
		else if (cur_target != prev_target) {
			wpacket_group_count++;
			prev_target = cur_target;
		}
	}
	for (uint j = 0; j < 2 * wpacket_group_count; j++) {
		wpacket[num_wpacket + wpacket_alloc + j] = WPACKET_NOP;
	}
	// enqueue_kernel
	
}

__kernel void
_clbptWPacketSuperGroupHandler(
	__global clbpt_wpacket *wpacket,
	uint num_wpacket_in_super_group,
	__global clbpt_wpacket *propagate,
	__global struct clheap *heap,
	__global clbpt_property *property,
	uint level_proc
	)
{
	int wpacket_group_start;
	uintptr_t prev_target, cur_target;
	uint top_propagate = 0;
	
	wpacket_group_start = 0;
	prev_target = wpacket[0].target;
	for (int i = 1; i < num_wpacket_in_super_group; i++) {
		cur_target = wpacket[i].target;
		if (cur_target != prev_target) {
			// call function
			wpacket_group_start = i;
			prev_target = cur_target;
		}
	}
	// call function
}

void
_clbptWPacketGroupHandler(
	clbpt_wpacket *wpacket,
	uint num_wpacket_in_group,
	clbpt_wpacket *propagate,
	uint *top_propagate,
	struct clheap *heap,
	clbpt_property *property,
	uint level_proc
	)
{
	clbpt_int_node *target = (clbpt_int_node *)(wpacket[0].target);
	clbpt_entry merge_entry[2 * CLBPT_ORDER];
	uint num_merge_entry = 0;
	uint index_old_entry = 0;
	uint num_old_entry = target->num_entry;
	uint index_wpacket = 0;
	clbpt_wpacket wpkt;
	
	// Deal with W-packets
	for (index_wpacket = 0; index_wpacket < num_wpacket_in_group; index_wpacket++) {
		wpkt = wpacket[index_wpacket];
		if (isNopWPacket(wpkt)) {
			continue;
		} else if (isInsertWPacket(wpkt)) {
			while (index_old_entry < num_old_entry && 
				getKeyFromEntry(target->entry[index_old_entry]) < getKeyFromWPacket(wpkt))
			{
				merge_entry[num_merge_entry++] = target->entry[index_old_entry];
				index_old_entry++;
			}
			merge_entry[num_merge_entry].key = wpkt.key;
			merge_entry[num_merge_entry].child = wpkt.new_addr;
			num_merge_entry++;
		} else {
			while (index_old_entry < num_old_entry && 
				getKeyFromEntry(target->entry[index_old_entry]) < getKeyFromWPacket(wpkt))
			{
				merge_entry[num_merge_entry++] = target->entry[index_old_entry];
				index_old_entry++;
			}
			index_old_entry++;
		}
	}
	
	// Propagate
	if (num_merge_entry == 0) {		// Merge
		//_clbptMerge(target, propagate, top_propagate, heap);
	} else if (num_merge_entry < CLBPT_ORDER) {		// No propagate
		for (int i = 0; i < num_merge_entry; i++) {
			target->entry[i] = merge_entry[i];
		}
		target->num_entry = num_merge_entry;
	} else {	// Split
		//_clbptSplit(target, propagate, top_propagate, heap);
	}
}

void
_clbptMerge(
	clbpt_int_node *target,
	clbpt_wpacket *propagate,
	uint *top_propagate,
	struct clheap *heap,
	clbpt_property *property,
	uint level_proc
	)
{
	clbpt_int_node *parent = (clbpt_int_node *)(target->parent);
	int key = getParentKeyFromNode(*target);
	uint index_entry_low, index_entry_high, index_entry_mid;
	clbpt_int_node *sibling;
	
	if (level_proc == 0) {
		
	}
	index_entry_low = 0;
	index_entry_high = parent->num_entry - 1;
	for (;;) {
		index_entry_mid = (index_entry_low + index_entry_high) / 2;
		if (key < getKeyFromEntry(parent->entry[index_entry_mid])) {
			index_entry_high = index_entry_mid - 1;
		}
		else if (key >= getKeyFromEntry(parent->entry[index_entry_mid + 1])) {
			index_entry_low = index_entry_mid + 1;
		}
		else {
			break;
		}
	}
	
}

void
_clbptSplit(
	clbpt_int_node *target,
	clbpt_wpacket *propagate,
	uint *top_propagate,
	struct clheap *heap,
	clbpt_property *property,
	uint level_proc
	)
{

}
