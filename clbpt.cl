/*
	Issues:		1. Packet select: isOver
*/

#include "kma.cl"

// Temporary. Replace this by compiler option later.
#define CLBPT_ORDER 8
#define CPU_BITNESS 64
#define MAX_LOCAL_SIZE 256

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

typedef struct _clbpt_ins_pkt {
	clbpt_int_node *target;
	clbpt_entry entry;
} clbpt_ins_pkt;

typedef struct _clbpt_del_pkt {
	clbpt_int_node *target;
	clbpt_key key;
} clbpt_del_pkt;

typedef struct _clbpt_property {
	uintptr_t root;
	int level;
} clbpt_property;

#define getKey(X) (int)(((X) << 1) & 0x80000000 | (X) & 0x7FFFFFFF)
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

#define isWPacketValid(X) ((X).target != 0)

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
_clbptWPacketBufferHandler(
        __global clbpt_ins_pkt *ins,
        uint num_ins,
        __global clbpt_del_pkt *del,
        uint num_del,
        __global struct clheap *heap,
        __global clbpt_property *property,
        uint level_proc
        );

__kernel void
_clbptWPacketCompact(
        __global clbpt_ins_pkt *ins,
        uint num_ins,
        __global clbpt_del_pkt *del,
        uint num_del,
        __global struct clheap *heap,
        __global clbpt_property *property,
        uint level_proc_old
        );

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
						_clbptPacketSort(execute, result_addr, execute_temp, result_addr_temp, less_index);
					 }
				);
			}
			if (num_execute - equal_index > 0) {
				enqueue_kernel(
					get_default_queue(),
					CLK_ENQUEUE_FLAGS_NO_WAIT,
					ndrange_1D(l_size, l_size),
					^{
						_clbptPacketSort(execute + equal_index, result_addr + equal_index, execute_temp + equal_index, result_addr_temp + equal_index, num_execute - equal_index);
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

uint
_binary_search(
	clbpt_int_node *node,
	int target_key
	)
{
	int key;
	uint index_entry_high, index_entry_low, index_entry_mid;
	
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
	return index_entry_low;
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
	clbpt_int_node *node;
	uint entry_index;
	
	if (gid >= buffer_size) return;
	
	key = getKeyFromPacket(execute[gid]);
	node = (clbpt_int_node *)(property->root);
	for (int i = 0; i < property->level - 1; i++) {
		entry_index = _binary_search(node, key);
		node = (clbpt_int_node *)getChildFromEntry(node->entry[entry_index]);
	}
	result[gid] = *((cpu_address_t *)node);
}
	
/**
 *	This is the only kernel function for host to call.
 *	The global size must equal to or greater than num_ins and num_del.
 */
__kernel void
_clbptWPacketInit(		
	__global clbpt_ins_pkt *ins,
	__global cpu_address_t *addr,
	uint num_ins,
	__global clbpt_del_pkt *del,
	uint num_del,
	__global struct clheap *heap,
	__global clbpt_property *property
	)
{
	uint gid = get_global_id(0);

	// Initialize Insert Packet
	if (gid < num_ins) {
		cpu_address_t *new_alloc;

		new_alloc = (cpu_address_t *)malloc(heap, sizeof(cpu_address_t));
		*new_alloc = addr[gid];
		ins[gid].entry.child = (uintptr_t)new_alloc;
	}
	// Initialize Delete Packet
	if (gid < num_del) {
		clbpt_del_pkt del_pkt = del[gid];
		clbpt_int_node *node = del_pkt.target;
		uint entry_index;

		entry_index = _binary_search(node, getKey(del_pkt.key));
		free(heap, node->entry[entry_index].child);
	}
	// Enqueue _clbptWPacketBufferHandler
	if (gid == 0) {
		int level_proc = property->level - 2;
		if (level_proc >= 0) {
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_WAIT_KERNEL,
				ndrange_1D(CLBPT_ORDER, CLBPT_ORDER),
				^{
					 _clbptWPacketBufferHandler(ins,	num_ins, del, num_del, 
						heap, property, level_proc);
				 }
			);
		} 
		else {
			// Enqueue level up procedure
		}
	}
}

__kernel void
_clbptWPacketBufferHandler(
	__global clbpt_ins_pkt *ins,
	uint num_ins,
	__global clbpt_del_pkt *del,
	uint num_del,
	__global struct clheap *heap,
	__global clbpt_property *property,
	uint level_proc
	)
{
	uint gid = get_global_id(0);
	uint ins_begin, del_begin;
	uint num_ins_sgroup, num_del_sgroup;
	uint is_in_sgroup;
	uintptr_t cur_parent;
	clk_event_t level_bar;

	ins_begin = 0;
	del_begin = 0;
	while (ins_begin != num_ins || del_begin != num_del) {
		// Define cur_target
		if (gid == 0) {
			if (ins_begin == num_ins) {
				cur_parent = del[del_begin].target->parent;
			}
			else if (del_begin== num_del) {
				cur_parent = ins[ins_begin].target->parent;
			}
			else if (getKey(ins[ins_begin].entry.key) <
				getKey(del[del_begin].key))
			{
				cur_parent = ins[ins_begin].target->parent;
			}
			else {
				cur_parent = del[del_begin].target->parent;
			}
		}
		work_group_barrier(0);
		cur_parent = work_group_broadcast(cur_parent, 0);
		// Find siblings in ins and del
		if (ins_begin + gid < num_ins) {
			if (ins[ins_begin + gid].target->parent == cur_parent) {
				is_in_sgroup = 1;
			}
			else {
				is_in_sgroup = 0;
			}
		}
		else {
			is_in_sgroup = 0;
		}
		work_group_barrier(0);
		num_ins_sgroup = work_group_reduce_add(is_in_sgroup);
		if (del_begin + gid < num_del) {
			if (del[del_begin + gid].target->parent == cur_parent) {
				is_in_sgroup = 1;
			}
			else {
				is_in_sgroup = 0;
			}
		}
		else {
			is_in_sgroup = 0;
		}
		work_group_barrier(0);
		num_del_sgroup = work_group_reduce_add(is_in_sgroup);
		// Enqueue super group handler
		if (gid == 0) {
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_NO_WAIT,
				ndrange_1D(CLBPT_ORDER, CLBPT_ORDER),
				^{
					_clbptWPacketSuperGroupHandler(
						// Arguments
					);
				 }
			);
		}
		work_group_barrier(0);
		ins_begin += num_ins_sgroup;
		del_begin += num_del_sgroup;
	}
	if (gid == 0) {
		enqueue_marker(get_default_queue(), 0, NULL, &level_bar);
		release_event(level_bar);
		enqueue_kernel(
			get_default_queue(),
			CLK_ENQUEUE_FLAGS_NO_WAIT,
			ndrange_1D(MAX_LOCAL_SIZE, MAX_LOCAL_SIZE),
			^{
				_clbptWPacketCompact(ins, num_ins, del, num_del, heap,
					property, level_proc);
			 }
		);
	}
}

__kernel void
_clbptWPacketCompact(
	__global clbpt_ins_pkt *ins,
	uint num_ins,
	__global clbpt_del_pkt *del,
	uint num_del,
	__global struct clheap *heap,
	__global clbpt_property *property,
	uint level_proc_old
	)
{
	uint gid = get_global_id(0);
	uint grsize = get_local_size(0);
	int valid, id;
	uint id_base;
	clbpt_ins_pkt ins_proc;
	clbpt_del_pkt del_proc;

	// Compace Insert Packet List
	id_base = 0;
	for (uint old_ins_i_base = 0; old_ins_i_base < num_ins; old_ins_i_base += grsize) {
		uint old_ins_i = old_ins_i_base + gid;
		if (old_ins_i < num_ins) {
			valid = isWPacketValid(ins_proc = ins[old_ins_i]);
		}
		else {
			valid = 0;
		}
		work_group_barrier(0);
		id = work_group_scan_exclusive_add(valid);
		if (valid) {
			ins[id_base + id] = ins_proc;
		}
		work_group_barrier(0);
		id_base += work_group_broadcast(id, grsize - 1) + 1;
	}
	num_ins = id_base;
	// Compace Delete Packet List
	id_base = 0;
	for (uint old_del_i_base = 0; old_del_i_base < num_del; 
		old_del_i_base += grsize) 
	{
		uint old_del_i = old_del_i_base + gid;
		if (old_del_i < num_del) {
			valid = isWPacketValid(del_proc = del[old_del_i]);
		}
		else {
			valid = 0;
		}
		work_group_barrier(0);
		id = work_group_scan_exclusive_add(valid);
		if (valid) {
			del[id_base + id] = del_proc;
		}
		work_group_barrier(0);
		id_base += work_group_broadcast(id, grsize - 1) + 1;
	}
	num_del = id_base;
	// Enqueue _clbptWPacketBufferHandler
	if (gid == 0) {
		int level_proc = level_proc_old - 1;
		if (level_proc >= 0) {
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_WAIT_KERNEL,
				ndrange_1D(CLBPT_ORDER, CLBPT_ORDER),
				^{
					_clbptWPacketBufferHandler(ins,	num_ins, del, num_del, 
						heap, property, level_proc);
				 }
			);
		} 
		else {
			// Enqueue level up procedure
		}
	}
}

/*
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
*/
