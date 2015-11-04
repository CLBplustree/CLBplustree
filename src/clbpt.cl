#include "clIndexedQueue.cl"
#include "kma.cl"

// Temporary. Replace this by compiler option later.
#define CLBPT_ORDER 4		// Should be less than or equal to half
							// of MAX_LOCAL_SIZE
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

// ISSUE: Now this structure works fine. However, when the order of its content
// changes, AMD OpenCL compiler will crash... It's quite weird.
typedef struct _clbpt_int_node {
	uintptr_t parent;
	clbpt_entry entry[CLBPT_ORDER];
	uint num_entry;
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
	uintptr_t target;
	clbpt_entry entry;
} clbpt_ins_pkt;

typedef struct _clbpt_del_pkt {
	uintptr_t target;
	clbpt_key key;
} clbpt_del_pkt;

typedef struct _clbpt_property {
	uintptr_t root;
	int level;
} clbpt_property;

typedef struct _clbpt_leafmirror {
	uintptr_t parent;
	cpu_address_t leaf;
} clbpt_leafmirror;

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
#define ENTRY_NULL ((clbpt_entry){.key = 0x3FFFFFFF, .child = 0})
#define isNullEntry(X) ((X).child == 0)

#define getParentKeyFromNode(X) (int)((((X).parent_key) << 1) & 0x80000000 | ((X).parent_key) & 0x7FFFFFFF)

#define isWPacketValid(X) ((X).target != 0)

#define half_c(X) (((X) + 1) / 2)
#define half_f(X) ((X) / 2)

#define KEY_MIN (0xC0000000)

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
_clbptWPacketBufferRootHandler(
	__local clbpt_entry *proc_list,
    __global clbpt_ins_pkt *ins,
    uint num_ins,
    __global clbpt_del_pkt *del,
    uint num_del,
    __global struct clheap *heap,
    __global clbpt_property *property
    );

void
_clbptWPacketBufferPreRootHandler(
    __global clbpt_ins_pkt *ins,
    __global struct clheap *heap,
    __global clbpt_property *property
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
_clbptWPacketSuperGroupHandler(
	__local clbpt_entry *proc_list,
	__global clbpt_ins_pkt *ins,
	uint num_ins,
	__global clbpt_del_pkt *del,
	uint num_del,
	__global struct clheap *heap,
	uint aboveMirror
	);

void
_clbptWPacketGroupHandler(
	__local clbpt_entry *proc_list,
	__global clbpt_ins_pkt *ins,
	uint num_ins,
	__global clbpt_del_pkt *del,
	uint num_del,
	__global struct clheap *heap,
	clbpt_int_node *parent,
	uint target_branch_index,
	clbpt_int_node *target,
	clbpt_int_node *sibling,
	uint aboveMirror
	);

void
_clbptWPacketBufferPreRootHandler(
    __global clbpt_ins_pkt *ins,
    __global struct clheap *heap,
    __global clbpt_property *property
    )
{
	uint gid = get_global_id(0);
	clbpt_int_node *new_root;

	new_root = (clbpt_int_node *)malloc(heap, sizeof(clbpt_int_node));
	new_root->parent = 0;
	new_root->parent_key = KEY_MIN;
	new_root->num_entry = 2;
	new_root->entry[0].key = KEY_MIN;
	new_root->entry[0].child = property->root;
	((clbpt_leafmirror *)(new_root->entry[0].child))->parent =
		(uintptr_t)new_root;
	new_root->entry[1] = ins[0].entry;
	((clbpt_leafmirror *)(new_root->entry[1].child))->parent =
		(uintptr_t)new_root;
	property->root = (uintptr_t)new_root;
	property->level += 1;
}
__kernel void
_clbptPacketSort(
			__global clbpt_packet *execute,
			__global cpu_address_t *result_addr,
			__global clbpt_packet *execute_temp,
			__global cpu_address_t *result_addr_temp,
			uint num_execute
			)
{

	const int gid = get_global_id(0);
	const int lid = get_local_id(0);
	const int l_size = get_local_size(0);
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
	const int gid = get_global_id(0);
	const int grid = get_group_id(0);
	const int lsize = get_local_size(0);

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
	property->root = (uintptr_t)malloc(heap, sizeof(clbpt_leafmirror));
	((clbpt_leafmirror *)(property->root))->leaf = host_root;
	((clbpt_leafmirror *)(property->root))->parent = 0;
}

uint
_binary_search(
	clbpt_int_node *node,
	int target_key
	)
{
	uint index_entry_high, index_entry_low, index_entry_mid;

	index_entry_low = 0;
	index_entry_high = node->num_entry - 1;
	for (;;) {
		index_entry_mid = (index_entry_low + index_entry_high) / 2;
		if (index_entry_mid == node->num_entry - 1) {
			break;
		}
		else if (target_key < getKeyFromEntry(node->entry[index_entry_mid])) {
			index_entry_high = index_entry_mid - 1;
		}
		else if (target_key >= getKeyFromEntry(node->entry[index_entry_mid + 1])) {
			index_entry_low = index_entry_mid + 1;
		}
		else {
			break;
		}
	}
	return index_entry_mid;
}

__kernel void
_clbptSearch(
	__global cpu_address_t *result,
	__global clbpt_property *property,
	__global clbpt_packet *execute,
	__const uint num_packet
	)
{
	uint gid = get_global_id(0);
	int key;
	clbpt_int_node *node;
	uint entry_index;

	if (gid >= num_packet) return;

	key = getKeyFromPacket(execute[gid]);
	node = (clbpt_int_node *)(property->root);
	for (int i = 0; i < property->level - 1; i++) {
		entry_index = _binary_search(node, key);
		node = (clbpt_int_node *)getChildFromEntry(node->entry[entry_index]);
	}
	result[gid] = ((clbpt_leafmirror *)node)->leaf;
}

/**
 *	This is the only kernel function for host to call.
 *	The global size must equal to or greater than num_ins and num_del.
 */
__kernel void
_clbptWPacketInit(

	__global clbpt_ins_pkt *ins,
	__global cpu_address_t *addr,
	__global uintptr_t *leafmirror_addr,
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
		clbpt_leafmirror *new_alloc;

		new_alloc = (clbpt_leafmirror *)malloc(heap, sizeof(clbpt_leafmirror));
		new_alloc->leaf = addr[gid];
		new_alloc->parent = ((clbpt_leafmirror *)(ins[gid].target))->parent;
		ins[gid].target = ((clbpt_leafmirror *)(ins[gid].target))->parent;
		ins[gid].entry.child = (uintptr_t)new_alloc;
		leafmirror_addr[gid] = (uintptr_t)new_alloc;
	}
	// Initialize Delete Packet
	if (gid < num_del) {
		clbpt_leafmirror *mirror = del[gid].target;
		del[gid].target = (clbpt_int_node *)mirror->parent;
		free(heap, (uintptr_t)mirror);
	}
	// Handle them
	if (gid == 0) {
		int level_proc = property->level - 2;
		
		if (level_proc > 0) {
			// Handle internal node layer
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_NO_WAIT,
				ndrange_1D(2 * CLBPT_ORDER, 2 * CLBPT_ORDER),
				^{
					 _clbptWPacketBufferHandler(ins, num_ins, del, num_del,
						heap, property, level_proc);
				 }
			);
		}
		else if (level_proc == 0) {
			// Handle root node layer
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_NO_WAIT,
				ndrange_1D(2 * CLBPT_ORDER, 2 * CLBPT_ORDER),
				^(__local void *proc_list){
					_clbptWPacketBufferRootHandler(proc_list, ins, num_ins, del,
						num_del, heap, property);
				},
				2 * CLBPT_ORDER * sizeof(clbpt_entry)
			);
		}
		else {
			_clbptWPacketBufferPreRootHandler(ins, heap, property);
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
	int num_kernel = 0;
	clk_event_t level_barrier, wait_level_barrier;

	ins_begin = 0;
	del_begin = 0;
	wait_level_barrier = create_user_event();
	set_user_event_status(wait_level_barrier, CL_COMPLETE);
	while (ins_begin != num_ins || del_begin != num_del) {
		// Define cur_target
		if (gid == 0) {
			if (ins_begin == num_ins) {
				cur_parent = ((clbpt_int_node *)del[del_begin].target)->parent;
			}
			else if (del_begin == num_del) {
				cur_parent = ((clbpt_int_node *)ins[ins_begin].target)->parent;
			}
			else if (getKey(ins[ins_begin].entry.key) <
				getKey(del[del_begin].key))
			{
				cur_parent = ((clbpt_int_node *)ins[ins_begin].target)->parent;
			}
			else {
				cur_parent = ((clbpt_int_node *)del[del_begin].target)->parent;
			}
		}
		work_group_barrier(0);
		cur_parent = work_group_broadcast(cur_parent, 0);
		// Find siblings in ins and del
		if (ins_begin + gid < num_ins) {
			if (((clbpt_int_node *)ins[ins_begin + gid].target)->parent == cur_parent) {
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
			if (((clbpt_int_node *)del[del_begin + gid].target)->parent == cur_parent) {
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
				ndrange_1D(2 * CLBPT_ORDER, 2 * CLBPT_ORDER),
				1,
				&wait_level_barrier,
				&level_barrier,
				^(__local void *proc_list){
					_clbptWPacketSuperGroupHandler(
						proc_list,
						ins + ins_begin,
						num_ins_sgroup,
						del + del_begin,
						num_del_sgroup,
						heap,
						level_proc == property->level - 2
					);
				},
				2 * CLBPT_ORDER * sizeof(clbpt_entry)
			);
			release_event(wait_level_barrier);
			wait_level_barrier = level_barrier;
			/*
			if (ins_begin == 0 && del_begin == 0) {
				enqueue_marker(get_default_queue(), 1, level_barrier, 
					&level_barrier[1]);
			}
			else {
				enqueue_marker(get_default_queue(), 2, level_barrier, 
					&level_barrier[1]);
			}
			*/
		}
		ins_begin += num_ins_sgroup;
		del_begin += num_del_sgroup;
	}	
	if (gid == 0) {
		enqueue_kernel(
			get_default_queue(),
			CLK_ENQUEUE_FLAGS_WAIT_KERNEL,
			ndrange_1D(MAX_LOCAL_SIZE, MAX_LOCAL_SIZE),
			1,
			&wait_level_barrier,
			NULL,
			^{
				_clbptWPacketCompact(ins, num_ins, del, num_del, heap,
					property, level_proc);
			 }
		);
		release_event(wait_level_barrier);
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

	// Compact Insert Packet List
	id_base = 0;
	for (uint old_ins_i_base = 0; old_ins_i_base < num_ins;
		old_ins_i_base += grsize)
	{
		uint old_ins_i = old_ins_i_base + gid;
		if (old_ins_i < num_ins) {
			ins_proc = ins[old_ins_i];
			valid = (ins_proc.target != 0) ? 1 : 0;
		}
		else {
			valid = 0;
		}
		work_group_barrier(0);
		id = work_group_scan_exclusive_add(valid);
		if (valid) {
			ins[id_base + id] = ins_proc;
		} 
		work_group_barrier(CLK_GLOBAL_MEM_FENCE);
		id_base += work_group_reduce_add(valid);
	}
	if (gid == 0)
		num_ins = id_base;
	work_group_barrier(CLK_GLOBAL_MEM_FENCE);
	// Compact Delete Packet List
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
		work_group_barrier(CLK_GLOBAL_MEM_FENCE);
		id_base += work_group_reduce_add(valid);
	}
	if (gid == 0)
		num_del = id_base;
	work_group_barrier(CLK_GLOBAL_MEM_FENCE);
	// Enqueue _clbptWPacketBufferHandler
	if (gid == 0 && (num_ins != 0 || num_del != 0)) {
		int level_proc = level_proc_old - 1;
		if (level_proc > 0) {
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_WAIT_KERNEL,
				ndrange_1D(2 * CLBPT_ORDER, 2 * CLBPT_ORDER),
				^{
					_clbptWPacketBufferHandler(ins,	num_ins, del, num_del,
						heap, property, level_proc);
				 }
			);
		}
		else if (level_proc == 0) {
			// Handle root node layer
			enqueue_kernel(
				get_default_queue(),
				CLK_ENQUEUE_FLAGS_WAIT_KERNEL,
				ndrange_1D(2 * CLBPT_ORDER, 2 * CLBPT_ORDER),
				^(__local void *proc_list){
					_clbptWPacketBufferRootHandler(proc_list, ins, num_ins, del,
						num_del, heap, property);
				},
				2 * CLBPT_ORDER * sizeof(clbpt_entry)
			);
		}
		else {
			// Handle pre-root stage
			_clbptWPacketBufferPreRootHandler(ins, heap, property);
		}
	}
}

__kernel void
_clbptWPacketSuperGroupHandler(
	__local clbpt_entry *proc_list,
	__global clbpt_ins_pkt *ins,
	uint num_ins,
	__global clbpt_del_pkt *del,
	uint num_del,
	__global struct clheap *heap,
	uint aboveMirror
	)
{
	uint gid = get_global_id(0);
	uint ins_begin, del_begin;
	uint num_ins_group, num_del_group;
	uint is_in_group;
	uintptr_t cur_target;
	clk_event_t level_bar;
	uint target_branch_index;
	clbpt_int_node *parent;
	clbpt_int_node *sibling = 0;
		
	ins_begin = 0;
	del_begin = 0;
	if (num_ins > 0) {
		parent = (clbpt_int_node *)
			(((clbpt_int_node *)(ins[0].target))->parent);
	}
	else {
		parent = (clbpt_int_node *)
			(((clbpt_int_node *)(del[0].target))->parent);
	}
	for (target_branch_index = 0; target_branch_index < parent->num_entry;
		target_branch_index++)
	{
		// Define cur_target
		cur_target = parent->entry[target_branch_index].child;
		// Find siblings in ins and del
		if (ins_begin + gid < num_ins) {
			if ((uintptr_t)(ins[ins_begin + gid].target) == cur_target) {
				is_in_group = 1;
			}
			else {
				is_in_group = 0;
			}
		}
		else {
			is_in_group = 0;
		}
		work_group_barrier(0);
		num_ins_group = work_group_reduce_add(is_in_group);
		if (del_begin + gid < num_del) {
			if ((uintptr_t)(del[del_begin + gid].target) == cur_target) {
				is_in_group = 1;
			}
			else {
				is_in_group = 0;
			}
		}
		else {
			is_in_group = 0;
		}
		work_group_barrier(0);
		num_del_group = work_group_reduce_add(is_in_group);
		if (num_ins_group == 0 && num_del_group == 0)
			continue;
		_clbptWPacketGroupHandler(proc_list, ins + ins_begin, num_ins_group,
			del + del_begin, num_del_group, heap, parent, target_branch_index,
			(clbpt_int_node *)cur_target, sibling, aboveMirror);
		ins_begin += num_ins_group;
		del_begin += num_del_group;
	}
}

void
_clbptWPacketGroupHandler(
	__local clbpt_entry *proc_list,
	__global clbpt_ins_pkt *ins,
	uint num_ins,
	__global clbpt_del_pkt *del,
	uint num_del,
	__global struct clheap *heap,
	clbpt_int_node *parent,
	uint target_branch_index,
	clbpt_int_node *target,
	clbpt_int_node *sibling,
	uint aboveMirror
	)
{
	uint gid = get_global_id(0);

	// Clear proc_list
	proc_list[gid] = ENTRY_NULL;
	work_group_barrier(CLK_LOCAL_MEM_FENCE);
	
	// Copy the target node into proc_list
	if (gid < target->num_entry) {
		proc_list[gid] = target->entry[gid];
	}
	work_group_barrier(CLK_LOCAL_MEM_FENCE);

	// Delete
	if (gid < num_del) {
		int target_key;

		target_key = getKey(del[gid].key);
		for (uint i = 0; i < CLBPT_ORDER; i++) {
			if (getKey(proc_list[i].key) == target_key) {
				proc_list[i] = ENTRY_NULL;
			}
		}
	}
	work_group_barrier(CLK_LOCAL_MEM_FENCE);
	// Insert
	if (gid < num_ins) {
		proc_list[CLBPT_ORDER + gid] = ins[gid].entry;
	}
	work_group_barrier(CLK_LOCAL_MEM_FENCE);
	if (gid < 2 * CLBPT_ORDER) {
		uint bitonic_max_level, offset, local_offset;
		size_t exchg_index;
		clbpt_entry temp1, temp2;

		for (bitonic_max_level = 1; bitonic_max_level < 2 * CLBPT_ORDER;
			bitonic_max_level <<= 1)
		{
			if ((local_offset = gid & ((bitonic_max_level << 1) - 1))
				< bitonic_max_level)
			{
				temp1 = proc_list[gid];
				temp2 = proc_list[exchg_index = gid +
						((bitonic_max_level - local_offset) << 1) - 1];
				if (exchg_index < CLBPT_ORDER * 2 && 
					getKey(temp1.key) > getKey(temp2.key))
				{
					proc_list[gid] = temp2;
					proc_list[exchg_index] = temp1;
				}
			}
			work_group_barrier(CLK_LOCAL_MEM_FENCE);
			for (offset = (bitonic_max_level >> 1); offset != 0; offset >>= 1) {
				if ((gid & ((offset << 1) - 1)) < offset) {
					temp1 = proc_list[gid];
					temp2 = proc_list[exchg_index = gid + offset];
					if (exchg_index < CLBPT_ORDER * 2 && 
						getKey(temp1.key) > getKey(temp2.key))
					{
						proc_list[gid] = temp2;
						proc_list[exchg_index] = temp1;
					}
				}
				work_group_barrier(CLK_LOCAL_MEM_FENCE);
			}

		}
	}
	// Count num_entry
	uint proc_num_entry;
	{
		uint valid = 0;
		if (gid < 2 * CLBPT_ORDER && proc_list[gid].child != NULL) {
			valid = 1;
		}
		proc_num_entry = work_group_reduce_add(valid);
	}

	// Clear ins & del
	if (gid < num_ins) {
		ins[gid].target = 0;
	}
	if (gid < num_del) {
		del[gid].target = 0;
	}

	// Handle "proc".	
	if (target_branch_index != 0 &&
		proc_num_entry < half_c(CLBPT_ORDER))
	{
		// Need Borrow or Merge

		if (sibling->num_entry + proc_num_entry <
			2 * half_c(CLBPT_ORDER))
		{
			// Merge
			if (gid == 0) {
				sibling->entry[sibling->num_entry].key = target->parent_key;
				sibling->entry[sibling->num_entry].child = proc_list[0].child;
				if (aboveMirror)
					((clbpt_leafmirror *)proc_list[0].child)->parent = 
						(uintptr_t)sibling;
				else {
					((clbpt_int_node *)proc_list[0].child)->parent = 
						(uintptr_t)sibling;
					((clbpt_int_node *)proc_list[0].child)->parent_key = 
						KEY_MIN;
				}
			}
			else if (gid < proc_num_entry) {
				sibling->entry[sibling->num_entry + gid] = proc_list[gid];
				if (aboveMirror)
					((clbpt_leafmirror *)proc_list[gid].child)->parent = 
						(uintptr_t)sibling;
				else {
					((clbpt_int_node *)proc_list[gid].child)->parent = 
						(uintptr_t)sibling;
				}
			}
			if (gid == 0) {
				sibling->num_entry += proc_num_entry;
				del[0].target = parent;
				del[0].key = target->parent_key;
				free(heap, (uintptr_t)target);
			} 
		}
		else {
			// Borrow
			// Step1. extend proc_list with sibling nodes
			clbpt_entry temp_entry;
			
			if (gid < sibling->num_entry) {
				temp_entry = sibling->entry[gid];
			}
			else if (gid == sibling->num_entry) {
				temp_entry.key = target->parent_key;
				temp_entry.child = proc_list[0].child;
			}
			else if (gid < sibling->num_entry + proc_num_entry) {
				temp_entry = proc_list[gid - sibling->num_entry];
			}

			// Step2. Refresh entries
			if (gid < half_c(sibling->num_entry + proc_num_entry)) {
				// Do nothing. Leave slibing node the same.
			}
			else if (gid == half_c(sibling->num_entry + proc_num_entry)) {
				target->entry[0].key = KEY_MIN;
				target->entry[0].child = temp_entry.child;
				if (aboveMirror)
					((clbpt_leafmirror *)temp_entry.child)->parent = 
						(uintptr_t)target;
				else {
					((clbpt_int_node *)temp_entry.child)->parent = 
						(uintptr_t)target;
					((clbpt_int_node *)temp_entry.child)->parent_key = 
						KEY_MIN;
				}
			}
			else if (gid < sibling->num_entry + proc_num_entry) {
				target->entry
					[gid - half_c(sibling->num_entry + proc_num_entry)] =
					temp_entry;
				if (aboveMirror)
					((clbpt_leafmirror *)temp_entry.child)->parent = 
						(uintptr_t)target;
				else {
					((clbpt_int_node *)temp_entry.child)->parent = 
						(uintptr_t)target;
				}
			}

			// Step3. Refresh metadata
			if (gid == half_c(sibling->num_entry + proc_num_entry)) {
				sibling->num_entry = 
					half_c(sibling->num_entry + proc_num_entry);
				del[0].target = parent;
				del[0].key = target->parent_key;
				ins[0].target = parent;
				ins[0].entry.key = temp_entry.key;
				ins[0].entry.child = (uintptr_t)target;
				target->num_entry = sibling->num_entry + proc_num_entry -
					half_c(sibling->num_entry + proc_num_entry);
				target->parent_key = temp_entry.key;
			}

			// Step4. Refresh sibling ptr.
			sibling = target;
		}
	}
	else if (proc_num_entry > CLBPT_ORDER) {
		// Split
		// Step1. Allocate a new node
		clbpt_int_node *new_node;
		if (gid == 0) {
			new_node = (clbpt_int_node *)malloc(heap, sizeof(clbpt_int_node));
		}
		work_group_barrier(0);
		new_node = (clbpt_int_node *)
			work_group_broadcast((uintptr_t)new_node, 0);

		// Step2. Refresh entries
		if (gid < half_c(proc_num_entry)) {
			target->entry[gid] = proc_list[gid];
		}
		else if (gid == half_c(proc_num_entry)) {
			new_node->entry[0].key = KEY_MIN;
			new_node->entry[0].child = (uintptr_t)proc_list[gid].child;
			if (aboveMirror)
				((clbpt_leafmirror *)proc_list[gid].child)->parent = 
					(uintptr_t)new_node;
			else {
				((clbpt_int_node *)proc_list[gid].child)->parent = 
					(uintptr_t)new_node;
				((clbpt_int_node *)proc_list[gid].child)->parent_key = 
					KEY_MIN;
			}
		}
		else if (gid < proc_num_entry) {
			new_node->entry[gid - half_c(proc_num_entry)] =
				proc_list[gid];
			if (aboveMirror)
				((clbpt_leafmirror *)proc_list[gid].child)->parent = 
					(uintptr_t)new_node;
			else {
				((clbpt_int_node *)proc_list[gid].child)->parent = 
					(uintptr_t)new_node;
			}
		}

		// Step3. Refresh metadata
		if (gid == 0) {
			target->num_entry = half_c(proc_num_entry);
			ins[0].target = parent;
			ins[0].entry.key = proc_list[half_c(proc_num_entry)].key;
			ins[0].entry.child = (uintptr_t)new_node;
			new_node->parent = (uintptr_t)parent;
			new_node->parent_key = proc_list[half_c(proc_num_entry)].key;
			new_node->num_entry = proc_num_entry - half_c(proc_num_entry);
		}
		sibling = new_node;
	}
	else {
		// Leftmost node or the number of entries is legal
		if (gid < proc_num_entry) {
			target->entry[gid] = proc_list[gid];
		}
		if (gid == 0) {
			target->num_entry = proc_num_entry;
		}
		sibling = target;
	}
	
}

/*
void
_clbptWPacketLeftmostChildHandler(
	__local clbpt_entry *proc_list,
	__global struct clheap *heap,
	__global clbpt_del_pkt *del,
	clbpt_int_node *parent,
	clbpt_int_node *target,
	clbpt_int_node *right_sibling
	)
{
	if (right_sibling == 0)
		return;
	if (target->num_entry < half_c(CLBPT_ORDER)) {
		if (target->num_entry + right_sibling->num_entry <
			2 * half_c(CLBPT_ORDER))
		{
			// Merge
			if (gid == 0) {
				target->entry[target->num_entry].key =
					right_sibling->parent_key;
				target->entry[target->num_entry].child =
					right_sibling->entry[0].child;
			}
			else if (gid < right_sibling->num_entry) {
				target->entry[target->num_entry + gid] =
					right_sibling->entry[gid];
			}
			if (gid == 0) {
				target->num_entry += right_sibling->num_entry;
				del[0].target = parent;
				del[0].key = right_sibling->parent_key;
				free(heap, (uintptr_t)right_sibling);
			}
		}
		else {
			// Borrow
			if (gid == 0) {
				target->entry[target->num_entry].key =
					right_sibling->parent_key;
				target->entry[target->num_entry].child =
					right_sibling->entry[0].child;
			}
			else if (gid < half_f(target->num_entry +
				right_sibling->num_entry - 1) - target->num_entry + 1)
			{
				target->entry[target->num_entry + gid] =
					right_sibling->entry[gid];
			}
			else if (gid == half_f(target->num_entry +
				right_sibling->num_entry - 1) - target->num_entry + 1)
			{
				right_sibling->entry[0].key = KEY_MIN;
				right_sibling->entry[0].child = right_sibling->entry[gid].child;
			}
			else if (gid < proc_num_entry) {
				target->entry[gid - half_f(sibling->num_entry +
					proc_num_entry - 1) + sibling->num_entry - 1] =
					proc_list[gid];
			}
			if (gid == 0) {
				sibling->num_entry = half_f(sibling->num_entry +
					proc_num_entry - 1) + 1;
				del[0].target = parent;
				del[0].key = target->parent_key;
				ins[0].target = parent;
				ins[0].entry.key = proc_list[half_f(sibling->num_entry +
					proc_num_entry - 1) - sibling->num_entry + 1].key;
				ins[0].entry.child = (uintptr_t)target;
				target->parent_key = proc_list[half_f(sibling->num_entry +
					proc_num_entry - 1) - sibling->num_entry + 1].key;
				target->num_entry = proc_num_entry - half_f(sibling->num_entry +
					proc_num_entry - 1) + sibling->num_entry - 1;
			}
			if (sibling == (clbpt_int_node *)(parent->entry[0].child))
			{
				right_sibling = target;
			}
			sibling = target;
		}
	}
}
*/

__kernel void
_clbptWPacketBufferRootHandler(
	__local clbpt_entry *proc_list,
    __global clbpt_ins_pkt *ins,
    uint num_ins,
    __global clbpt_del_pkt *del,
    uint num_del,
    __global struct clheap *heap,
    __global clbpt_property *property
    )
{
	int gid = get_global_id(0);
	clbpt_int_node *target;

	// Clear proc_list
	proc_list[gid] = ENTRY_NULL;
	work_group_barrier(CLK_LOCAL_MEM_FENCE);

	// Copy the target node into proc_list
	if (num_ins != 0) {
		target = (clbpt_int_node *)(ins[0].target);
	}
	else {
		target = (clbpt_int_node *)(del[0].target);
	}

	if (gid < target->num_entry) {
		proc_list[gid] = target->entry[gid];
	}
	work_group_barrier(CLK_LOCAL_MEM_FENCE);
	
	// Delete
	if (gid < num_del) {
		int target_key;

		target_key = getKey(del[gid].key);
		for (uint i = 0; i < CLBPT_ORDER; i++) {
			if (getKey(proc_list[i].key) == target_key) {
				proc_list[i] = ENTRY_NULL;
			}
		}
	}
	work_group_barrier(CLK_LOCAL_MEM_FENCE);
	// Insert
	if (gid < num_ins) {
		proc_list[CLBPT_ORDER + gid] = ins[gid].entry;
	}
	work_group_barrier(CLK_LOCAL_MEM_FENCE);
	if (gid < 2 * CLBPT_ORDER) {
		uint bitonic_max_level, offset, local_offset;
		size_t exchg_index;
		clbpt_entry temp1, temp2;

		for (bitonic_max_level = 1; bitonic_max_level < 2 * CLBPT_ORDER;
			bitonic_max_level <<= 1)
		{
			if ((local_offset = gid & ((bitonic_max_level << 1) - 1))
				< bitonic_max_level)
			{
				temp1 = proc_list[gid];
				temp2 = proc_list[exchg_index = gid +
						((bitonic_max_level - local_offset) << 1) - 1];
				if (exchg_index < CLBPT_ORDER * 2 && 
					getKey(temp1.key) > getKey(temp2.key))
				{
					proc_list[gid] = temp2;
					proc_list[exchg_index] = temp1;
				}
			}
			work_group_barrier(CLK_LOCAL_MEM_FENCE);
			for (offset = (bitonic_max_level >> 1); offset != 0; offset >>= 1) {
				if ((gid & ((offset << 1) - 1)) < offset) {
					temp1 = proc_list[gid];
					temp2 = proc_list[exchg_index = gid + offset];
					if (exchg_index < CLBPT_ORDER * 2 && 
						getKey(temp1.key) > getKey(temp2.key))
					{
						proc_list[gid] = temp2;
						proc_list[exchg_index] = temp1;
					}
				}
				work_group_barrier(CLK_LOCAL_MEM_FENCE);
			}

		}
	}

	// Count num_entry
	uint proc_num_entry;
	{
		uint valid = 0;
		if (gid < 2 * CLBPT_ORDER && proc_list[gid].child != NULL) {
			valid = 1;
		}
		proc_num_entry = work_group_reduce_add(valid);
	}

	// Handle proc
	if (proc_num_entry == 1) {
		// Need downleveling
		if (gid == 0) {
			property->root = proc_list[0].child;
			property->level -= 1;
			free(heap, (uintptr_t)target);
		}
	}
	else if (proc_num_entry <= CLBPT_ORDER) {
		// Peaceful
		if (gid < CLBPT_ORDER) {
			target->entry[gid] = proc_list[gid];
		}
		if (gid == 0) {
			target->num_entry = proc_num_entry;
		}
	}
	else {
		// Need upleveling
		// Step1. Allocate a new node
		clbpt_int_node *new_root, *new_sibling;
		if (gid == 0) {
			new_root = (clbpt_int_node *)malloc(heap, sizeof(clbpt_int_node));
			new_sibling = (clbpt_int_node *)malloc(heap, sizeof(clbpt_int_node));
		}
		work_group_barrier(0);
		new_root = (clbpt_int_node *)
			work_group_broadcast((uintptr_t)new_root, 0);
		new_sibling = (clbpt_int_node *)
			work_group_broadcast((uintptr_t)new_sibling, 0);

		// Step2. Refresh entries
		if (gid < half_c(proc_num_entry)) {
			target->entry[gid] = proc_list[gid];
		}
		else if (gid == half_c(proc_num_entry)) {
			new_sibling->entry[0].key = KEY_MIN;
			new_sibling->entry[0].child = (uintptr_t)proc_list[gid].child;
			if (property->level == 2)
				((clbpt_leafmirror *)proc_list[gid].child)->parent = 
					(uintptr_t)new_sibling;
			else {
				((clbpt_int_node *)proc_list[gid].child)->parent = 
					(uintptr_t)new_sibling;
				((clbpt_int_node *)proc_list[gid].child)->parent_key = 
					KEY_MIN;
			}
		}
		else if (gid < proc_num_entry) {
			new_sibling->entry[gid - half_c(proc_num_entry)] =
				proc_list[gid];
			if (property->level == 2)
				((clbpt_leafmirror *)proc_list[gid].child)->parent = 
					(uintptr_t)new_sibling;
			else {
				((clbpt_int_node *)proc_list[gid].child)->parent = 
					(uintptr_t)new_sibling;
			}
		}

		// Step3. Create new root & refresh meta-data
		if (gid == 0) {
			target->num_entry = half_c(proc_num_entry);
			target->parent = (uintptr_t)new_root;
			property->root = (uintptr_t)new_root;
			property->level += 1;
			new_root->parent = (uintptr_t)property;
			new_root->parent_key = KEY_MIN;
			new_root->num_entry = 2;
			new_root->entry[0].key = KEY_MIN;
			new_root->entry[0].child = (uintptr_t)target;
			new_root->entry[1].key = proc_list[half_c(proc_num_entry)].key;
			new_root->entry[1].child = (uintptr_t)new_sibling;
			new_sibling->parent = (uintptr_t)new_root;
			new_sibling->parent_key = proc_list[half_c(proc_num_entry)].key;
			new_sibling->num_entry = proc_num_entry - half_c(proc_num_entry);
		}
	}
}

// These functions are for debug use
/*
void
_clbptPrintMirror(
		int level,
		clbpt_leafmirror *mirror
	)
{
	for (int i = 0; i < level; i++)
		printf(" ");
#if CPU_BITNESS == 32
	printf("Mirror:%p (parent=%p, leaf=%u)\n", 
		(void *)mirror, (void *)mirror->parent, mirror->leaf);
#else
	printf("Mirror:%p (parent=%p, leaf=%lu)\n", 
		(void *)mirror, (void *)mirror->parent, mirror->leaf);
#endif

}

void
_clbptPrintNode(
	int level_proc,
	int level_tree,
	clbpt_int_node *node
	)
{
	for (int i = 0; i < level_proc; i++)
		printf(" ");
	printf("Node:%p (num_entry=%u, parent=%p, parent_key=%d)\n",
		(void *)node, node->num_entry, (void *)node->parent, 
		getKey(node->parent_key));
	if (level_proc < level_tree - 2)
		for (int i = 0; i < node->num_entry; i++) {
			for (int j = 0; j < level_proc; j++)
				printf(" ");
			printf(">Entry #%d (key=%d, child=%p)\n", 
				i, getKey(node->entry[i].key), (void *)node->entry[i].child);
			_clbptPrintNode(level_proc + 1, level_tree, 
				(clbpt_int_node *)node->entry[i].child);
		}
	else
		for (int i = 0; i < node->num_entry; i++) {
			for (int j = 0; j < level_proc; j++)
				printf(" ");
			printf(" Entry #%d (key=%d, child=%p)\n", 
				i, getKey(node->entry[i].key), (void *)node->entry[i].child);
			_clbptPrintMirror(level_proc + 1, 
				(clbpt_leafmirror *)node->entry[i].child);
		}

}

void
_clbptPrintTree(
	clbpt_property *property
	)
{
	int level = property->level;
	uintptr_t root = property->root;

	printf("### Traversal of GPU-side tree ###\n");
	printf("Level of tree: %d\n", level);
	if (level <= 1)
		_clbptPrintMirror(0, (clbpt_leafmirror *)root);
	else 
		_clbptPrintNode(0, level, (clbpt_int_node *)root);
}

__kernel void
_clbptPrintTreeKernelWrapper(
	__global clbpt_property *property,
	__global struct clheap *heap
	)
{
	_clbptPrintTree(property);
}
*/
