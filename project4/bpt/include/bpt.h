#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct pghead {
	uint32_t isleaf;
	uint32_t key_num;
}pghead;

typedef struct record {
	uint64_t key;
	char value[120];
}record_t;


typedef struct branch {
	uint64_t key;
	uint64_t pgnum;
}branch_t;

typedef struct paget {
	union
	{
		uint64_t free_num;
		uint64_t next_freepg;
		uint64_t parent;
	};
	union
	{
		uint64_t root_num;
		pghead head_info;
	};
	uint64_t page_num;
	uint64_t this_num;
	char Reserved[88];
	union
	{
		uint64_t right_sibling;
		uint64_t left_page;
	};
	union
	{
		record_t record[31];
		branch_t branch[248];
	};
}page_t;
typedef struct buffer_t buffer;

struct buffer_t {
	page_t Framev;
	int table_id;
	uint64_t this_num;
	int is_dirty;
	int is_pinned;
	buffer* nextLRU;
	buffer* preLRU;
};

//file manager API

typedef uint64_t pagenum_t;

#define page_size 4096


typedef struct queue
{
	uint64_t* arr;
	int f;
	int r;
}queue;


void read_frame(int table_id, pagenum_t pagenum, page_t* src);
int get_buffer_index(int table_id, pagenum_t pagenum);
int find_last_LRU_index();
int find_first_LRU_index();

pagenum_t file_alloc_page(int table_id);
void file_read_page(int table_id, pagenum_t pagenum, page_t* tmp);
void file_write_page(int table_id, pagenum_t pagenum, page_t* fr);
void file_free_page(int table_id, pagenum_t pagenum);

int path_to_root(int table_id, uint64_t pgnum);
void enqueue(uint64_t pgnum, queue* q);
uint64_t dequeue(queue* q);
void print_tree(int table_id);

int open_table(char *pathname);
int close_table(int table_id);
int init_db(int num_buf);
int shutdown_db();

int join_table(int table_id_1, int table_id_2, char * pathname);
//Find
pagenum_t find_leaf(int table_id, int64_t key);
char* find_in_function(int table_id, int64_t key);
char* find(int table_id, int64_t key);
int cut(int length);

//Insert
record_t* make_record(int64_t key, char* value);
branch_t* make_branch(int64_t key, pagenum_t pgnum);
page_t* make_leaf();
page_t* make_internal();
void insert_into_leaf(int table_id, page_t* leaf, int64_t key, record_t* record);
void insert_into_leaf_after_splitting(int table_id, page_t* leaf, int64_t key, record_t* record);
void insert_into_node(int table_id, page_t* parent, pagenum_t right_pgnum, int64_t key);
void insert_into_node_after_splitting(int table_id, page_t* parent, pagenum_t right_pgnum, int64_t key);
void insert_into_parent(int table_id, page_t* left, page_t* right, int64_t key);
void insert_into_new_root(int table_id, page_t* left, page_t* right, int64_t key);
void start_new_tree(int table_id, int64_t key, record_t* record);
int insert(int table_id, int64_t key, char* value);

//Delete
int get_neighbor_index(int table_id, page_t* parent, pagenum_t pgnum);
void remove_entry_from_node(int table_id, page_t* n, int64_t key);
void adjust_root(int table_id, page_t* root);
void coalesce_nodes(int table_id, page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int64_t k_prime);
void redistribute_nodes(int table_id, page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int k_prime_index);
void delete_entry(int table_id, page_t* n, int64_t key);
int delete(int table_id, int64_t key);

#endif /* __BPT_H__*/

