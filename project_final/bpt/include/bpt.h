#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include "buffer_manager.h"
#include "txn_manager.h"
#include <stdbool.h>
#include <stdlib.h>

//file manager API

typedef struct queue_t
{
	uint64_t* arr;
	int f;
	int r;
}queue_t;

int db_find(int table_id, int64_t key, char* ret_val, int trx_id);
int db_update(int table_id, int64_t key, char* values, int trx_id);

int path_to_root(int table_id, uint64_t pgnum);
void enqueue(uint64_t pgnum, queue_t* q);
uint64_t dequeue(queue_t* q);
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
int db_delete(int table_id, int64_t key);

#endif /* __BPT_H__*/

