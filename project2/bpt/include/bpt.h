#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

//file manager API

typedef uint64_t pagenum_t;

#define page_size 4096

typedef struct header {
	uint64_t free_num;
	uint64_t root_num;
	uint64_t page_num;
	char Reserved[4072];
}header_t;

typedef struct freepg {
	uint64_t next_freepg;
	char Reserved[4088];
}freepg_t;

typedef struct record {
	uint64_t key;
	char value[120];
}record_t;

typedef struct leafpg {
	uint64_t parent;
	uint32_t isleaf;
	uint32_t key_num;
	char Reserved[104];
	uint64_t right_sibling;
	record_t record[31];
}leafpg_t;

typedef struct branch {
	uint64_t key;
	uint64_t pgnum;
}branch_t;

typedef struct internalpg {
	uint64_t parent;
	uint32_t isleaf;
	uint32_t key_num;
	char Reserved[104];
	uint64_t left_page;
	branch_t branch[248];
}internalpg_t;

typedef struct nonfreepg {
	uint64_t parent;
	uint32_t isleaf;
	uint32_t key_num;
	char Reserved[104];
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
}nonfreepg_t;

typedef struct paget {
	// in-memory page structure
	uint64_t this_num;
	//header
	uint64_t free_num;
	uint64_t root_num;
	uint64_t page_num;
	//freepg
	uint64_t next_freepg;
	//page header
	uint64_t parent;
	uint32_t isleaf;
	uint32_t key_num;
	//leaf or internal
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

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src);

typedef struct queue
{
	uint64_t* arr;
	int f;
	int r;
}queue;

int path_to_root(uint64_t pgnum);
void enqueue(uint64_t offset, queue* q);
uint64_t dequeue(queue* q);
void print_tree();

pagenum_t find_leaf(int64_t key);
char* find(int64_t key);
int db_find(int64_t key, char * ret_val);
int cut(int length);

int open_table(char *pathname);

// Insertion.

record_t* make_record(int64_t key, char* value);
branch_t* make_branch(int64_t key, pagenum_t pgnum);
page_t* make_leaf();
page_t* make_internal();

void insert_into_leaf(page_t* leaf, int64_t key, record_t* record);
void insert_into_leaf_after_splitting(page_t* leaf, int64_t key, record_t* record);
void insert_into_node(page_t* parent, pagenum_t right_pgnum, int64_t key);
void insert_into_node_after_splitting(page_t* parent, pagenum_t right_pgnum, int64_t key);
void insert_into_parent(page_t* left, page_t* right, int64_t key);
void insert_into_new_root(page_t* left, page_t* right, int64_t key);
void start_new_tree(int64_t key, record_t* record);
int db_insert(int64_t key, char* value);

// Deletion.

int get_neighbor_index(page_t* parent, pagenum_t pgnum);
void remove_entry_from_node(page_t* n, int64_t key);
void adjust_root(page_t* root);
void coalesce_nodes(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int64_t k_prime);
void redistribute_nodes(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int k_prime_index);
void delete_entry(page_t* n, int64_t key);
int db_delete(int64_t key);

#endif /* __BPT_H__*/

