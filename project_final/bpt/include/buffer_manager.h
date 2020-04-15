#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

using namespace std;

typedef uint64_t pagenum_t;

#define page_size 4096

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
	uint64_t page_LSN;
	uint64_t this_num;
	char Reserved[80];
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
	pthread_mutex_t page_mutex;
};

void read_frame(int table_id, pagenum_t pagenum, page_t* src);
int get_buffer_index(int table_id, pagenum_t pagenum);
int find_last_LRU_index();
int find_first_LRU_index();

pagenum_t file_alloc_page(int table_id);
void file_read_page(int table_id, pagenum_t pagenum, page_t* tmp);
void file_write_page(int table_id, pagenum_t pagenum, page_t* fr);
void file_free_page(int table_id, pagenum_t pagenum);


#endif
