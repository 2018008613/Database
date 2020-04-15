#ifndef __LOG_MANAGER_H__
#define __LOG_MANAGER_H__

#include <stdint.h>

typedef struct log_record{
	uint64_t LSN;
	union
	{
		uint64_t pre_LSN;
		uint64_t next_LSN;
	};
	uint32_t trx_id;
	uint32_t type;
	uint32_t table_id;
	uint32_t pgnum;
	uint32_t offset;
	uint32_t length;
	char old_img[30];
	char new_img[30];
}log_record;
//type
//BEGIN(0)
//UPDATE(1)
//COMMIT(2)
//ABORT(3)


typedef struct log_buffer log_buffer;

struct log_buffer {
	log_record Framev;
	int is_dirty;
	int is_pinned;
	log_buffer* nextLRU;
	log_buffer* preLRU;
};

void recovery();
void redo_log(log_record* log);
void undo_log(log_record* log);
void flush_log();
void read_log(uint64_t LSN, log_record* src);
int get_log_index(uint64_t LSN);
int find_last_log_LRU_index();
int find_first_log_LRU_index();
void file_read_log(uint64_t LSN, log_record* tmp);
void file_write_log(uint64_t LSN, log_record* fr);
	
#endif
