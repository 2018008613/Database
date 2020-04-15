#include "log_manager.h"
#include "bpt.h"
#include "buffer_manager.h"
#include <algorithm>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

FILE* fpL = NULL;
extern int buffer_size;
log_buffer** log_buf;
log_record* log_header;
extern FILE* fp[11];
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

void recovery()
{
	//log_file이 이미 존재할 때만 recovery를 시행
	if ((fpL = fopen("log_file", "r+b")) != NULL)
	{
		int max_trx = 0;
		int loser_list[10001];
		for (int i = 0;i <= 10000;i++)
			loser_list[i] = 0;
		vector<int> vec[10000];
		for (int i = 1;i <= 10;i++)
		{
			char ch[10] = "DATA";
			ch[4] = '0' + i;
			ch[5] = '\0';
			open_table(ch);
		}
		read_log(0, log_header);
		int this_LSN = 100;
		while (this_LSN != log_header->next_LSN)
		{
			log_record tmp_log;
			file_read_log(this_LSN, &tmp_log);
			max_trx = max(max_trx, (int)tmp_log.trx_id);
			if (tmp_log.type == 0)
				loser_list[tmp_log.trx_id] = 1;
			else if (tmp_log.type == 1)
			{
				redo_log(&tmp_log);
				vec[tmp_log.trx_id].push_back((int)tmp_log.LSN);
			}
			else if (tmp_log.type == 2 && loser_list[tmp_log.trx_id] != 2)
				loser_list[tmp_log.trx_id] = 0;
			else
				loser_list[tmp_log.trx_id] = 2;
			this_LSN += 100;
		}
		for (int i = max_trx;i > 0;i--)
		{
			if (loser_list[i] != 0)
			{
				log_record tmp_log;
				for (int j = vec[i].size() - 1;j >= 0;j--)
				{
					this_LSN = vec[i][j];
					file_read_log(this_LSN, &tmp_log);
					undo_log(&tmp_log);
				}
			}
		}
	}
	else
	{
		fpL = fopen("log_file", "w+b");
		log_header = (log_record*)malloc(sizeof(log_record));
		log_header->LSN = 0;
		log_header->next_LSN = 100;
		file_write_log(0, log_header);
	}
	for (int i = 1;i <= 10;i++)
		close_table(i);
	flush_log();
}

void redo_log(log_record* log)
{
	page_t pg;
	file_read_page(log->table_id, log->pgnum, &pg);
	if (pg.page_LSN >= log->LSN)
		return;
	pg.page_LSN = log->LSN;
	strcpy(pg.record[log->offset].value, log->new_img);
	file_write_page(log->table_id, log->pgnum, &pg);
}

void undo_log(log_record* log)
{
	page_t pg;
	file_read_page(log->table_id, log->pgnum, &pg);
	if (pg.page_LSN < log->LSN)
		return;
	pg.page_LSN = log->LSN;
	strcpy(pg.record[log->offset].value, log->old_img);
	file_write_page(log->table_id, log->pgnum, &pg);
}

void flush_log()
{
	for (int i = 0;i < buffer_size;i++)
	{
		if (log_buf[i] == NULL)
			break;
		log_buf[i]->Framev;
		fseek(fpL, log_buf[i]->Framev.LSN * 100, SEEK_SET);
		fwrite(&(log_buf[i]->Framev), 100, 1, fpL);
		fflush(fpL);
	}
}

void read_log(uint64_t LSN, log_record* src)
{
	fseek(fpL, LSN * 100, SEEK_SET);
	fread(src, 100, 1, fpL);
}

int get_log_index(uint64_t LSN)
{
	int idx = -1;
	for (int i = 0;i < buffer_size;i++)
	{
		if (log_buf[i] != NULL)
		{
			if (log_buf[i]->Framev.LSN == LSN)
				return i;
		}
	}
	return idx;
}


int find_last_log_LRU_index()
{
	int idx = -1;
	for (int i = 0;i < buffer_size;i++)
	{
		if (log_buf[i] != NULL)
		{
			if (log_buf[i]->nextLRU == NULL)
				return i;
		}
	}
	return idx;
}

int find_first_log_LRU_index()
{
	int idx = -1;
	for (int i = 0;i < buffer_size;i++)
	{
		if (log_buf[i] != NULL)
		{
			if (log_buf[i]->preLRU == NULL)
				return i;
		}
	}
	return idx;
}

void file_read_log(uint64_t LSN, log_record* tmp)
{
	int idx = get_log_index(LSN);
	if (idx == -1)
	{
		read_log(LSN, tmp);
		file_write_log(LSN, tmp);
	}
	else
		memcpy(tmp, &(log_buf[idx]->Framev), 100);
}

void file_write_log(uint64_t LSN, log_record* fr)
{
	int this_idx = get_log_index(LSN);
	if (this_idx != -1)
	{
		memcpy(&(log_buf[this_idx]->Framev), fr, 100);
		log_buf[this_idx]->is_dirty = 1;
	}
	else
	{
		for (int i = 0;i < buffer_size;i++)
		{
			if (log_buf[i] == NULL)
			{
				log_buf[i] = (log_buffer*)malloc(sizeof(log_buffer));
				memcpy(&(log_buf[i]->Framev), fr, 100);
				log_buf[i]->is_dirty = 0;
				log_buf[i]->is_pinned = 0;
				log_buf[i]->preLRU = log_buf[i];
				log_buf[i]->nextLRU = log_buf[i];
				int last_idx = find_last_log_LRU_index();
				if (last_idx == -1)
				{
					log_buf[i]->preLRU = NULL;
					log_buf[i]->nextLRU = NULL;
				}
				else
				{
					log_buf[i]->preLRU = log_buf[last_idx];
					log_buf[last_idx]->nextLRU = log_buf[i];
					log_buf[i]->nextLRU = NULL;
				}
				return;
			}
		}
		int first_idx = find_first_log_LRU_index();
		log_buffer* victim_buffer = log_buf[first_idx];
		while (victim_buffer != NULL && victim_buffer->is_pinned != 0)
			victim_buffer = victim_buffer->nextLRU;
		if (victim_buffer == NULL)
		{
			fseek(fpL, LSN*100, SEEK_SET);
			fwrite(fr, 100, 1, fpL);
			fflush(fpL);
			return;
		}
		pagenum_t this_LSN = victim_buffer->Framev.LSN;
		fseek(fpL, this_LSN * 100, SEEK_SET);
		fwrite(&(victim_buffer->Framev), 100, 1, fpL);
		fflush(fpL);
		int victim_idx = get_log_index(this_LSN);
		if (victim_buffer->preLRU != NULL)
			victim_buffer->preLRU->nextLRU = victim_buffer->nextLRU;
		if (victim_buffer->nextLRU != NULL)
			victim_buffer->nextLRU->preLRU = victim_buffer->preLRU;
		if (victim_buffer->nextLRU != NULL)
		{
			int last_idx = find_last_log_LRU_index();
			log_buffer* last_buf = log_buf[last_idx];
			last_buf->nextLRU = victim_buffer;
			victim_buffer->preLRU = last_buf;
			victim_buffer->nextLRU = NULL;
		}
		memcpy(&(victim_buffer->Framev), fr, 100);
		victim_buffer->is_dirty = 0;
		victim_buffer->is_pinned = 0;
	}
}
