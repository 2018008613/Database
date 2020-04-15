#include "buffer_manager.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>

FILE* fp[11];
page_t* header = NULL;
buffer** buf = NULL;
char** pathname_list;
int buffer_size;

void read_frame(int table_id, pagenum_t pagenum, page_t* src)
{
	fseek(fp[table_id], pagenum*page_size, SEEK_SET);
	fread(src, page_size, 1, fp[table_id]);
	src->this_num = pagenum;
}

int get_buffer_index(int table_id, pagenum_t pagenum)
{
	int idx = -1;
	for (int i = 0;i < buffer_size;i++)
	{
		if (buf[i] != NULL)
		{
			if (buf[i]->this_num == pagenum && buf[i]->table_id == table_id)
				return i;
		}
	}
	return idx;
}


int find_last_LRU_index()
{
	int idx = -1;
	for (int i = 0;i < buffer_size;i++)
	{
		if (buf[i] != NULL)
		{
			if (buf[i]->nextLRU == NULL)
				return i;
		}
	}
	return idx;
}

int find_first_LRU_index()
{
	int idx = -1;
	for (int i = 0;i < buffer_size;i++)
	{
		if (buf[i] != NULL)
		{
			if (buf[i]->preLRU == NULL)
				return i;
		}
	}
	return idx;
}

pagenum_t file_alloc_page(int table_id)
{
	pagenum_t num;
	//free page가 존재하지 않는 경우
	if (header->free_num == 0)
	{
		page_t* tmp = (page_t*)malloc(sizeof(page_t));
		tmp->next_freepg = 0;
		num = header->page_num;
		tmp->this_num = num;
		(header->page_num)++;
		file_write_page(table_id, num, tmp);
		file_write_page(table_id, 0, header);
		//
		free(tmp);
		return num;
	}
	//free page가 존재하는 경우
	num = header->free_num;
	page_t* tmp = (page_t*)malloc(sizeof(page_t));
	tmp->this_num = num;
	file_read_page(table_id, num, tmp);
	header->free_num = tmp->next_freepg;
	file_write_page(table_id, 0, header);
	//
	free(tmp);
	return num;
}

void file_read_page(int table_id, pagenum_t pagenum, page_t* tmp)
{
	int idx = get_buffer_index(table_id, pagenum);
	if (idx == -1)
	{
		read_frame(table_id, pagenum, tmp);
		file_write_page(table_id, pagenum, tmp);
	}
	else
		memcpy(tmp, &(buf[idx]->Framev), page_size);
}

void file_write_page(int table_id, pagenum_t pagenum, page_t* fr)
{
	int this_idx = get_buffer_index(table_id, pagenum);
	if (this_idx != -1)
	{
		memcpy(&(buf[this_idx]->Framev), fr, page_size);
		buf[this_idx]->is_dirty = 1;
	}
	else
	{
		for (int i = 0;i < buffer_size;i++)
		{
			if (buf[i] == NULL)
			{
				buf[i] = (buffer*)malloc(sizeof(buffer));
				memcpy(&(buf[i]->Framev), fr, page_size);
				buf[i]->table_id = table_id;
				buf[i]->this_num = pagenum;
				buf[i]->is_dirty = 0;
				buf[i]->is_pinned = 0;
				buf[i]->preLRU = buf[i];
				buf[i]->nextLRU = buf[i];
				buf[i]->page_mutex = PTHREAD_MUTEX_INITIALIZER;
				int last_idx = find_last_LRU_index();
				if (last_idx == -1)
				{
					buf[i]->preLRU = NULL;
					buf[i]->nextLRU = NULL;
				}
				else
				{
					buf[i]->preLRU = buf[last_idx];
					buf[last_idx]->nextLRU = buf[i];
					buf[i]->nextLRU = NULL;
				}
				return;
			}
		}
		int first_idx = find_first_LRU_index();
		buffer* victim_buffer = buf[first_idx];
		while (victim_buffer != NULL && victim_buffer->is_pinned != 0)
			victim_buffer = victim_buffer->nextLRU;
		if (victim_buffer == NULL)
		{
			fseek(fp[table_id], pagenum*page_size, SEEK_SET);
			fwrite(fr, page_size, 1, fp[table_id]);
			fflush(fp[table_id]);
			return;
		}
		pagenum_t pgnum = victim_buffer->this_num;
		fseek(fp[victim_buffer->table_id], pgnum*page_size, SEEK_SET);
		fwrite(&(victim_buffer->Framev), page_size, 1, fp[victim_buffer->table_id]);
		fflush(fp[victim_buffer->table_id]);
		int victim_idx = get_buffer_index(victim_buffer->table_id, victim_buffer->this_num);
		if (victim_buffer->preLRU != NULL)
			victim_buffer->preLRU->nextLRU = victim_buffer->nextLRU;
		if (victim_buffer->nextLRU != NULL)
			victim_buffer->nextLRU->preLRU = victim_buffer->preLRU;
		if (victim_buffer->nextLRU != NULL)
		{
			int last_idx = find_last_LRU_index();
			buffer* last_buf = buf[last_idx];
			last_buf->nextLRU = victim_buffer;
			victim_buffer->preLRU = last_buf;
			victim_buffer->nextLRU = NULL;
		}
		memcpy(&(victim_buffer->Framev), fr, page_size);
		victim_buffer->table_id = table_id;
		victim_buffer->this_num = pagenum;
		victim_buffer->is_dirty = 0;
		victim_buffer->is_pinned = 0;
	}
}

void file_free_page(int table_id, pagenum_t pagenum)
{
	page_t tmp;
	tmp.next_freepg = header->free_num;
	tmp.this_num = pagenum;
	header->free_num = pagenum;
	file_write_page(table_id, pagenum, &tmp);
	file_write_page(table_id, 0, header);
}
