#include "bpt.h"
#include <string.h>

FILE* fp[11];
page_t* header = NULL;
buffer** buf = NULL;
int buffer_size;
char** pathname_list;


void read_frame(int table_id, pagenum_t pagenum, page_t* src)
{
	fseek(fp[table_id], pagenum*page_size, SEEK_SET);
	fread(src, page_size, 1, fp[table_id]);
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


//

//페이지를 할당해준다. free page가 존재하면 free page를 할당해주고
//free page가 존재하지 않으면 새로운 페이지를 만들어 할당해준 뒤 그 page의 번호를 return해준다

//record는 31칸, branch는 248칸짜리 배열
int leaf_order = 32;
int internal_order = 249;

//print tree를 위한 함수들
int path_to_root(int table_id, uint64_t pgnum)
{
	page_t* tmp = (page_t*)malloc(sizeof(page_t));
	file_read_page(table_id, 0, header);
	uint64_t next_page = pgnum;
	int level = 0;
	while (next_page != header->root_num)
	{
		file_read_page(table_id, next_page, tmp);
		next_page = tmp->parent;
		level++;
	}
	free(tmp);
	return level;
}

void enqueue(uint64_t pgnum, queue* q)
{
	q->arr[++(q->r)] = pgnum;
}

uint64_t dequeue(queue* q)
{
	return q->arr[(q->f)++];
}


void print_tree(int table_id)
{
	file_read_page(table_id, 0, header);
	if (header->root_num == 0)
	{
		printf("Empty tree\n");
		return;
	}

	queue* q = (queue*)malloc(sizeof(queue));
	q->arr = (uint64_t*)malloc(sizeof(uint64_t) * 300000);
	q->f = 0;
	q->r = -1;

	enqueue(header->root_num, q);

	page_t page, parentpg;
	int level = 0;
	while (q->f <= q->r)
	{
		uint64_t pgnum = dequeue(q);
		file_read_page(table_id, pgnum, &page);

		if (page.parent != 0)
		{
			file_read_page(table_id, page.parent, &parentpg);
			if (path_to_root(table_id, pgnum) != level)
			{
				level = path_to_root(table_id, pgnum);
				printf("\n");
			}
		}

		if (!(page.head_info.isleaf))
		{
			for (int i = 0; i < page.head_info.key_num; ++i)
				printf("%ld  ", page.branch[i].key);
			enqueue(page.left_page, q);
			for (int i = 0; i < page.head_info.key_num; ++i)
				enqueue(page.branch[i].pgnum, q);
		}
		else
		{
			for (int i = 0; i < page.head_info.key_num; ++i)
				printf("%ld : %s  ", page.record[i].key, page.record[i].value);
		}
		printf("| ");
	}
	printf("\n");
	free(q->arr);
	free(q);
}

//pathname을 열어준다
int open_table(char *pathname)
{
	int table_id = -1;
	for (int i = 1;i <= 10;i++)
	{
		if (pathname_list[i] == NULL)
		{
			table_id = i;
			break;
		}
	}
	if (table_id == -1)
		return -1;
	//pathname이 이미 존재할 때
	if ((fp[table_id] = fopen(pathname, "r+b")) != NULL)
	{
		printf("%s already exist\n", pathname);
		if(header == NULL)
			header = (page_t*)malloc(sizeof(page_t));
		file_read_page(table_id, 0, header);
		pathname_list[table_id] = (char*)malloc(sizeof(char));
		strcpy(pathname_list[table_id], pathname);
		return table_id;
	}
	//존재하지 않을 때
	fp[table_id] = fopen(pathname, "w+b");
	printf("Creating %s\n", pathname);
	if(header == NULL)
		header = (page_t*)malloc(sizeof(page_t));
	header->free_num = 0;
	header->root_num = 0;
	header->page_num = 1;
	header->this_num = 0;
	header->head_info.key_num = 0;
	file_write_page(table_id, 0, header);
	pathname_list[table_id] = (char*)malloc(sizeof(char) * 100);
	strcpy(pathname_list[table_id], pathname);
	return table_id;
}

int close_table(int table_id)
{
	if (pathname_list[table_id] == NULL)
		return 1;
	free(pathname_list[table_id]);
	pathname_list[table_id] = NULL;
	for (int i = 0;i < buffer_size;i++)
	{
		if (buf[i] != NULL && buf[i]->table_id == table_id)
		{
			fseek(fp[table_id], buf[i]->this_num *page_size, SEEK_SET);
			fwrite(&(buf[i]->Framev), page_size, 1, fp[table_id]);
			fflush(fp[table_id]);
			if (buf[i]->preLRU != NULL)
				buf[i]->preLRU->nextLRU = buf[i]->nextLRU;
			if (buf[i]->nextLRU != NULL)
				buf[i]->nextLRU->preLRU = buf[i]->preLRU;
			free(buf[i]);
			buf[i] = NULL;
		}
	}
	fclose(fp[table_id]);
	fp[table_id] = NULL;
	return 0;
}

int init_db(int num_buf)
{
	buffer_size = num_buf;
	pathname_list = (char**)malloc(sizeof(char*) * 11);
	for (int i = 1;i <= 10;i++)
	{
		buf = (buffer**)malloc(sizeof(buffer*)*num_buf);
		pathname_list[i] = NULL;
		if (buf == NULL)
		{
			printf("Create buffer failed.\n");
			return 1;
		}
		for (int j = 0;j < num_buf;j++)
			buf[j] = NULL;
	}
	return 0;
}

int shutdown_db()
{
	if (buf != NULL)
	{
		free(buf);
		buf = NULL;
	}

	return 0;
}

int join_table(int table_id_1, int table_id_2, char * pathname)
{
	page_t t1, t2;
	if (pathname_list[table_id_1] == NULL || pathname_list[table_id_2] == NULL)
		return 1;
	file_read_page(table_id_1, 0, &t1);
	file_read_page(table_id_2, 0, &t2);
	if (t1.root_num == 0)
		return 1;
	if (t2.root_num == 0)
		return 1;
	FILE* fp1 = fopen(pathname, "w+b");
	file_read_page(table_id_1, t1.root_num, &t1);
	file_read_page(table_id_2, t2.root_num, &t2);
	while(!t1.head_info.isleaf)
		file_read_page(table_id_1, t1.left_page, &t1);
	while (!t2.head_info.isleaf)
		file_read_page(table_id_2, t2.left_page, &t2);
	int i = 0, j = 0;
	while (1)
	{
		if (t1.record[i].key == t2.record[j].key)
		{
			fprintf(fp1, "%d,%s,%d,%s\n", t1.record[i].key, t1.record[i].value, t2.record[j].key, t2.record[j].value);
			i++;
			j++;
			if (i == t1.head_info.key_num && t1.right_sibling == 0)
			{
				fclose(fp1);
				return 0;
			}
			else if (i == t1.head_info.key_num)
			{
				file_read_page(table_id_1, t1.right_sibling, &t1);
				i = 0;
			}
			if (j == t2.head_info.key_num && t2.right_sibling == 0)
			{
				fclose(fp1);
				return 0;
			}
			else if (j == t2.head_info.key_num)
			{
				file_read_page(table_id_2, t2.right_sibling, &t2);
				j = 0;
			}
		}
		else if (t1.record[i].key > t2.record[j].key)
		{
			j++;
			if (j == t2.head_info.key_num && t2.right_sibling == 0)
			{
				fclose(fp1);
				return 0;
			}
			else if (j == t2.head_info.key_num)
			{
				file_read_page(table_id_2, t2.right_sibling, &t2);
				j = 0;
			}
		}
		else
		{
			i++;
			if (i == t1.head_info.key_num && t1.right_sibling == 0)
			{
				fclose(fp1);
				return 0;
			}
			else if (i == t1.head_info.key_num)
			{
				file_read_page(table_id_1, t1.right_sibling, &t1);
				i = 0;
			}
		}
	}
	return 0;
}


//key 값이 들어있을 수 있는 leaf의 page_num을 return해준다
pagenum_t find_leaf(int table_id, int64_t key)
{
	//root가 없는 경우
	pagenum_t root_num = header->root_num;
	if (root_num == 0)
		return 0;
	//num에 root page를 할당, node에 root page의 정보를 할당
	pagenum_t num = root_num;
	page_t* node = (page_t*)malloc(sizeof(page_t));
	file_read_page(table_id, num, node);
	//node가 internal page인 경우 반복
	while (!(node->head_info.isleaf))
	{
		int idx = -2;
		for (int i = 0;i < node->head_info.key_num;i++)
		{
			if (node->branch[i].key > key)
			{
				idx = i - 1;
				break;
			}
		}
		//첫 key값보다 작을 경우
		if (idx == -1)
			num = node->left_page;
		//마지막 key값보다 클 경우
		else if (idx == -2)
			num = node->branch[node->head_info.key_num - 1].pgnum;
		else
			num = node->branch[idx].pgnum;
		file_read_page(table_id, num, node);
	}
	//
	free(node);
	return num;
}

//key 값이 없으면 NULL을 return, 그렇지 않으면 key 값에 해당하는 value 값을 return
char* find_in_function(int table_id, int64_t key)
{
	//num에 key가 들어있을 수 있는 leaf의 page를 할당
	pagenum_t num = find_leaf(table_id, key);
	page_t* tmp = (page_t*)malloc(sizeof(page_t));
	//root node가 없을 경우
	if (num == 0)
	{
		//
		free(tmp);
		return NULL;
	}
	file_read_page(table_id, num, tmp);
	//key값이 있는 idx를 찾는다
	int idx = -1;
	for (int i = 0;i < tmp->head_info.key_num;i++)
	{
		if (key == tmp->record[i].key)
		{
			idx = i;
			break;
		}
	}
	//key 값이 들어있지 않은 경우
	if (idx == -1)
		return NULL;
	char* value = (char*)malloc(sizeof(char) * 120);
	strcpy(value, tmp->record[idx].value);
	//
	free(tmp);
	return value;
}

char* find(int table_id, int64_t key)
{
	//num에 key가 들어있을 수 있는 leaf의 page를 할당
	file_read_page(table_id, 0, header);
	pagenum_t num = find_leaf(table_id, key);
	page_t* tmp = (page_t*)malloc(sizeof(page_t));
	//root node가 없을 경우
	if (num == 0)
	{
		//
		free(tmp);
		return NULL;
	}
	file_read_page(table_id, num, tmp);
	//key값이 있는 idx를 찾는다
	int idx = -1;
	for (int i = 0;i < tmp->head_info.key_num;i++)
	{
		if (key == tmp->record[i].key)
		{
			idx = i;
			break;
		}
	}
	//key 값이 들어있지 않은 경우
	if (idx == -1)
		return NULL;
	char* value = (char*)malloc(sizeof(char) * 120);
	strcpy(value, tmp->record[idx].value);
	//
	free(tmp);
	return value;
}

//length를 반으로 나누어 할당해준다
int cut(int length) {
	if (length % 2 == 0)
		return length / 2;
	else
		return length / 2 + 1;
}


// INSERTION

//새로운 key, value를 갖는 record를 만들어준다
record_t* make_record(int64_t key, char* value)
{
	record_t * new_record = (record_t*)malloc(sizeof(record_t));
	if (new_record == NULL)
	{
		perror("create record failed.");
		exit(EXIT_FAILURE);
	}
	new_record->key = key;
	strcpy(new_record->value, value);
	return new_record;
}
//새로운 branch를 만들어 return해준다
branch_t* make_branch(int64_t key, pagenum_t pgnum)
{
	
	branch_t* new_branch = (branch_t*)malloc(sizeof(branch_t));
	if (new_branch == NULL)
	{
		perror("create branch failed.");
		exit(EXIT_FAILURE);
	}
	new_branch->key = key;
	new_branch->pgnum = pgnum;
	return new_branch;
}
//새로운 leaf page를 만들어서 return해준다.
page_t* make_leaf()
{
	page_t* new_leaf = (page_t*)malloc(sizeof(page_t));
	if (new_leaf == NULL)
	{
		perror("create leaf failed.");
		exit(EXIT_FAILURE);
	}
	new_leaf->parent = 0;
	new_leaf->head_info.isleaf = 1;
	new_leaf->head_info.key_num = 0;
	new_leaf->right_sibling = 0;
	return new_leaf;
}
//새로운 internal page를 만들어 return해준다
page_t* make_internal()
{
	page_t* new_internal = (page_t*)malloc(sizeof(page_t));
	if (new_internal == NULL)
	{
		perror("create internal failed.");
		exit(EXIT_FAILURE);
	}
	new_internal->parent = 0;
	new_internal->head_info.isleaf = 0;
	new_internal->head_info.key_num = 0;
	new_internal->left_page = 0;
	return new_internal;
}
//leaf에 record를 적당한 위치에 넣은 뒤 file에 write해준다.
void insert_into_leaf(int table_id, page_t* leaf, int64_t key, record_t* record)
{
	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->head_info.key_num && leaf->record[insertion_point].key < key)
		insertion_point++;
	for (i = leaf->head_info.key_num; i > insertion_point; i--) 
	{
		leaf->record[i].key = leaf->record[i - 1].key;
		strcpy(leaf->record[i].value, leaf->record[i - 1].value);
	}
	leaf->record[insertion_point].key = key;
	strcpy(leaf->record[insertion_point].value, record->value);
	(leaf->head_info.key_num)++;
	file_write_page(table_id, leaf->this_num, leaf);
}


//leaf page에 insert할 때 overflow가 일어나는 경우
void insert_into_leaf_after_splitting(int table_id, page_t* leaf, int64_t key, record_t* record)
{
	page_t* new_leaf = make_leaf();
	record_t* tmp = (record_t*)malloc(sizeof(record_t)*leaf_order);
	if (tmp == NULL)
	{
		perror("make tmp failed");
		exit(EXIT_FAILURE);
	}
	int insertion_index, split, i, j;
	int64_t new_key;
	insertion_index = 0;
	while (insertion_index < leaf_order - 1 && leaf->record[insertion_index].key < key)
		insertion_index++;
	for (i = 0, j = 0; i < leaf->head_info.key_num; i++, j++) {
		if (j == insertion_index) j++;
		tmp[j].key = leaf->record[i].key;
		strcpy(tmp[j].value, leaf->record[i].value);
	}
	tmp[insertion_index].key = key;
	strcpy(tmp[insertion_index].value, record->value);

	leaf->head_info.key_num = 0;

	split = cut(leaf_order - 1);

	for (i = 0; i < split; i++) {
		leaf->record[i].key = tmp[i].key;
		strcpy(leaf->record[i].value, tmp[i].value);
		(leaf->head_info.key_num)++;
	}

	for (i = split, j = 0; i < leaf_order; i++, j++) {
		new_leaf->record[j].key = tmp[i].key;
		strcpy(new_leaf->record[j].value, tmp[i].value);
		(new_leaf->head_info.key_num)++;
	}

	free(tmp);

	new_leaf->this_num = file_alloc_page(table_id);
	new_leaf->right_sibling = leaf->right_sibling;
	leaf->right_sibling = new_leaf->this_num;

	new_leaf->parent = leaf->parent;
	file_write_page(table_id, leaf->this_num, leaf);
	file_write_page(table_id, new_leaf->this_num, new_leaf);
	
	new_key = new_leaf->record[0].key;
	int newidx = get_buffer_index(table_id, new_leaf->this_num);
	if (newidx != -1)
	{
		(buf[newidx]->is_pinned)++;
		insert_into_parent(table_id, leaf, new_leaf, new_key);
		(buf[newidx]->is_pinned)--;
	}
	else
		insert_into_parent(table_id, leaf, new_leaf, new_key);
	free(new_leaf);
}

//overflow되지 않을 때 internal node에 insert.
void insert_into_node(int table_id, page_t* parent, pagenum_t right_pgnum, int64_t key)
{
	int insert_position = 0;
	while (insert_position < parent->head_info.key_num && parent->branch[insert_position].key < key)
		insert_position++;
	for (int i = parent->head_info.key_num; i > insert_position; --i)
	{
		parent->branch[i].key = parent->branch[i - 1].key;
		parent->branch[i].pgnum = parent->branch[i - 1].pgnum;
	}
	parent->branch[insert_position].key = key;
	parent->branch[insert_position].pgnum = right_pgnum;
	(parent->head_info.key_num++);
	file_write_page(table_id, parent->this_num, parent);
}

//overflow될때 internal node에 insert 후 split해준다.
void insert_into_node_after_splitting(int table_id, page_t* parent, pagenum_t right_pgnum, int64_t key)
{
	int i, j, split, k_prime;
	branch_t* tmp = (branch_t*)malloc(sizeof(branch_t) * internal_order);
	if (tmp == NULL) {
		perror("create tmp failed.");
		exit(EXIT_FAILURE);
	}
	int insert_position = 0;
	while (insert_position < internal_order - 1 && parent->branch[insert_position].key < key)
		insert_position++;
	for (i = 0, j = 0; i < internal_order - 1; ++i, ++j)
	{
		if (j == insert_position) 
			j++;
		tmp[j].key = parent->branch[i].key;
		tmp[j].pgnum = parent->branch[i].pgnum;
	}
	tmp[insert_position].key = key;
	tmp[insert_position].pgnum = right_pgnum;

	page_t* new_internal = make_internal();
	parent->head_info.key_num = 0;
	split = cut(internal_order);
	for (int i = 0; i < split - 1; ++i)
	{
		parent->branch[i].key = tmp[i].key;
		parent->branch[i].pgnum = tmp[i].pgnum;
		parent->head_info.key_num++;
	}
	for (i = split, j = 0; i < internal_order; ++i, ++j)
	{
		new_internal->branch[j].key = tmp[i].key;
		new_internal->branch[j].pgnum = tmp[i].pgnum;
		(new_internal->head_info.key_num)++;
	}
	new_internal->left_page = tmp[split - 1].pgnum;
	new_internal->parent = parent->parent;

	new_internal->this_num = file_alloc_page(table_id);
	file_write_page(table_id, new_internal->this_num, new_internal);
	file_write_page(table_id, parent->this_num, parent);

	page_t* child = (page_t*)malloc(sizeof(page_t));
	file_read_page(table_id, new_internal->left_page,child);
	child->parent = new_internal->this_num;
	file_write_page(table_id, new_internal->left_page, child);
	for (i = 0; i < new_internal->head_info.key_num; ++i)
	{
		file_read_page(table_id, new_internal->branch[i].pgnum, child);
		child->parent = new_internal->this_num;
		file_write_page(table_id, new_internal->branch[i].pgnum, child);
	}
	int newidx = get_buffer_index(table_id, new_internal->this_num);
	if (newidx != -1)
	{
		(buf[newidx]->is_pinned)++;
		insert_into_parent(table_id, parent, new_internal, tmp[split - 1].key);
		(buf[newidx]->is_pinned)--;
	}
	else
		insert_into_parent(table_id, parent, new_internal, tmp[split - 1].key);
	free(tmp);
	free(new_internal);
	//
	free(child);
}
//parent에 key 값을 insert한다.
void insert_into_parent(int table_id, page_t* left, page_t* right, int64_t key)
{
	if (left->parent == 0)
		return insert_into_new_root(table_id, left, right, key);
	page_t* parent = (page_t*)malloc(sizeof(page_t));
	file_read_page(table_id, left->parent, parent);

	if (parent->head_info.key_num < internal_order - 1)
	{
		int idx = get_buffer_index(table_id, parent->this_num);
		if (idx != -1)
		{
			(buf[idx]->is_pinned)++;
			insert_into_node(table_id, parent, right->this_num, key);
			(buf[idx]->is_pinned)--;
		}
		else
			insert_into_node(table_id, parent, right->this_num, key);
	}
	else
	{
		int idx = get_buffer_index(table_id, parent->this_num);
		if (idx != -1)
		{
			(buf[idx]->is_pinned)++;
			insert_into_node_after_splitting(table_id, parent, right->this_num, key);
			(buf[idx]->is_pinned)--;
		}
		else
			insert_into_node_after_splitting(table_id, parent, right->this_num, key);
	}
	//
	free(parent);
}

//root를 split해서 새로운 root를 만드는 경우
void insert_into_new_root(int table_id, page_t* left, page_t* right, int64_t key)
{
	page_t* root = make_internal();
	root->left_page = left->this_num;
	root->branch[0].key = key;
	root->branch[0].pgnum = right->this_num;
	(root->head_info.key_num)++;

	root->this_num = file_alloc_page(table_id);
	left->parent = root->this_num;
	right->parent = root->this_num;
	header->root_num = root->this_num;
	file_write_page(table_id, left->this_num, left);
	file_write_page(table_id, right->this_num, right);
	file_write_page(table_id, root->this_num, root);
	file_write_page(table_id, 0, header);
	free(root);
}

//새로운 root를 만들어 tree를 생성한다.
void start_new_tree(int table_id, int64_t key, record_t* record)
{
	page_t* root = make_leaf();
	root->record[0].key = key;
	strcpy(root->record[0].value, record->value);
	(root->head_info.key_num)++;

	root->this_num = file_alloc_page(table_id);
	header->root_num = root->this_num;
	file_write_page(table_id, root->this_num, root);
	file_write_page(table_id, 0, header);
	free(root);
}

// Master insertion function.

int insert(int table_id, int64_t key, char* value)
{
	file_read_page(table_id, 0, header);
	//key값이 이미 존재하는 경우
	if (find_in_function(table_id, key) != NULL)
		return 1;

	record_t* new_record = make_record(key, value);

	//root가 존재하지 않는 경우
	if (header->root_num == 0)
	{
		start_new_tree(table_id, key, new_record);
		return 0;
	}

	pagenum_t leaf_page = find_leaf(table_id, key);
	page_t* leaf = (page_t*)malloc(sizeof(page_t));
	file_read_page(table_id, leaf_page, leaf);

	//overflow가 일어나지 않는 경우
	if (leaf->head_info.key_num < leaf_order - 1)
	{
		int leafidx = get_buffer_index(table_id, leaf->this_num);
		if (leafidx != -1)
		{
			(buf[leafidx]->is_pinned)++;
			insert_into_leaf(table_id, leaf, key, new_record);
			(buf[leafidx]->is_pinned)--;
		}
		else
			insert_into_leaf(table_id, leaf, key, new_record);
		free(new_record);
		//
		free(leaf);
		return 0;
	}

	//overflow가 일어나는 경우
	int leafidx = get_buffer_index(table_id, leaf->this_num);
	if (leafidx != -1)
	{
		buf[leafidx]->is_pinned = 1;
		insert_into_leaf_after_splitting(table_id, leaf, key, new_record);
		buf[leafidx]->is_pinned = 0;
	}
	else
		insert_into_leaf_after_splitting(table_id, leaf, key, new_record);
	free(new_record);
	//
	free(leaf);
	return 0;
}

// DELETION.

//pgnum의 왼쪽 노드의 index를 반환해준다. 가장 왼쪽이면 -1을 반환
int get_neighbor_index(int table_id, page_t* parent, pagenum_t pgnum)
{
	int i;
	if (parent->left_page == pgnum)
		return -2;
	for (i = 0; i < parent->head_info.key_num; i++)
		if (parent->branch[i].pgnum == pgnum)
			return i - 1;

	// Error state.
	printf("Search for nonexistent page to node in parent.\n");
	printf("Page:  %ld\n", pgnum);
	exit(EXIT_FAILURE);
}


//n에서 key를 제거한다.
void remove_entry_from_node(int table_id, page_t* n, int64_t key)
{
	int i, num_pointers;
	//leaf node
	if (n->head_info.isleaf)
	{
		i = 0;
		while (n->record[i].key != key)
			i++;
		for (++i; i < n->head_info.key_num; ++i)
		{
			n->record[i - 1].key = n->record[i].key;
			strcpy(n->record[i - 1].value, n->record[i].value);
		}
		(n->head_info.key_num)--;
	}
	//internal node
	else
	{
		i = 0;
		while (n->branch[i].key != key)
			i++;
		for (++i; i < n->head_info.key_num; ++i)
		{
			n->branch[i - 1].key = n->branch[i].key;
			n->branch[i - 1].pgnum = n->branch[i].pgnum;
		}
		(n->head_info.key_num)--;
	}
	file_write_page(table_id, n->this_num, n);
}

void adjust_root(int table_id, page_t* root)
{
	if (root->head_info.key_num > 0)
	{
		file_write_page(table_id, root->this_num, root);
		return;
	}
	//root page의 key개수가 0개이고, child를 가질때
	if (!(root->head_info.isleaf))
	{
		page_t* new_root = (page_t*)malloc(sizeof(page_t));
		file_read_page(table_id, root->left_page, new_root);
		header->root_num = root->left_page;
		new_root->parent = 0;
		file_free_page(table_id, root->this_num);
		file_write_page(table_id, header->root_num, new_root);
		file_write_page(table_id, 0, header);
	}
	//root page의 key 개수가 1개 이상이고,leaf 일때
	else
	{
		file_free_page(table_id, root->this_num);
		header->root_num = 0;
		file_write_page(table_id, 0, header);
	}
}

//neighbor node와 합쳐도 overflow가 일어나지 않는 경우 합쳐준다.
void coalesce_nodes(int table_id, page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int64_t k_prime)
{
	int i, j, neighbor_insertion_index, n_end;
	page_t* tmp;

	if (neighbor_index == -2) {
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	neighbor_insertion_index = neighbor->head_info.key_num;

	//internal page
	if (!(n->head_info.isleaf))
	{
		neighbor->branch[neighbor_insertion_index].key = k_prime;
		neighbor->branch[neighbor_insertion_index].pgnum = n->left_page;
		(neighbor->head_info.key_num)++;

		n_end = n->head_info.key_num;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->branch[i].key = n->branch[j].key;
			neighbor->branch[i].pgnum = n->branch[j].pgnum;
			(neighbor->head_info.key_num)++;
			(n->head_info.key_num)--;
		}

		for (i = 0; i < neighbor->head_info.key_num; i++) {
			page_t tmp2;
			file_read_page(table_id, neighbor->branch[i].pgnum, &tmp2);
			tmp2.parent = neighbor->this_num;
			file_write_page(table_id, neighbor->branch[i].pgnum, &tmp2);
		}
	}
	//leaf page
	else
	{
		for (i = neighbor_insertion_index, j = 0; j < n->head_info.key_num; i++, j++) {
			neighbor->record[i].key = n->record[j].key;
			strcpy(neighbor->record[i].value, n->record[j].value);
			(neighbor->head_info.key_num)++;
		}
		neighbor->right_sibling = n->right_sibling;
	}
	file_write_page(table_id, neighbor->this_num, neighbor);
	file_write_page(table_id, n->this_num, n);
	file_write_page(table_id, parent->this_num, parent);
	file_free_page(table_id, n->this_num);
	delete_entry(table_id, parent, k_prime);
}

//neighbor node의 key 값을 하나 가져온다.
void redistribute_nodes(int table_id, page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int k_prime_index)
{
	int i;
	page_t* tmp = (page_t*)malloc(sizeof(page_t));
	pagenum_t k_prime = parent->branch[k_prime_index].key;

	if (neighbor_index != -2) {

		if (!(n->head_info.isleaf))
		{
			for (i = n->head_info.key_num; i > 0; i--) {
				n->branch[i].key = n->branch[i - 1].key;
				n->branch[i].pgnum = n->branch[i - 1].pgnum; 
			}
			n->branch[0].pgnum = n->left_page;
			n->left_page = neighbor->branch[neighbor->head_info.key_num - 1].pgnum;
			file_read_page(table_id, n->left_page, tmp);
			tmp->parent = n->this_num;
			file_write_page(table_id, n->left_page, tmp);
			n->branch[0].key = k_prime;
			parent->branch[k_prime_index].key = neighbor->branch[neighbor->head_info.key_num - 1].key;
		}

		else
		{
			for (i = n->head_info.key_num; i > 0; i--) {
				n->record[i].key = n->record[i - 1].key;
				strcpy(n->record[i].value, n->record[i - 1].value);
			}
			n->record[0].key = neighbor->record[neighbor->head_info.key_num - 1].key;
			strcpy(n->record[0].value, neighbor->record[neighbor->head_info.key_num - 1].value);
			parent->branch[k_prime_index].key = n->record[0].key;
		}
	}
	
	else
	{
		if (n->head_info.isleaf)
		{
			n->record[n->head_info.key_num].key = neighbor->record[0].key;
			strcpy(n->record[n->head_info.key_num].value, neighbor->record[0].value);
			parent->branch[k_prime_index].key = neighbor->record[1].key;
			for (i = 0; i < neighbor->head_info.key_num - 1; i++) {
				neighbor->record[i].key = neighbor->record[i + 1].key;
				strcpy(neighbor->record[i].value, neighbor->record[i + 1].value);
			}
		}

		else
		{
			n->branch[n->head_info.key_num].key = k_prime;
			n->branch[n->head_info.key_num].pgnum = neighbor->left_page;
			file_read_page(table_id, n->branch[n->head_info.key_num].pgnum, tmp);
			tmp->parent = n->this_num;
			file_write_page(table_id, n->branch[n->head_info.key_num].pgnum, tmp);
			parent->branch[k_prime_index].key = neighbor->branch[0].key;
			neighbor->left_page = neighbor->branch[0].pgnum;
			for (i = 0; i < neighbor->head_info.key_num - 1; i++) {
				neighbor->branch[i].key = neighbor->branch[i + 1].key;
				neighbor->branch[i].pgnum = neighbor->branch[i + 1].pgnum;
			}
		}
	}
	(n->head_info.key_num)++;
	(neighbor->head_info.key_num)--;
	
	file_write_page(table_id, n->this_num, n);
	file_write_page(table_id, neighbor->this_num, neighbor);
	file_write_page(table_id, parent->this_num, parent);
	//
	free(tmp);
}

//n에서 key를 delete해준다
void delete_entry(int table_id, page_t* n, int64_t key)
{
	remove_entry_from_node(table_id, n, key);
	if (header->root_num == n->this_num)
		return adjust_root(table_id, n);

	if (n->head_info.key_num > 0)
	{
		file_write_page(table_id, n->this_num, n);
		return;
	}

	page_t parent;
	file_read_page(table_id, n->parent, &parent);

	int neighbor_index;
	int pidx = get_buffer_index(table_id, parent.this_num);
	if (pidx != -1)
	{
		(buf[pidx]->is_pinned)++;
		neighbor_index = get_neighbor_index(table_id, &parent, n->this_num);
		(buf[pidx]->is_pinned)--;
	}
	else
		neighbor_index = get_neighbor_index(table_id, &parent, n->this_num);
	int k_prime_index;
	if (neighbor_index == -2 || neighbor_index == -1)
		k_prime_index = 0;
	else
		k_prime_index = neighbor_index + 1;
	int64_t k_prime = parent.branch[k_prime_index].key;
	pagenum_t neighbor_page;
	if (neighbor_index == -2)
		neighbor_page = parent.branch[0].pgnum;
	else if (neighbor_index == -1)
		neighbor_page = parent.left_page;
	else
		neighbor_page = parent.branch[neighbor_index].pgnum;

	page_t neighbor;
	file_read_page(table_id, neighbor_page, &neighbor);

	//neighbor의 key가 1개뿐일 때 가져오지 못하므로, 이 경우에만 merge해준다

	if (neighbor.head_info.key_num == 1)
	{
		int pidx = get_buffer_index(table_id, parent.this_num);
		int neighboridx = get_buffer_index(table_id, neighbor.this_num);
		if (pidx != -1 && neighboridx != -1)
		{
			(buf[pidx]->is_pinned)++;
			(buf[neighboridx]->is_pinned)++;
			coalesce_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime);
			(buf[pidx]->is_pinned)--;
			(buf[neighboridx]->is_pinned)--;
		}
		else if (pidx != -1)
		{
			(buf[pidx]->is_pinned)++;
			coalesce_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime);
			(buf[pidx]->is_pinned)--;
		}
		else if (neighboridx != -1)
		{
			(buf[neighboridx]->is_pinned)++;
			coalesce_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime);
			(buf[neighboridx]->is_pinned)--;
		}
		else
			coalesce_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime);
	}


	//neighbor의 key가 1개보다 많을 경우에는 neighbor에서 하나 가져온다.

	else
	{
		int pidx = get_buffer_index(table_id, parent.this_num);
		int neighboridx = get_buffer_index(table_id, neighbor.this_num);
		if (pidx != -1 && neighboridx != -1)
		{
			(buf[pidx]->is_pinned)++;
			(buf[neighboridx]->is_pinned)++;
			redistribute_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime_index);
			(buf[pidx]->is_pinned)--;
			(buf[neighboridx]->is_pinned)--;
		}
		else if (pidx != -1)
		{
			(buf[pidx]->is_pinned)++;
			redistribute_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime_index);
			(buf[pidx]->is_pinned)--;
		}
		else if (neighboridx != -1)
		{
			(buf[neighboridx]->is_pinned)++;
			redistribute_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime_index);
			(buf[neighboridx]->is_pinned)--;
		}
		else
			redistribute_nodes(table_id, n, &parent, &neighbor, neighbor_index, k_prime_index);
	}
}


/* Master deletion function.
 */
int delete(int table_id, int64_t key)
{
	file_read_page(table_id, 0, header);
	char* value;
	value = find_in_function(table_id, key);

	if (value == NULL)
	{
		printf("key doesn't exist.\n");
		return 1;
	}

	page_t leaf;
	file_read_page(table_id, find_leaf(table_id, key), &leaf);
	int lidx = get_buffer_index(table_id, leaf.this_num);
	if (lidx != -1)
	{
		(buf[lidx]->is_pinned)++;
		delete_entry(table_id, &leaf, key);
		(buf[lidx]->is_pinned)--;
	}
	else
		delete_entry(table_id, &leaf, key);
	free(value);
	file_write_page(table_id, 0, header);
	return 0;
}

