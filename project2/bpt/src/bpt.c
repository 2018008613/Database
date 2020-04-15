#include "bpt.h"
#include <string.h>

FILE* fp = NULL;
page_t* header;

//페이지를 할당해준다. free page가 존재하면 free page를 할당해주고
//free page가 존재하지 않으면 새로운 페이지를 만들어 할당해준 뒤 그 page의 번호를 return해준다
pagenum_t file_alloc_page()
{
	file_read_page(0, header);
	pagenum_t num;
	//free page가 존재하지 않는 경우
	if (header->free_num == 0)
	{
		freepg_t tmp;
		tmp.next_freepg = 0;
		num = header->page_num;
		(header->page_num)++;
		fseek(fp, num*page_size, SEEK_SET);
		fwrite(&tmp, page_size, 1, fp);
		fflush(fp);
		file_write_page(0, header);
		return num;
	}
	//free page가 존재하는 경우
	num = header->free_num;
	freepg_t tmp;
	fseek(fp, num*page_size, SEEK_SET);
	fread(&tmp, page_size, 1, fp);
	header->free_num = tmp.next_freepg;
	file_write_page(0, header);
	return num;
}

//입력받은 page를 free page로 만들어준다.
void file_free_page(pagenum_t pagenum)
{
	file_read_page(0, header);
	freepg_t tmp;
	tmp.next_freepg = header->free_num;
	header->free_num = pagenum;
	fseek(fp, pagenum*page_size, SEEK_SET);
	fwrite(&tmp, page_size, 1, fp);
	fflush(fp);
	file_read_page(0, header);
}

//page를 읽어 dest에 정보를 저장해준다
void file_read_page(pagenum_t pagenum, page_t* dest)
{
	fseek(fp, pagenum*page_size, SEEK_SET);
	//header page인 경우
	if (pagenum == 0)
	{
		header_t tmp;
		fread(&tmp, page_size, 1, fp);
		dest->this_num = 0;
		dest->free_num = tmp.free_num;
		dest->root_num = tmp.root_num;
		dest->page_num = tmp.page_num;
	}
	else
	{
		nonfreepg_t tmp;
		fread(&tmp, page_size, 1, fp);
		dest->this_num = pagenum;
		dest->parent = tmp.parent;
		dest->isleaf = tmp.isleaf;
		dest->key_num = tmp.key_num;
		//leaf page인 경우
		if (tmp.isleaf)
		{
			dest->right_sibling = tmp.right_sibling;
			for (int i = 0;i < tmp.key_num;i++)
			{
				dest->record[i].key = tmp.record[i].key;
				strcpy(dest->record[i].value, tmp.record[i].value);
			}
		}
		//internal page인 경우
		else
		{
			dest->left_page = tmp.left_page;
			for (int i = 0;i < tmp.key_num;i++)
			{
				dest->branch[i].key = tmp.branch[i].key;
				dest->branch[i].pgnum = tmp.branch[i].pgnum;
			}
		}
	}
}
//src page에 담긴 정보를 파일에 적어준다.
void file_write_page(pagenum_t pagenum, const page_t* src)
{
	fseek(fp, pagenum*page_size, SEEK_SET);
	//header인 경우
	if (pagenum == 0)
	{
		header_t tmp;
		tmp.free_num = src->free_num;
		tmp.page_num = src->page_num;
		tmp.root_num = src->root_num;
		fwrite(&tmp, page_size, 1, fp);
		fflush(fp);
	}
	else
	{
		//leaf인 경우
		if (src->isleaf)
		{
			leafpg_t tmp;
			tmp.parent = src->parent;
			tmp.isleaf = src->isleaf;
			tmp.key_num = src->key_num;
			tmp.right_sibling = src->right_sibling;
			for (int i = 0;i < src->key_num;i++)
			{
				tmp.record[i].key = src->record[i].key;
				strcpy(tmp.record[i].value, src->record[i].value);
			}
			fwrite(&tmp, page_size, 1, fp);
			fflush(fp);
		}
		//internal인 경우
		else
		{
			internalpg_t tmp;
			tmp.parent = src->parent;
			tmp.isleaf = src->isleaf;
			tmp.key_num = src->key_num;
			tmp.left_page = src->left_page;
			for (int i = 0;i < src->key_num;i++)
			{
				tmp.branch[i].key = src->branch[i].key;
				tmp.branch[i].pgnum = src->branch[i].pgnum;
			}
			fwrite(&tmp, page_size, 1, fp);
			fflush(fp);
		}
	}
}

//record는 31칸, branch는 248칸짜리 배열
int leaf_order = 32;
int internal_order = 249;

//print tree를 위한 함수들
int path_to_root(uint64_t pgnum)
{
	page_t* tmp = (page_t*)malloc(sizeof(page_t));
	file_read_page(0, header);
	uint64_t next_page = pgnum;
	int level = 0;
	while (next_page != header->root_num)
	{
		file_read_page(next_page, tmp);
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


void print_tree()
{
	file_read_page(0, header);

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
		file_read_page(pgnum, &page);

		if (page.parent != 0)
		{
			file_read_page(page.parent, &parentpg);
			if (path_to_root(pgnum) != level)
			{
				level = path_to_root(pgnum);
				printf("\n");
			}
		}

		if (!(page.isleaf))
		{
			for (int i = 0; i < page.key_num; ++i)
				printf("%ld  ", page.branch[i].key);
			enqueue(page.left_page, q);
			for (int i = 0; i < page.key_num; ++i)
				enqueue(page.branch[i].pgnum, q);
		}
		else
		{
			for (int i = 0; i < page.key_num; ++i)
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
	//pathname이 이미 존재할 때
	if ((fp = fopen(pathname, "r+b")) != NULL)
	{
		printf("%s already exist\n", pathname);
		header = (page_t*)malloc(sizeof(page_t));
		file_read_page(0, header);
		return 1;
	}
	//존재하지 않을 때
	fp = fopen(pathname, "w+b");
	printf("Creating %s\n", pathname);
	header = (page_t*)malloc(sizeof(page_t));
	header->free_num = 0;
	header->root_num = 0;
	header->page_num = 1;
	header->this_num = 0;
	file_write_page(0, header);
	return 1;
}

//key 값이 들어있을 수 있는 leaf의 page_num을 return해준다
pagenum_t find_leaf(int64_t key)
{
	//root가 없는 경우
	pagenum_t root_num = header->root_num;
	if (root_num == 0)
		return 0;
	//num에 root page를 할당, node에 root page의 정보를 할당
	pagenum_t num = root_num;
	page_t node;
	file_read_page(num, &node);
	//node가 internal page인 경우 반복
	while (!(node.isleaf))
	{
		int idx = -2;
		for (int i = 0;i < node.key_num;i++)
		{
			if (node.branch[i].key > key)
			{
				idx = i - 1;
				break;
			}
		}
		//첫 key값보다 작을 경우
		if (idx == -1)
			num = node.left_page;
		//마지막 key값보다 클 경우
		else if (idx == -2)
			num = node.branch[node.key_num - 1].pgnum;
		else
			num = node.branch[idx].pgnum;
		file_read_page(num, &node);
	}
	return num;
}

//key 값이 없으면 NULL을 return, 그렇지 않으면 key 값에 해당하는 value 값을 return
char* find(int64_t key)
{
	//num에 key가 들어있을 수 있는 leaf의 page를 할당
	pagenum_t num = find_leaf(key);
	page_t tmp;
	//root node가 없을 경우
	if (num == 0)
		return NULL;
	file_read_page(num, &tmp);
	//key값이 있는 idx를 찾는다
	int idx = -1;
	for (int i = 0;i < tmp.key_num;i++)
	{
		if (key == tmp.record[i].key)
		{
			idx = i;
			break;
		}
	}
	//key 값이 들어있지 않은 경우
	if (idx == -1)
		return NULL;
	char* value = (char*)malloc(sizeof(char) * 120);
	strcpy(value, tmp.record[idx].value);
	return value;
}
//key 값을 찾아서 key 값이 존재하면 그에 해당하는 value 값을 ret_val에 넣어준다
int db_find(int64_t key, char * ret_val)
{
	//num에 key가 들어있을 수 있는 leaf의 page를 할당
	pagenum_t num = find_leaf(key);
	page_t tmp;
	//root node가 없을 경우
	if (num == 0)
		return 1;
	file_read_page(num, &tmp);
	//key값이 있는 idx를 찾는다
	int idx = -1;
	for (int i = 0;i < tmp.key_num;i++)
	{
		if (key == tmp.record[i].key)
		{
			idx = i;
			break;
		}
	}
	//key 값이 들어있지 않은 경우
	if (idx == -1)
		return 1;

	strcpy(ret_val, tmp.record[idx].value);
	return 0;
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
	new_leaf->isleaf = 1;
	new_leaf->key_num = 0;
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
	new_internal->isleaf = 0;
	new_internal->key_num = 0;
	new_internal->left_page = 0;
	return new_internal;
}
//leaf에 record를 적당한 위치에 넣은 뒤 file에 write해준다.
void insert_into_leaf(page_t* leaf, int64_t key, record_t* record)
{
	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->key_num && leaf->record[insertion_point].key < key)
		insertion_point++;
	for (i = leaf->key_num; i > insertion_point; i--) 
	{
		leaf->record[i].key = leaf->record[i - 1].key;
		strcpy(leaf->record[i].value, leaf->record[i - 1].value);
	}
	leaf->record[insertion_point].key = key;
	strcpy(leaf->record[insertion_point].value, record->value);
	(leaf->key_num)++;
	file_write_page(leaf->this_num, leaf);
}


//leaf page에 insert할 때 overflow가 일어나는 경우
void insert_into_leaf_after_splitting(page_t* leaf, int64_t key, record_t* record)
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
	for (i = 0, j = 0; i < leaf->key_num; i++, j++) {
		if (j == insertion_index) j++;
		tmp[j].key = leaf->record[i].key;
		strcpy(tmp[j].value, leaf->record[i].value);
	}
	tmp[insertion_index].key = key;
	strcpy(tmp[insertion_index].value, record->value);

	leaf->key_num = 0;

	split = cut(leaf_order - 1);

	for (i = 0; i < split; i++) {
		leaf->record[i].key = tmp[i].key;
		strcpy(leaf->record[i].value, tmp[i].value);
		(leaf->key_num)++;
	}

	for (i = split, j = 0; i < leaf_order; i++, j++) {
		new_leaf->record[j].key = tmp[i].key;
		strcpy(new_leaf->record[j].value, tmp[i].value);
		(new_leaf->key_num)++;
	}

	free(tmp);

	new_leaf->this_num = file_alloc_page();
	new_leaf->right_sibling = leaf->right_sibling;
	leaf->right_sibling = new_leaf->this_num;

	new_leaf->parent = leaf->parent;
	file_write_page(leaf->this_num, leaf);
	file_write_page(new_leaf->this_num, new_leaf);
	
	new_key = new_leaf->record[0].key;

	insert_into_parent(leaf, new_leaf, new_key);
	free(new_leaf);
}

//overflow되지 않을 때 internal node에 insert.
void insert_into_node(page_t* parent, pagenum_t right_pgnum, int64_t key)
{
	int insert_position = 0;
	while (insert_position < parent->key_num && parent->branch[insert_position].key < key)
		insert_position++;
	for (int i = parent->key_num; i > insert_position; --i)
	{
		parent->branch[i].key = parent->branch[i - 1].key;
		parent->branch[i].pgnum = parent->branch[i - 1].pgnum;
	}
	parent->branch[insert_position].key = key;
	parent->branch[insert_position].pgnum = right_pgnum;
	(parent->key_num++);
	file_write_page(parent->this_num, parent);
}

//overflow될때 internal node에 insert 후 split해준다.
void insert_into_node_after_splitting(page_t* parent, pagenum_t right_pgnum, int64_t key)
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
	parent->key_num = 0;
	split = cut(internal_order);
	for (int i = 0; i < split - 1; ++i)
	{
		parent->branch[i].key = tmp[i].key;
		parent->branch[i].pgnum = tmp[i].pgnum;
		parent->key_num++;
	}
	for (i = split, j = 0; i < internal_order; ++i, ++j)
	{
		new_internal->branch[j].key = tmp[i].key;
		new_internal->branch[j].pgnum = tmp[i].pgnum;
		(new_internal->key_num)++;
	}
	new_internal->left_page = tmp[split - 1].pgnum;
	new_internal->parent = parent->parent;

	new_internal->this_num = file_alloc_page();
	file_write_page(parent->this_num, parent);
	file_write_page(new_internal->this_num, new_internal);

	page_t child;
	file_read_page(new_internal->left_page, &child);
	child.parent = new_internal->this_num;
	file_write_page(new_internal->left_page, &child);
	for (i = 0; i < new_internal->key_num; ++i)
	{
		file_read_page(new_internal->branch[i].pgnum, &child);
		child.parent = new_internal->this_num;
		file_write_page(new_internal->branch[i].pgnum, &child);
	}
	insert_into_parent(parent, new_internal, tmp[split - 1].key);
	free(tmp);
	free(new_internal);
}
//parent에 key 값을 insert한다.
void insert_into_parent(page_t* left, page_t* right, int64_t key)
{
	if (left->parent == 0)
		return insert_into_new_root(left, right, key);
	page_t parent;
	file_read_page(left->parent, &parent);

	if (parent.key_num < internal_order - 1)
		return insert_into_node(&parent, right->this_num, key);

	insert_into_node_after_splitting(&parent, right->this_num, key);
}

//root를 split해서 새로운 root를 만드는 경우
void insert_into_new_root(page_t* left, page_t* right, int64_t key)
{
	page_t* root = make_internal();
	root->left_page = left->this_num;
	root->branch[0].key = key;
	root->branch[0].pgnum = right->this_num;
	(root->key_num)++;

	root->this_num = file_alloc_page();
	left->parent = root->this_num;
	right->parent = root->this_num;
	header->root_num = root->this_num;
	file_write_page(left->this_num, left);
	file_write_page(right->this_num, right);
	file_write_page(root->this_num, root);
	file_write_page(0, header);
	free(root);
}

//새로운 root를 만들어 tree를 생성한다.
void start_new_tree(int64_t key, record_t* record)
{
	page_t* root = make_leaf();
	root->record[0].key = key;
	strcpy(root->record[0].value, record->value);
	(root->key_num)++;

	root->this_num = file_alloc_page();
	header->root_num = root->this_num;
	file_write_page(root->this_num, root);
	file_write_page(0, header);
	free(root);
}

// Master insertion function.

int db_insert(int64_t key, char* value)
{
	//key값이 이미 존재하는 경우
	if (find(key) != NULL)
		return 1;

	record_t* new_record = make_record(key, value);

	//root가 존재하지 않는 경우
	if (header->root_num == 0)
	{
		start_new_tree(key, new_record);
		return 0;
	}

	pagenum_t leaf_page = find_leaf(key);
	page_t leaf;
	file_read_page(leaf_page, &leaf);

	//overflow가 일어나지 않는 경우
	if (leaf.key_num < leaf_order - 1)
	{
		insert_into_leaf(&leaf, key, new_record);
		free(new_record);
		return 0;
	}

	//overflow가 일어나는 경우
	insert_into_leaf_after_splitting(&leaf, key, new_record);
	free(new_record);
	return 0;
}

// DELETION.

//pgnum의 왼쪽 노드의 index를 반환해준다. 가장 왼쪽이면 -1을 반환
int get_neighbor_index(page_t* parent, pagenum_t pgnum)
{
	int i;
	if (parent->left_page == pgnum)
		return -2;
	for (i = 0; i < parent->key_num; i++)
		if (parent->branch[i].pgnum == pgnum)
			return i - 1;

	// Error state.
	printf("Search for nonexistent page to node in parent.\n");
	printf("Page:  %ld\n", pgnum);
	exit(EXIT_FAILURE);
}

//n에서 key를 제거한다.
void remove_entry_from_node(page_t* n, int64_t key)
{
	int i, num_pointers;
	//leaf node
	if (n->isleaf)
	{
		i = 0;
		while (n->record[i].key != key)
			i++;
		for (++i; i < n->key_num; i++)
		{
			n->record[i - 1].key = n->record[i].key;
			strcpy(n->record[i - 1].value, n->record[i].value);
		}
		(n->key_num)--;
	}
	//internal node
	else
	{
		i = 0;
		while (n->branch[i].key != key)
			i++;
		for (++i; i < n->key_num; i++)
		{
			n->branch[i - 1].key = n->branch[i].key;
			n->branch[i - 1].pgnum = n->branch[i].pgnum;
		}
		(n->key_num)--;
	}
	file_write_page(n->this_num, n);
}

void adjust_root(page_t* root)
{
	if (root->key_num > 0)
	{
		file_write_page(root->this_num, root);
		return;
	}
	//root page의 key개수가 0개이고, child를 가질때
	if (!(root->isleaf))
	{
		page_t new_root;
		file_read_page(root->left_page, &new_root);
		header->root_num = root->left_page;
		new_root.parent = 0;
		file_free_page(root->this_num);
		file_write_page(header->root_num, &new_root);
		file_write_page(0, header);
	}
	//root page의 key 개수가 1개 이상이고,leaf 일때
	else
	{
		file_free_page(root->this_num);
		header->root_num = 0;
		file_write_page(0, header);
	}
}

//neighbor node와 합쳐도 overflow가 일어나지 않는 경우 합쳐준다.
void coalesce_nodes(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int64_t k_prime)
{
	int i, j, neighbor_insertion_index, n_end;
	page_t* tmp;

	if (neighbor_index == -2) {
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	neighbor_insertion_index = neighbor->key_num;

	//internal page
	if (!(n->isleaf))
	{
		neighbor->branch[neighbor_insertion_index].key = k_prime;
		neighbor->branch[neighbor_insertion_index].pgnum = n->left_page;
		(neighbor->key_num)++;

		n_end = n->key_num;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->branch[i].key = n->branch[j].key;
			neighbor->branch[i].pgnum = n->branch[j].pgnum;
			neighbor->key_num++;
			n->key_num--;
		}

		for (i = 0; i < neighbor->key_num; i++) {
			file_read_page(neighbor->branch[i].pgnum, tmp);
			tmp->parent = neighbor->this_num;
			file_write_page(neighbor->branch[i].pgnum, tmp);
		}
	}
	//leaf page
	else
	{
		for (i = neighbor_insertion_index, j = 0; j < n->key_num; i++, j++) {
			neighbor->record[i].key = n->record[j].key;
			strcpy(neighbor->record[i].value, n->record[j].value);
			(neighbor->key_num)++;
		}
		neighbor->right_sibling = n->right_sibling;
	}

	file_write_page(neighbor->this_num, neighbor);
	file_free_page(n->this_num);
	delete_entry(parent, k_prime);
}

//neighbor node의 key 값을 하나 가져온다.
void redistribute_nodes(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int k_prime_index)
{
	int i;
	page_t* tmp;
	pagenum_t k_prime = parent->branch[k_prime_index].key;

	if (neighbor_index != -2) {

		if (!(n->isleaf))
		{
			for (i = n->key_num; i > 0; i--) {
				n->branch[i].key = n->branch[i - 1].key;
				n->branch[i].pgnum = n->branch[i - 1].pgnum; 
			}
			n->branch[0].pgnum = n->left_page;
			n->left_page = neighbor->branch[neighbor->key_num - 1].pgnum;
			file_read_page(n->left_page, tmp);
			tmp->parent = n->this_num;
			file_write_page(n->left_page, tmp);
			n->branch[0].key = k_prime;
			parent->branch[k_prime_index].key = neighbor->branch[neighbor->key_num - 1].key;
		}

		else
		{
			for (i = n->key_num; i > 0; i--) {
				n->record[i].key = n->record[i - 1].key;
				strcpy(n->record[i].value, n->record[i - 1].value);
			}
			n->record[0].key = neighbor->record[neighbor->key_num - 1].key;
			strcpy(n->record[0].value, neighbor->record[neighbor->key_num - 1].value);
			parent->branch[k_prime_index].key = n->record[0].key;
		}
	}
	
	else
	{
		if (n->isleaf)
		{
			n->record[n->key_num].key = neighbor->record[0].key;
			strcpy(n->record[n->key_num].value, neighbor->record[0].value);
			parent->branch[k_prime_index].key = neighbor->record[1].key;
			for (i = 0; i < neighbor->key_num - 1; i++) {
				neighbor->record[i].key = neighbor->record[i + 1].key;
				strcpy(neighbor->record[i].value, neighbor->record[i + 1].value);
			}
		}

		else
		{
			n->branch[n->key_num].key = k_prime;
			n->branch[n->key_num].pgnum = neighbor->left_page;
			file_read_page(n->branch[n->key_num].pgnum, tmp);
			tmp->parent = n->this_num;
			file_write_page(n->branch[n->key_num].pgnum, tmp);
			parent->branch[k_prime_index].key = neighbor->branch[0].key;
			neighbor->left_page = neighbor->branch[0].pgnum;
			for (i = 0; i < neighbor->key_num - 1; i++) {
				neighbor->branch[i].key = neighbor->branch[i + 1].key;
				neighbor->branch[i].pgnum = neighbor->branch[i + 1].pgnum;
			}
		}
	}
	(n->key_num)++;
	(neighbor->key_num)--;
	
	file_write_page(n->this_num, n);
	file_write_page(neighbor->this_num, neighbor);
	file_write_page(parent->this_num, parent);
}

//n에서 key를 delete해준다
void delete_entry(page_t* n, int64_t key)
{
	remove_entry_from_node(n, key);

	if (header->root_num == n->this_num)
		return adjust_root(n);

	if (n->key_num > 0)
	{
		file_write_page(n->this_num, n);
		return;
	}

	page_t parent;
	file_read_page(n->parent, &parent);

	int neighbor_index = get_neighbor_index(&parent, n->this_num);
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
	file_read_page(neighbor_page, &neighbor);

	//neighbor의 key가 1개뿐일 때 가져오지 못하므로, 이 경우에만 merge해준다

	if (neighbor.key_num == 1)
		return coalesce_nodes(n, &parent, &neighbor, neighbor_index, k_prime);

	//neighbor의 key가 1개보다 많을 경우에는 neighbor에서 하나 가져온다.

	else
		return redistribute_nodes(n, &parent, &neighbor, neighbor_index, k_prime_index);
}


/* Master deletion function.
 */
int db_delete(int64_t key)
{
	char* value;
	value = find(key);

	if (value == NULL)
	{
		printf("key doesn't exist.\n");
		return 1;
	}

	page_t leaf;
	file_read_page(find_leaf(key), &leaf);
	delete_entry(&leaf, key);

	free(value);
	return 0;
}

