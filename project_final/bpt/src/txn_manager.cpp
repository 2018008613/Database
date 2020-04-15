#include "txn_manager.h"
#include "bpt.h"
#include "buffer_manager.h"
#include "log_manager.h"
#include <stdlib.h>
#include <string.h>
#include <queue>

pthread_mutex_t trx_sys_mutex = PTHREAD_MUTEX_INITIALIZER, table_mutex = PTHREAD_MUTEX_INITIALIZER, buff_mutex = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t log_mutex;
int next_trx = 1;
queue<int> free_trx;
trx_t* trx = NULL;
unordered_map<int, LSH> table;
extern log_record* log_header;
extern FILE* fpL;
extern log_buffer** log_buf;

int trx_size = 0;

extern buffer** buf;

void extend_trx()
{
	if (trx == NULL)
	{
		trx = (trx_t*)malloc(sizeof(trx_t) * 50001);
		trx_size = 50000;
		return;
	}
	trx = (trx_t*)realloc(trx, sizeof(trx_t)*(trx_size * 2 + 1));
	trx_size *= 2;
}

int begin_trx()
{
	pthread_mutex_lock(&trx_sys_mutex);
	int tid;
	if (!free_trx.empty())
	{
		tid = free_trx.front();

		pthread_mutex_lock(&(log_mutex));
		log_record tmp_log;
		tmp_log.LSN = log_header->next_LSN;
		log_header->next_LSN += 100;
		tmp_log.trx_id = tid;
		tmp_log.type = 0;
		file_write_log(0, log_header);
		file_write_log(tmp_log.LSN, &tmp_log);
		pthread_mutex_unlock(&(log_mutex));

		free_trx.pop();
		trx[tid].state = 0;
		trx[tid].trx_id = tid;
		trx[tid].wait_lock = NULL;
		trx[tid].trx_locks.clear();
		trx[tid].undo_list.clear();
		pthread_mutex_unlock(&trx_sys_mutex);
		return tid;
	}
	if (next_trx > trx_size)
		extend_trx();
	tid = next_trx++;

	pthread_mutex_lock(&(log_mutex));
	log_record tmp_log;
	tmp_log.LSN = log_header->next_LSN;
	log_header->next_LSN += 100;
	tmp_log.trx_id = tid;
	tmp_log.type = 0;
	file_write_log(0, log_header);
	file_write_log(tmp_log.LSN, &tmp_log);
	pthread_mutex_unlock(&(log_mutex));

	trx[tid].state = 0;
	trx[tid].trx_id = tid;
	trx[tid].wait_lock = NULL;
	trx[tid].trx_locks.clear();
	trx[tid].undo_list.clear();
	pthread_mutex_unlock(&trx_sys_mutex);
	return tid;
}

int end_trx(int tid)
{
	pthread_mutex_lock(&(log_mutex));
	log_record tmp_log;
	tmp_log.LSN = log_header->next_LSN;
	log_header->next_LSN += 100;
	tmp_log.trx_id = tid;
	tmp_log.type = 2;
	file_write_log(0, log_header);
	file_write_log(tmp_log.LSN, &tmp_log);
	flush_log();
	pthread_mutex_unlock(&(log_mutex));

	//table lock을 잡아준다
	pthread_mutex_lock(&table_mutex);
	trx_t* t = &trx[tid];

	//해당 trx에 걸려 있는 모든 lock을 체크해줌
	for (int i = 0;i < t->trx_locks.size();i++)
	{
		lock_t* lock_tmp = t->trx_locks[i];
		int table_id = lock_tmp->table_id;
		int key = lock_tmp->key;
		LSH tmp_LSH = table[key];
		//X mode의 lock인 경우
		if (lock_tmp->mode == 2)
		{
			//뒤에 wait되어있는 lock이 있는 경우
			if (lock_tmp->aflock != NULL)
			{
				lock_tmp->aflock->bflock = NULL;
				tmp_LSH.head = lock_tmp->aflock;
				tmp_LSH.mode = lock_tmp->aflock->mode;
				//다음에 걸어줄 lock이 S mode인 경우
				if (tmp_LSH.mode == 1)
				{
					lock_t* cond_tmp = lock_tmp->aflock;
					while (cond_tmp != NULL && cond_tmp->mode == 1)
					{
						pthread_cond_signal(&(cond_tmp->trx->trx_cond));
						cond_tmp = cond_tmp->aflock;
					}
				}
				//다음에 걸어줄 lock이 X mode인 경우
				else
					pthread_cond_signal(&(lock_tmp->aflock->trx->trx_cond));
			}
			//다음에 걸어줄 lock이 존재하지 않는 경우
			else
			{
				tmp_LSH.head = NULL;
				tmp_LSH.tail = NULL;
				tmp_LSH.mode = 0;
			}
		}
		//S mode의 lock인 경우
		else
		{
			//앞에 다른 S mode의 lock이 있는 경우
			if (lock_tmp->bflock != NULL)
				lock_tmp->bflock->aflock = lock_tmp->aflock;
			//다른 lock이 걸려있지 않은 경우
			else if (lock_tmp->aflock == NULL)
			{
				tmp_LSH.head = NULL;
				tmp_LSH.tail = NULL;
				tmp_LSH.mode = 0;
			}
			//뒤에 다른 S mode의 lock이 있는 경우
			else if (lock_tmp->aflock->mode == 1)
			{
				lock_tmp->aflock->bflock = NULL;
				tmp_LSH.head = lock_tmp->aflock;
				tmp_LSH.mode = 1;
			}
			//뒤에 다른 X mode의 lock이 있는 경우
			else
			{
				lock_tmp->aflock->bflock = NULL;
				tmp_LSH.head = lock_tmp->aflock;
				tmp_LSH.mode = lock_tmp->aflock->mode;
				pthread_cond_signal(&(lock_tmp->aflock->trx->trx_cond));
			}
		}
		table[key] = tmp_LSH;
		free(lock_tmp);
	}
	t->trx_locks.clear();
	t->undo_list.clear();
	pthread_mutex_unlock(&table_mutex);

	pthread_mutex_lock(&trx_sys_mutex);
	free_trx.push(tid);
	pthread_mutex_unlock(&trx_sys_mutex);
	return tid;
}

void dfs(int* chk, int* visit, int trx_id, int now)
{
	//이미 체크한 trx이거나, deadlock을 이미 찾았다면, return해줌
	if (visit[now] == 1 || *(chk) == 1)
		return;
	//deadlock을 찾았을 경우, chk_deadlock을 1로 만들어준 뒤 return해줌
	if (now == trx_id)
	{
		*(chk) = 1;
		return;
	}
	visit[now] = 1;
	//해당 trx에 wait_lock이 존재하는 경우
	if (trx[now].wait_lock != NULL)
	{
		int chk_key = trx[now].wait_lock->key;
		LSH chk_LSH = table[chk_key];
		if (chk_LSH.mode == 1)
		{
			lock_t* chk_lock = chk_LSH.head;
			while (chk_lock != NULL && chk_lock->mode == 1)
			{
				dfs(chk, visit, trx_id, chk_lock->trx->trx_id);
				chk_lock = chk_lock->aflock;
			}
		}
		else
			dfs(chk, visit, trx_id, chk_LSH.head->trx->trx_id);
	}
}

int find_deadlock(int chk, int64_t key, int trx_id)
{
	LSH tmp_LSH = table[key];
	int chk_deadlock = 0;
	int* visit = (int*)malloc(sizeof(int)*(trx_size + 1));
	for (int i = 0;i <= trx_size;i++)
		visit[i] = 0;
	lock_t* tmp_lock = tmp_LSH.head;
	
	if (tmp_lock->mode == 1)
	{
		while (tmp_lock != NULL && tmp_lock->mode == 1)
		{
			int tid = tmp_lock->trx->trx_id;
			dfs(&chk_deadlock, visit, trx_id, tid);
			tmp_lock = tmp_lock->aflock;
		}
	}
	else
	{
		int tid = tmp_lock->trx->trx_id;
		dfs(&chk_deadlock, visit, trx_id, tid);
	}
	free(visit);
	return chk_deadlock;
}

//success : 0, wait : 1, dead lock : 2
int acquire_record_lock(int table_id, int chk, int64_t key, int trx_id)
{
	pthread_mutex_lock(&table_mutex);
	trx_t* t = &trx[trx_id];
	//chk 0 : find, 1 : update
	//find
	if (chk == 0)
	{
		//table에 key가 존재할 때
		if (table.count(key))
		{
			LSH tmp_LSH = table[key];
			lock_t* new_lock = (lock_t*)malloc(sizeof(lock_t));
			new_lock->table_id = table_id;
			new_lock->key = key;
			new_lock->mode = 1;
			new_lock->trx = t;
			new_lock->aflock = NULL;
			new_lock->bflock = NULL;
			//해당 record에 걸려 있는 lock이 없을 때
			if (tmp_LSH.mode == 0)
			{
				tmp_LSH.head = new_lock;
				tmp_LSH.tail = new_lock;
				tmp_LSH.mode = 1;
				table[key] = tmp_LSH;
				t->trx_locks.push_back(new_lock);
				pthread_mutex_unlock(&table_mutex);
				return 0;
			}
			//해당 record에 X mode가 lock으로 걸려있을 때
			else if (tmp_LSH.mode == 2)
			{
				//동일 transaction의 X mode lock이 걸려있을 때
				if (tmp_LSH.head->trx->trx_id == trx_id)
				{
					pthread_mutex_unlock(&table_mutex);
					return 0;
				}
				//deadlock이 발견되지 않았을 때
				else if (!find_deadlock(chk, key, trx_id))
				{
					tmp_LSH.tail->aflock = new_lock;
					new_lock->bflock = tmp_LSH.tail;
					tmp_LSH.tail = new_lock;
					table[key] = tmp_LSH;
					trx[trx_id].wait_lock = new_lock;
					pthread_mutex_unlock(&table_mutex);
					return 1;
				}
				//deadlock이 발견되었을 때
				else
				{
					free(new_lock);
					pthread_mutex_unlock(&table_mutex);
					return 2;
				}
			}
			//해당 record에 S mode lock이 걸려있을 때
			else
			{
				lock_t* tmp = tmp_LSH.head;
				int chk_tmp = 0;
				while (tmp != NULL && tmp->mode == 1)
				{
					if (tmp->trx->trx_id == trx_id)
						chk_tmp = 1;
					tmp = tmp->aflock;
				}
				//해당 record에 이미 같은 transaction의 s mode lock이 걸려있을 때
				if (chk_tmp)
				{
					pthread_mutex_unlock(&table_mutex);
					return 0;
				}
				tmp = tmp_LSH.head;
				while (tmp != NULL && tmp->mode == 1)
					tmp = tmp->aflock;
				//wait하지 않고 실행시켜도 될 때, 즉 S mode의 lock들만이 걸려있을 때
				if (tmp == NULL)
				{
					tmp_LSH.tail->aflock = new_lock;
					new_lock->bflock = tmp_LSH.tail;
					tmp_LSH.tail = new_lock;
					table[key] = tmp_LSH;
					t->trx_locks.push_back(new_lock);
					pthread_mutex_unlock(&table_mutex);
					return 0;
				}
				//deadlock이 발견되지 않아 wait시켜도 될 때
				if (!find_deadlock(chk, key, trx_id))
				{
					tmp_LSH.tail->aflock = new_lock;
					new_lock->bflock = tmp_LSH.tail;
					tmp_LSH.tail = new_lock;
					table[key] = tmp_LSH;
					trx[trx_id].wait_lock = new_lock;
					pthread_mutex_unlock(&table_mutex);
					return 1;
				}
				//deadlock이 발견되었을 때
				free(new_lock);
				pthread_mutex_unlock(&table_mutex);
				return 2;
			}
		}
		//record가 hash table에 들어있지 않을 때
		else
		{
			LSH new_LSH;
			lock_t* new_lock = (lock_t*)malloc(sizeof(lock_t));
			new_lock->table_id = table_id;
			new_lock->key = key;
			new_lock->mode = 1;
			new_lock->trx = t;
			new_lock->aflock = NULL;
			new_lock->bflock = NULL;
			new_LSH.head = new_lock;
			new_LSH.tail = new_lock;
			new_LSH.mode = 1;
			table[key] = new_LSH;
			t->trx_locks.push_back(new_lock);
			pthread_mutex_unlock(&table_mutex);
			return 0;
		}
	}
	//update일 때
	else
	{
		//hash table에 record 값이 이미 존재할 때
		if (table.count(key))
		{
			LSH tmp_LSH = table[key];
			lock_t* new_lock = (lock_t*)malloc(sizeof(lock_t));
			new_lock->table_id = table_id;
			new_lock->key = key;
			new_lock->mode = 2;
			new_lock->trx = t;
			new_lock->aflock = NULL;
			new_lock->bflock = NULL;
			//해당 record에 lock이 걸려있지 않을 때
			if (tmp_LSH.mode == 0)
			{
				tmp_LSH.head = new_lock;
				tmp_LSH.tail = new_lock;
				tmp_LSH.mode = 2;
				table[key] = tmp_LSH;
				t->trx_locks.push_back(new_lock);
				pthread_mutex_unlock(&table_mutex);
				return 0;
			}
			//해당 record에 X mode lock이 걸려있을 때
			else if (tmp_LSH.mode == 2)
			{
				//걸려 있는 lock이 해당 transaction일 때 그냥 실행시켜준다.
				if (tmp_LSH.head->trx->trx_id == trx_id)
				{
					pthread_mutex_unlock(&table_mutex);
					return 0;
				}
				//deadlock이 발견되지 않을 때 wait시켜준다.
				else if (!find_deadlock(chk, key, trx_id))
				{
					tmp_LSH.tail->aflock = new_lock;
					new_lock->bflock = tmp_LSH.tail;
					tmp_LSH.tail = new_lock;
					table[key] = tmp_LSH;
					trx[trx_id].wait_lock = new_lock;
					pthread_mutex_unlock(&table_mutex);
					return 1;
				}
				//deadlock이 발견되었을 때
				else
				{
					free(new_lock);
					pthread_mutex_unlock(&table_mutex);
					return 2;
				}
			}
			//S mode lock이 걸려있을 때
			else
			{
				lock_t* tmp = tmp_LSH.head;
				int chk_tmp = 0;
				while (tmp != NULL && tmp->mode == 1)
				{
					if (tmp->trx->trx_id == trx_id)
						chk_tmp = 1;
					tmp = tmp->aflock;
				}
				//걸려 있는 S mode lock중 해당 transaction의 lock이 있을 때 deadlock이 발생함.
				if (chk_tmp)
				{
					free(new_lock);
					pthread_mutex_unlock(&table_mutex);
					return 2;
				}
				//deadlock이 발생했을 때
				if (find_deadlock(chk, key, trx_id))
				{
					pthread_mutex_unlock(&table_mutex);
					return 2;
				}
				//deadlock이 발생하지 않을 때 wait시켜준다
				tmp_LSH.tail->aflock = new_lock;
				new_lock->bflock = tmp_LSH.tail;
				tmp_LSH.tail = new_lock;
				table[key] = tmp_LSH;
				trx[trx_id].wait_lock = new_lock;
				pthread_mutex_unlock(&table_mutex);
				return 1;
			}
		}
		//record에 해당 key가 존재하지 않는 경우
		else
		{
			LSH new_LSH;
			lock_t* new_lock = (lock_t*)malloc(sizeof(lock_t));
			new_lock->table_id = table_id;
			new_lock->key = key;
			new_lock->mode = 2;
			new_lock->trx = t;
			new_lock->aflock = NULL;
			new_lock->bflock = NULL;
			new_LSH.head = new_lock;
			new_LSH.tail = new_lock;
			new_LSH.mode = 2;
			table[key] = new_LSH;
			t->trx_locks.push_back(new_lock);
			pthread_mutex_unlock(&table_mutex);
			return 0;
		}
	}
}


void abort_trx(int tid)
{
	pthread_mutex_lock(&(log_mutex));
	log_record tmp_log;
	tmp_log.LSN = log_header->next_LSN;
	log_header->next_LSN += 100;
	tmp_log.trx_id = tid;
	tmp_log.type = 3;
	file_write_log(0, log_header);
	file_write_log(tmp_log.LSN, &tmp_log);
	pthread_mutex_unlock(&(log_mutex));

	trx_t* t = &trx[tid];
	for (int i = 0;i < t->undo_list.size();i++)
	{
		pthread_mutex_lock(&buff_mutex);
		undo tmp = t->undo_list[i];
		int pgnum = find_leaf(tmp.table_id, tmp.key);
		page_t leaf;
		file_read_page(tmp.table_id, pgnum, &leaf);
		int idx = get_buffer_index(tmp.table_id, pgnum);
		buffer* tmp_buf = buf[idx];
		(tmp_buf->is_pinned)++;
		pthread_mutex_lock(&(tmp_buf->page_mutex));
		pthread_mutex_unlock(&buff_mutex);
		file_read_page(tmp.table_id, pgnum, &leaf);
		int vidx = -1;
		for (int i = 0;i < leaf.head_info.key_num;i++)
		{
			if (leaf.record[i].key == tmp.key)
			{
				vidx = i;
				break;
			}
		}
		if (vidx != -1)
			strcpy(leaf.record[i].value, tmp.value);
		file_write_page(tmp.table_id, pgnum, &leaf);
		(tmp_buf->is_pinned)--;
		pthread_mutex_unlock(&(tmp_buf->page_mutex));
	}
	t->undo_list.clear();
	end_trx(tid);
}
