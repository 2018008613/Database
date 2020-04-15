#ifndef __TXN_MANGER_H__
#define __TXN_MANGER_H__

#include "buffer_manager.h"
#include <vector>
#include <unordered_map>

typedef struct trx_t trx_t;
typedef struct lock_t lock_t;

typedef struct undo {
	int table_id;
	int key;
	char* value;
}undo;

struct trx_t {
	int trx_id;
	int state = 0; // IDLE : 0, RUNNING : 1, WAITING : 2
	pthread_mutex_t trx_mutex;
	pthread_cond_t trx_cond;
	vector<lock_t*> trx_locks; // list of holding locks
	vector<undo> undo_list;
	lock_t* wait_lock = NULL; // lock object that trx is waiting
};

struct lock_t {
	int table_id;
	int key;
	int mode; // IDLE : 0, SHARED : 1, EXCLUSIVE : 2 
	trx_t* trx; // backpointer to lock holder
	lock_t* bflock;
	lock_t* aflock;
};

typedef struct LockHashTable {
	int page_id;
	int mode = 0; // IDLE : 0, SHARED : 1, EXCLUSIVE : 2 
	lock_t* head = NULL;
	lock_t* tail = NULL;
}LSH;

void extend_trx();
int begin_trx();
int end_trx(int tid);
void dfs(int* chk, int* visit, int trx_id, int now);
int find_deadlock(int chk, int64_t key, int trx_id);
int acquire_record_lock(int table_id, int chk, int64_t key, int trx_id);
void abort_trx(int tid);

#endif
