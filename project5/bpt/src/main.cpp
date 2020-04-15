#include "bpt.h"
#include "txn_manager.h"
#include "buffer_manager.h"
#include <string.h>
#include <inttypes.h>

// MAIN

int foo = 1;
int foo2 = 2;
void* thread1(void* arg)
{
	printf("thread 1 begin\n");
	int tid = begin_trx();
	printf("tid is %d\n", tid);
	for (int i = 1;i <= 100;i++)
	{
		char v[11];
		if (i % 2 == 0)
		{
			if (db_find(1, i, v, tid))
			{
				printf("deadlock!\n");
				return NULL;
			}
			else
				printf("find %d success. %s\n", i, v);
		}
		else
		{
			char vars[11] = "b";
			if (db_update(1, i, vars, tid))
			{
				printf("deadlock!\n");
				return NULL;
			}
			else
				printf("update %d success\n", i);
		}
	}
	printf("thread 1 end\n");
	end_trx(tid);
	pthread_exit((void *)foo);
}

void* thread2(void* arg)
{
	printf("thread 2 begin\n");
	int tid = begin_trx();
	printf("tid is %d\n", tid);
	for (int i = 10000;i >= 1;i--)
	{
		char v[11];
		if (i % 2 == 0)
		{
			if (db_find(1, i, v, tid))
			{
				printf("deadlock!\n");
				return NULL;
			}
			else
				printf("find %d success. %s\n", i, v);
		}
		else
		{
			char vars[11] = "c";
			if (db_update(1, i, vars, tid))
			{
				printf("deadlock!\n");
				return NULL;
			}
			else
				printf("update %d success\n", i);
		}
	}
	printf("thread2 end\n");
	end_trx(tid);
	pthread_exit((void *)foo2);
}

void* thread3(void* arg)
{
	printf("thread 3 begin\n");
	int tid = begin_trx();
	printf("tid is %d\n", tid);
	for (int i = 1;i <= 10000;i++)
	{
		char v[11];
		if (i % 2 == 0)
		{
			if (db_find(1, i, v, tid))
			{
				printf("deadlock!\n");
				return NULL;
			}
			else
				printf("find %d success. %s\n", i, v);
		}
		else
		{
			char vars[11] = "d";
			if (db_update(1, i, vars, tid))
			{
				printf("deadlock!\n");
				return NULL;
			}
			else
				printf("update %d success\n", i);
		}
	}
	printf("thread 3 end\n");
	end_trx(tid);
	pthread_exit((void *)foo);
}

int main() {

	init_db(1000);
	char pname[11] = "a.db";
	char val[11] = "1";
	int tid = open_table(pname);
	for (int i = 1;i <= 10000;i++)
		insert(1, i, val);
	pthread_t tx1, tx2, tx3;
	//pthread_create(&tx1, 0, thread1, &tid);
	pthread_create(&tx2, 0, thread2, &tid);
	pthread_create(&tx3, 0, thread3, &tid);
	//pthread_join(tx1, NULL);
	pthread_join(tx2, NULL);
	pthread_join(tx3, NULL);
	close_table(1);

	/*char var[10], value[120];
	int64_t key;
	int table_id;

	while (true) {
	   scanf("%s", var);
	   if (var[0] == 't')
	   {
		   int num;
		   scanf("%d", &num);
		   init_db(num);
	   }
	   if (var[0] == 's')
	   {
		   shutdown_db();
	   }
	   if (var[0] == 'c')
	   {
		   int num;
		   scanf("%d", &num);
		   close_table(num);
	   }
	   if (var[0] == 'o')
	   {
		   char pathname[120];
		  scanf("%s", pathname);
		  table_id = open_table(pathname);
		  printf("table id is %d\n", table_id);
	   }
	   else if (var[0] == 'i')
	   {
		   scanf("%d", &table_id);
		  scanf("%"PRId64 "%s", &key, value);
		  if(insert(table_id, key, value)){
			 printf("insert %"PRId64 " : failed\n", key);
		  }
		  else {
			 printf("insert %"PRId64 " : successed\n", key);
		  }
	   }
	   else if (var[0] == 'j')
	   {
		   int t1, t2;
		   char pname[101];
		   scanf("%d%d%s", &t1, &t2, pname);
		   join_table(t1, t2, pname);
	   }
	   else if (var[0] == 'd')
	   {
		   scanf("%d", &table_id);
		   scanf("%"PRId64, &key);
		   if (db_delete(table_id, key)) {
			   printf("delete %"PRId64 " : failed\n", key);
		   }
		   else {
			   printf("delete %"PRId64 " : successed\n", key);
		   }
	   }
	   else if (var[0] == 'f')
	   {
		   scanf("%"PRId64, &key);
		   char find_var[120];
		   if (find(table_id, key))
		   {
			   strcpy(find_var, find(table_id, key));
			   printf("find %"PRId64 " : successed\n", key);
		   }
		   else
			   printf("find % "PRId64 " : failed\n", key);
	   }
	   else if (var[0] == 'q')
		   break;
	   else if (var[0] == 'p')
	   {
		   scanf("%d", &table_id);
		   print_tree(table_id);
	   }

	}*/
}


