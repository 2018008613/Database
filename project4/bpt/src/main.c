#include "bpt.h"
#include <string.h>
#include <inttypes.h>

// MAIN

int main() {
   char var[10], value[120];
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
		  if (delete(table_id, key)) {
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
		 
   }
}


