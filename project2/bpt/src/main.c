#include "bpt.h"
#include <string.h>
#include <inttypes.h>

// MAIN

int main() {
   char var[10], value[120];
   int64_t key;

   while (true) {
	  scanf("%s", var);
	  if (var[0] == 'o')
	  {
		  char pathname[120];
		 scanf("%s", pathname);
		 open_table(pathname);
	  }
	  else if (var[0] == 'i')
	  {
		 scanf("%"PRId64 "%s", &key, value);
		 if(db_insert(key, value)){
			printf("insert %"PRId64 " : failed\n", key);
		 }
		 else {
			printf("insert %"PRId64 " : successed\n", key);
		 }
	  }
	  else if (var[0] == 'd')
	  {
		  scanf("%"PRId64, &key);
		  if (db_delete(key)) {
			  printf("delete %"PRId64 " : failed\n", key);
		  }
		  else {
			  printf("delete %"PRId64 " : successed\n", key);
		  }
	  }
	  else if (var[0] == 'f')
	  {
		  scanf("%"PRId64, &key);
		  if (!db_find(key, value))
			  printf("find %"PRId64 " : successed\n", key);
		  else
			  printf("find % "PRId64 " : failed\n", key);
	  }
	  else if (var[0] == 'q') 
		  break;
	  else if(var[0] == 'p')
		 print_tree();
   }
}


