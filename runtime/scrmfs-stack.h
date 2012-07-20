/* implements a fixed-size stack which stores integer values in range
 * of 0 to size-1, entire structure stored in an int array of size+2
 *   int size
 *   int last
 *   int entries[size] 
 * last records index within entries that points to item one past
 * the item at the top of the stack
 *
 * used to record which entries in a fixed-size array are free */

typedef struct {
  int size;
  int last;
} scrmfs_stack;

/* returns number of bytes needed to represent stack data structure */
size_t scrmfs_stack_bytes(int size);
  
/* intializes stack to record all entries as being free */
void scrmfs_stack_init(void* start, int size);

/* pops one entry from stack and returns its value */
int scrmfs_stack_pop(void* start);

/* pushes item onto free stack */
void scrmfs_stack_push(void* start, int value);
