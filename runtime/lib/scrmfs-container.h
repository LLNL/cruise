// definitions for container module


#ifdef HAVE_CONTAINER_LIB

int scrmfs_container_init(char * info, cs_store_handle_t * cs_store_handle, cs_set_handle_t * cs_set_handle, size_t size, const char * prefix);
int scrmfs_container_finalize();
int scrmfs_container_open(cs_set_handle_t  cs_set_handle, cs_container_handle_t* ch, int fid, size_t size, const char * prefix);
int scrmfs_container_extend(cs_set_handle_t  cs_set_handle, cs_container_handle_t ch, off_t size);

int scrmfs_container_read(cs_container_handle_t ch, void * buf, size_t count, off_t offset);
int scrmfs_container_write(cs_container_handle_t ch, void * buf, size_t count, off_t offset);

#endif //HAVE_CONTAINER_LIB

