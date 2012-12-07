// definitions for container module


#ifdef HAVE_CONTAINER_LIB

int scrmfs_container_init(char * info, cs_store_handle_t * cs_store_handle);
int scrmfs_container_finalize();
int scrmfs_container_create(cs_store_handle_t * cs_store_handle, cs_set_handle_t * cs_set_handle);
int scrmfs_container_open(cs_set_handle_t * cs_set_handle, cs_container_handle_t** ch, int id, int fid);

#endif //HAVE_CONTAINER_LIB

