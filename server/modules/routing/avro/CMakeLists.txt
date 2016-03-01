add_library(avrorouter SHARED avro.c ../binlog/blr_master.c ../binlog/blr_file.c avro_client.c avro_schema.c rbr.c avro_file.c)
set_target_properties(avrorouter PROPERTIES INSTALL_RPATH ${CMAKE_INSTALL_RPATH}:${MAXSCALE_LIBDIR} VERSION "1.0.0")
target_link_libraries(avrorouter ssl pthread log_manager jansson avro)
install(TARGETS avrorouter DESTINATION ${MAXSCALE_LIBDIR})
