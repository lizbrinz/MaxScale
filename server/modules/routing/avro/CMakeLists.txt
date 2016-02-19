add_library(avrorouter SHARED avro.c blr_master.c blr_file.c avro_client.c)
set_target_properties(avrorouter PROPERTIES INSTALL_RPATH ${CMAKE_INSTALL_RPATH}:${MAXSCALE_LIBDIR} VERSION "1.0.0")
target_link_libraries(avrorouter ssl pthread log_manager)
install(TARGETS avrorouter DESTINATION ${MAXSCALE_LIBDIR})
