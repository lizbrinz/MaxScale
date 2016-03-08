add_library(MySQLAuth SHARED mysql_auth.c)
target_link_libraries(MySQLAuth maxscale-common)
set_target_properties(MySQLAuth PROPERTIES VERSION "1.0.0")
install(TARGETS MySQLAuth DESTINATION ${MAXSCALE_LIBDIR})

# if(BUILD_TESTS)
#   add_library(testprotocol SHARED testprotocol.c)
#   set_target_properties(testprotocol PROPERTIES VERSION "1.0.0")
#   target_link_libraries(testprotocol maxscale-common)
#   install(TARGETS testprotocol DESTINATION ${MAXSCALE_LIBDIR})
# endif()

# add_library(maxscaled SHARED maxscaled.c)
# target_link_libraries(maxscaled maxscale-common)
# set_target_properties(maxscaled PROPERTIES VERSION "1.0.0")
# install(TARGETS maxscaled DESTINATION ${MAXSCALE_LIBDIR})

