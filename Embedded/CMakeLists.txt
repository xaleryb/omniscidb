add_library(DBEngine SHARED DBETypes.cpp DBEngine.cpp DBEngine.h DBETypes.h DBEngine.pxd)

target_link_libraries(DBEngine QueryRunner QueryEngine DataMgr ${Arrow_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_DL_LIBS} ${TBB_LIBS})
install(TARGETS DBEngine DESTINATION lib)

add_executable(EmbeddedDbTest EmbeddedDbTest.cpp)
target_link_libraries(EmbeddedDbTest DBEngine)

add_executable(EmbeddedDbFSITest EmbeddedDbFSITest.cpp)
target_link_libraries(EmbeddedDbFSITest DBEngine)

#find_program(PYTHON "garbage in, garbage out")
#IF(PYTHON)
    set(MODULEDIR   "${CMAKE_CURRENT_BINARY_DIR}/omnidbe")
    make_directory (${MODULEDIR})
    set(SETUP_PY    "${CMAKE_CURRENT_BINARY_DIR}/setup.py")
    set(OUTPUT      "${MODULEDIR}/dbe.cpp")

    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/__init__.py" "${MODULEDIR}/" @ONLY)
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/dbe.pyx" "${MODULEDIR}/" @ONLY)
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/setup.in.py" "${CMAKE_CURRENT_BINARY_DIR}/setup.py.in" @ONLY)
    # cannot be done in one step, splitting in configure_file and file commands
    file(GENERATE OUTPUT "${SETUP_PY}" INPUT "${CMAKE_CURRENT_BINARY_DIR}/setup.py.in")

    add_custom_command(OUTPUT ${OUTPUT}
        DEPENDS DBEngine ${SETUP_PY} ${CMAKE_CURRENT_SOURCE_DIR}/DBEngine.h ${CMAKE_CURRENT_SOURCE_DIR}/dbe.pyx ${CMAKE_CURRENT_SOURCE_DIR}/DBEngine.pxd
        COMMAND cd ${CMAKE_CURRENT_BINARY_DIR} && python setup.py build_ext -g -f -I ${CMAKE_CURRENT_SOURCE_DIR}
        )

    add_custom_target(Python ALL DEPENDS ${OUTPUT})

    add_custom_target(editable-install
        cd ${CMAKE_CURRENT_BINARY_DIR} && pip install -v -e .
        DEPENDS Python)

    install(CODE "execute_process(COMMAND cd ${CMAKE_CURRENT_BINARY_DIR}; pip install -v . )")

#ENDIF(PYTHON)
