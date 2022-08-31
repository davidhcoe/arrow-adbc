# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.10)
get_filename_component(REPOSITORY_ROOT "../.." ABSOLUTE)
message(STATUS "${REPOSITORY_ROOT}")
list(APPEND CMAKE_MODULE_PATH "${REPOSITORY_ROOT}/c/cmake_modules/")
include(AdbcDefines)
include(BuildUtils)
include(DefineOptions)

project(adbc_driver_manager
        VERSION "${ADBC_BASE_VERSION}"
        LANGUAGES CXX)
include(CTest)

add_arrow_lib(adbc_driver_manager
              SOURCES
              adbc_driver_manager.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverManager
              PKG_CONFIG_NAME
              adbc-driver-manager)
include_directories(SYSTEM ${REPOSITORY_ROOT})
include_directories(SYSTEM ${REPOSITORY_ROOT}/c/)
foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
endforeach()

install(FILES "${REPOSITORY_ROOT}/adbc.h" adbc_driver_manager.h DESTINATION include)

if(ADBC_BUILD_TESTS)
  find_package(Arrow REQUIRED)
  if(ADBC_TEST_LINKAGE STREQUAL "shared")
    set(TEST_LINK_LIBS adbc_driver_manager_shared arrow_shared)
  else()
    set(TEST_LINK_LIBS adbc_driver_manager_static arrow_static)
  endif()

  add_test_case(driver_manager_test
                PREFIX
                adbc
                SOURCES
                adbc_driver_manager_test.cc
                ../validation/adbc_validation.c
                ${REPOSITORY_ROOT}/c/vendor/nanoarrow/nanoarrow.c
                EXTRA_LINK_LIBS
                ${TEST_LINK_LIBS})
  target_include_directories(adbc-driver-manager-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/vendor/nanoarrow/)
endif()

validate_config()
config_summary_message()