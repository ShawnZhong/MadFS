string(TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE)

if (${CMAKE_BUILD_TYPE} STREQUAL "relwithdebinfo")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g" CACHE STRING
        "Flags used by the C++ compiler during RelwithDebInfo build" FORCE)
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "asan")
    set(CMAKE_CXX_FLAGS_ASAN
        "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer" CACHE STRING
        "Flags used by the C++ compiler during AddressSanitizer build." FORCE)

    set(CMAKE_SHARED_LINKER_FLAGS_ASAN
        "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address" CACHE STRING
        "Flags used by the C++ linker during AddressSanitizer build." FORCE)
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "ubsan")
    set(CMAKE_CXX_FLAGS_UBSAN
        "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined" CACHE STRING
        "Flags used by the C++ compiler during UndefinedBehaviourSanitizer build." FORCE)
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "msan")
    set(CMAKE_CXX_FLAGS_MSAN
        "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=memory -fsanitize-memory-track-origins=2 -fno-omit-frame-pointer" CACHE STRING
        "Flags used by the C++ compiler during MemorySanitizer build." FORCE)
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "tsan")
    set(CMAKE_CXX_FLAGS_TSAN
        "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread" CACHE STRING
        "Flags used by the C++ compiler during ThreadSanitizer build." FORCE)
endif ()

if (${CMAKE_BUILD_TYPE} STREQUAL "pmemcheck")
    set(CMAKE_CXX_FLAGS_PMEMCHECK
        "${CMAKE_CXX_FLAGS_DEBUG} -mno-avx512f" CACHE STRING
        "Flags used by the C++ compiler during Pmemcheck build." FORCE)

    set(ULAYFS_USE_PMEMCHECK ON)
    FetchContent_Declare(pmemcheck URL https://github.com/ShawnZhong/pmemcheck/releases/download/v3.17/pmemcheck.tgz)
    FetchContent_MakeAvailable(pmemcheck)
    include_directories(${pmemcheck_SOURCE_DIR}/include)

    if (${ULAYFS_PERSIST} STREQUAL "PMDK")
        message(WARNING "pmemcheck is not compatible with ULAYFS_PERSIST == PMDK. Use native instead.")
        set(ULAYFS_PERSIST "NATIVE")
    endif ()
endif ()


if (${CMAKE_BUILD_TYPE} STREQUAL "profile")
    set(CMAKE_CXX_FLAGS_PROFILE
        "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}" CACHE STRING
        "Flags used by the C++ compiler during Profile build." FORCE)

    FetchContent_Declare(flamegraph GIT_REPOSITORY https://github.com/brendangregg/FlameGraph)
    FetchContent_MakeAvailable(flamegraph)
endif ()
