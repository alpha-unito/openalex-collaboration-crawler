cmake_minimum_required(VERSION 3.20)
project(openalex_collaboration_crawler LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

# -----------------------------
# Dependencies
# -----------------------------

FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG 11.0.2
)

FetchContent_Declare(
        args
        GIT_REPOSITORY https://github.com/Taywee/args.git
        GIT_TAG 6.4.7
)

FetchContent_Declare(
        indicators
        GIT_REPOSITORY https://github.com/p-ranav/indicators.git
        GIT_TAG v2.3
)

FetchContent_Declare(
        boost
        GIT_REPOSITORY https://github.com/boostorg/boost.git
        GIT_TAG boost-1.89.0
)

FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG v4.0.7
)

set(BOOST_ENABLE_CMAKE ON CACHE BOOL "" FORCE)
set(BOOST_INCLUDE_LIBRARIES iostreams filesystem)

FetchContent_MakeAvailable(fmt args indicators boost simdjson)

# -----------------------------
# Sources
# -----------------------------
file(GLOB_RECURSE PROJECT_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_SOURCE_DIR}/src/*.cpp"
)

# -----------------------------
# Executable: openalex_authors
# -----------------------------
add_executable(openalex_authors
        ${PROJECT_SOURCES}
        authors.cpp
)

target_include_directories(openalex_authors PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${args_SOURCE_DIR}
        ${indicators_SOURCE_DIR}/include
        ${boost_SOURCE_DIR}
        ${simdjson_SOURCE_DIR}/include
)

target_link_libraries(openalex_authors PRIVATE
        fmt::fmt
        Boost::iostreams
        Boost::filesystem
        simdjson::simdjson
)

# -----------------------------
# Executable: openalex_papers
# -----------------------------
add_executable(openalex_papers
        ${PROJECT_SOURCES}
        papers.cpp
)

target_include_directories(openalex_papers PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${args_SOURCE_DIR}
        ${indicators_SOURCE_DIR}/include
        ${boost_SOURCE_DIR}
        ${simdjson_SOURCE_DIR}/include
)

target_link_libraries(openalex_papers PRIVATE
        fmt::fmt
        Boost::iostreams
        Boost::filesystem
        simdjson::simdjson
)

# -----------------------------
# Status summary
# -----------------------------
message(STATUS "--------------------------------------------------")
message(STATUS "Build Configuration:")
message(STATUS "  C++ standard:     ${CMAKE_CXX_STANDARD}")
message(STATUS "  fmt:              ${fmt_SOURCE_DIR}")
message(STATUS "  args:             ${args_SOURCE_DIR}")
message(STATUS "  indicators:       ${indicators_SOURCE_DIR}")
message(STATUS "  Boost fetched:    ${boost_SOURCE_DIR}")
message(STATUS "  simdjson:         ${simdjson_SOURCE_DIR}")
message(STATUS "  include dir:      ${CMAKE_SOURCE_DIR}/include")
message(STATUS "--------------------------------------------------")
