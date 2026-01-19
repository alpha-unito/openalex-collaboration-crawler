cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0167 OLD)

project(openalex_collaboration_crawler LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

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
    simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG v4.0.7
)

set(BOOST_ENABLE_CMAKE ON CACHE BOOL "" FORCE)
set(BOOST_INCLUDE_LIBRARIES filesystem iostreams)

find_package(Boost QUIET COMPONENTS filesystem iostreams)

if (NOT Boost_FOUND)
    FetchContent_Declare(
        boost
        GIT_REPOSITORY https://github.com/boostorg/boost.git
        GIT_TAG boost-1.89.0
    )
    FetchContent_MakeAvailable(boost)
endif()

FetchContent_MakeAvailable(fmt args indicators simdjson)

file(GLOB_RECURSE PROJECT_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_SOURCE_DIR}/src/decompress.cpp"
    "${CMAKE_SOURCE_DIR}/src/openalex_json.cpp"
    "${CMAKE_SOURCE_DIR}/src/ui.cpp"
    "${CMAKE_SOURCE_DIR}/src/utils.cpp"
)

set(PROJECT_INCLUDE_DIRS
    ${CMAKE_SOURCE_DIR}/include
    ${args_SOURCE_DIR}
    ${indicators_SOURCE_DIR}/include
    ${simdjson_SOURCE_DIR}/include
)

add_executable(openalex_authors
    src/authors.cpp
    ${PROJECT_SOURCES}
)

target_include_directories(openalex_authors PRIVATE
    ${PROJECT_INCLUDE_DIRS}
)

target_link_libraries(openalex_authors PRIVATE
    fmt::fmt
    Boost::filesystem
    Boost::iostreams
    simdjson::simdjson
)

add_executable(openalex_papers
    src/papers.cpp
    ${PROJECT_SOURCES}
)

target_include_directories(openalex_papers PRIVATE
    ${PROJECT_INCLUDE_DIRS}
)

target_link_libraries(openalex_papers PRIVATE
    fmt::fmt
    Boost::filesystem
    Boost::iostreams
    simdjson::simdjson
)

add_executable(openalex_graph
    src/graph.cpp
    ${PROJECT_SOURCES}
)

target_include_directories(openalex_graph PRIVATE
    ${PROJECT_INCLUDE_DIRS}
)

target_link_libraries(openalex_graph PRIVATE
    fmt::fmt
    Boost::filesystem
    Boost::iostreams
    simdjson::simdjson
)
