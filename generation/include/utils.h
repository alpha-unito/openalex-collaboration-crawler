#ifndef OPENALEX_AUTHORS_CPP_UTILS_H
#define OPENALEX_AUTHORS_CPP_UTILS_H
#include <algorithm>
#include <cstdlib>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using VecStr  = std::vector<std::string>;
using YearMap = std::unordered_map<std::string, VecStr>;
using AffMap  = std::unordered_map<std::string, YearMap>;

unsigned get_num_threads();
void merge_files(const std::vector<std::string> &source_files, const std::string &output_file);
std::vector<std::string> split_str(const std::string &s, char delim);
void seek_to_line_start(std::ifstream &ifs, std::uint64_t offset);
int split_graph_to_single_years(const std::string &input_file, const std::string &output_dir);
#endif // OPENALEX_AUTHORS_CPP_UTILS_H
