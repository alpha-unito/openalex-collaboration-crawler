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
#endif // OPENALEX_AUTHORS_CPP_UTILS_H
