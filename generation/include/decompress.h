#ifndef OPENALEX_AUTHORS_CPP_DECOMPRESS_H
#define OPENALEX_AUTHORS_CPP_DECOMPRESS_H
#include <filesystem>
#include <set>
#include <vector>

std::vector<std::uint8_t> read_gz_to_memory(const std::string &gz_path);
void process_single_author_file(const std::string &gz_path, std::ofstream &out);
std::vector<std::string> find_gz_files(const std::string &root);
void process_single_paper_file(const std::filesystem::path &, std::ofstream &, const std::string &,
                               const std::string &, const std::set<std::string> &, double,
                               std::tuple<std::unordered_map<std::string, unsigned long int>,
                                          std::unordered_map<std::string, unsigned long int>> &);

#endif // OPENALEX_AUTHORS_CPP_DECOMPRESS_H
