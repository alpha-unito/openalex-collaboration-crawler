#ifndef OPENALEX_AUTHORS_CPP_DECOMPRESS_H
#define OPENALEX_AUTHORS_CPP_DECOMPRESS_H
#include <filesystem>
#include <vector>
#include <set>

std::vector<std::uint8_t> read_gz_to_memory(const std::string &gz_path);
void process_single_author_file(const std::string &gz_path, std::ofstream &out);
std::vector<std::string> find_gz_files(const std::string &root);
void process_single_paper_file(const std::filesystem::path &gz_path, std::ofstream &out,
                               const std::string &affiliation_country, const std::string &concept_filter,
                               const std::set<std::string>& keep_author_list, double confidence);

#endif // OPENALEX_AUTHORS_CPP_DECOMPRESS_H
