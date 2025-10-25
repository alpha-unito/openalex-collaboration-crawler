#ifndef OPENALEX_AUTHORS_CPP_OPENALEX_JSON_H
#define OPENALEX_AUTHORS_CPP_OPENALEX_JSON_H

#include "utils.h"
#include <filesystem>
#include <string>
#include <vector>

std::pair<std::string, std::vector<std::pair<std::string, std::string>>>
parse_json_author_line(std::string_view json_line);
void load_and_compress_authors(AffMap &affiliation_dataset, std::string &country_code_filter);
std::unordered_map<std::string, std::vector<std::vector<std::string>>>
load_authors_affiliations(const std::filesystem::path &author_file);
std::tuple<int64_t, std::vector<std::string>> get_paper_authors(std::string raw_json);

#endif // OPENALEX_AUTHORS_CPP_OPENALEX_JSON_H
