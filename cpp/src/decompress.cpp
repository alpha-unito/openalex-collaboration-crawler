#include "openalex_json.h"
#include "ui.h"

#include <algorithm>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

std::vector<char> read_gz_to_memory(const std::string &gz_path) {
    if (!std::filesystem::exists(gz_path)) {
        throw std::runtime_error("File not found: " + gz_path);
    }

    std::ifstream file(gz_path, std::ios_base::in | std::ios_base::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open: " + gz_path);
    }

    boost::iostreams::filtering_streambuf<boost::iostreams::input> in;
    in.push(boost::iostreams::gzip_decompressor());
    in.push(file);

    std::vector<char> output;
    boost::iostreams::copy(in, boost::iostreams::back_inserter(output));

    return output;
}

void process_single_author_file(const std::string &gz_path, std::ofstream &out) {
    try {
        std::ifstream file(gz_path, std::ios::binary);
        if (!file) {
            error_colored("Cannot open " + gz_path);
            return;
        }

        boost::iostreams::filtering_istream in;
        in.push(boost::iostreams::gzip_decompressor());
        in.push(file);

        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) {
                continue;
            }

            auto [id, aff] = parse_json_author_line(line);
            if (aff.empty()) {
                continue;
            }

            out << R"({ "id":")" << id << R"(","affs":[)";
            for (size_t i = 0; i < aff.size(); ++i) {
                const auto &[country, year] = aff[i];
                out << "{\"" << year << "\":\"" << country << "\"}";
                if (i + 1 < aff.size()) {
                    out << ",";
                }
            }
            out << "]}\n";
        }

        out.flush(); // once per file

    } catch (const std::exception &e) {
        error_colored("Failed to decompress/parse " + gz_path + ": " + e.what());
    }
}

// Recursively finds all .gz files under the given root directory
std::vector<std::string> find_gz_files(const std::string &root) {
    std::vector<std::string> paths;
    for (const auto &it : std::filesystem::recursive_directory_iterator(root)) {
        if (it.is_regular_file() && it.path().extension() == ".gz") {
            paths.emplace_back(it.path().string());
        }
    }
    return paths;
}

std::string to_lower(std::string s) {
    std::ranges::transform(s, s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

void process_single_paper_file(
    const std::filesystem::path &gz_path, std::ofstream &out,
    const std::string &affiliation_country, const std::string &topic,
    const std::unordered_map<std::string, std::vector<std::vector<std::string>>>
        &author_affiliations) {

    try {
        std::ifstream file(gz_path, std::ios::binary);
        if (!file) {
            error_colored("Cannot open " + gz_path.string());
            return;
        }

        boost::iostreams::filtering_istream in;
        in.push(boost::iostreams::gzip_decompressor());
        in.push(file);

        const std::string formatted_topic = to_lower(topic);
        std::string line;

        while (std::getline(in, line)) {
            if (line.empty()) {
                continue;
            }

            line = to_lower(line);
            if (line.find(formatted_topic) == std::string::npos) {
                continue;
            }

            std::cout << "FOUND computer science" << std::endl;

            const auto [year, paper_authors] = get_paper_authors(line);

            if (paper_authors.empty()) {
                continue;
            }

            bool keep_paper = std::ranges::any_of(paper_authors, [&](const std::string &authorid) {
                const auto it = author_affiliations.find(authorid);
                if (it == author_affiliations.end()) {
                    return false;
                }

                const std::vector<std::vector<std::string>> &aff_years = it->second;
                if (year <= 0 || static_cast<size_t>(year) >= aff_years.size()) {
                    return false;
                }

                // Affiliations for the current year
                if (const std::vector<std::string> &current_affiliation = aff_years[year];
                    std::ranges::find(current_affiliation, affiliation_country) !=
                    current_affiliation.end()) {
                    return true;
                }

                // Search for closest previous year
                for (auto prev = year - 1; prev >= 0; --prev) {
                    if (const std::vector<std::string> &previous_affiliation = aff_years[prev];
                        !previous_affiliation.empty()) {
                        return std::ranges::find(previous_affiliation, affiliation_country) !=
                               previous_affiliation.end();
                    }
                }

                return false;
            });

            if (keep_paper) {
                out << line << '\n';
            }
        }

        out.flush();

    } catch (const std::exception &e) {
        error_colored("Failed to decompress/parse " + gz_path.string() + ": " +
                      std::string(e.what()));
    }
}