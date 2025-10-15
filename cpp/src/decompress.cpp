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

// Reads a .gz file fully into memory and decompresses it
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
