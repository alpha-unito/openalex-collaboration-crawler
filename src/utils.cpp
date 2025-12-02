#include "utils.h"
#include "ui.h"

#include <filesystem>
#include <fstream>
#include <iostream>

unsigned get_num_threads() {
    unsigned avail   = std::max(1u, std::thread::hardware_concurrency());
    const char *envp = std::getenv("GRAPH_NUM_THREADS");
    if (envp) {
        try {
            if (unsigned v = static_cast<unsigned>(std::stoul(envp)); v > 0) {
                avail = v;
            }
        } catch (...) {
        }
    }

    avail = std::max(1u, avail);

    info_colored(std::string("Utilizing ") + std::to_string(avail) + " threads");

    return avail;
}

void merge_files(const std::vector<std::string> &source_files, const std::string &output_file) {
    using namespace indicators;

    auto bar = get_progress_bar("Merging partials", source_files.size());

    std::ofstream out(output_file, std::ios::binary | std::ios::app);
    if (!out) {
        throw std::runtime_error("Cannot open output file: " + output_file);
    }

    size_t i = 0;
    for (const auto &f : source_files) {
        std::ifstream in(f, std::ios::binary);
        if (!in) {
            throw std::runtime_error("Cannot open source file: " + f);
        }
        out << in.rdbuf();
        ++i;
        bar.set_progress(i);
    }

    // Remove parts
    for (const auto &f : source_files) {
        std::error_code ec;
        std::filesystem::remove(f, ec);
    }
}

std::vector<std::string> split_str(const std::string &s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    std::istringstream iss(s);
    while (std::getline(iss, cur, delim)) {
        out.push_back(cur);
    }
    return out;
}

void seek_to_line_start(std::ifstream &ifs, std::uint64_t offset) {
    ifs.clear();
    ifs.seekg(offset);
    if (!ifs.good()) {
        return;
    }
    if (offset == 0) {
        return;
    }

    std::string dummy;
    std::getline(ifs, dummy);
}