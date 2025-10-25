#include "utils.h"
#include "ui.h"

#include <filesystem>
#include <iostream>
#include <fstream>

unsigned get_num_threads() {
    unsigned avail   = std::max(1u, std::thread::hardware_concurrency());
    const char *envp = std::getenv("GRAPH_NUM_THREADS");
    if (envp) {
        try {
            unsigned v = static_cast<unsigned>(std::stoul(envp));
            if (v > 0) {
                avail = v;
            }
        } catch (...) {
        }
    }

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