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

int split_graph_to_single_years(const std::string &input_file, const std::string &output_dir) {
    if (!std::filesystem::exists(output_dir)) {
        std::filesystem::create_directory(output_dir);
    }

    std::unordered_map<std::string, std::ofstream> yearFiles;

    // Iterate over files in current directory
    for (const auto &entry : std::filesystem::directory_iterator(std::filesystem::current_path())) {
        if (entry.path().extension() == ".csv" && entry.path().filename() != "split_by_year.csv") {
            std::ifstream infile(entry.path());
            if (!infile.is_open()) {
                std::cerr << "Failed to open: " << entry.path() << std::endl;
                continue;
            }

            std::string line;
            while (std::getline(infile, line)) {
                if (line.empty()) {
                    continue;
                }

                std::vector<std::string> tokens = split_str(line, ',');
                if (tokens.empty()) {
                    continue;
                }

                const std::string &year = tokens[0];

                // Create and cache the output stream for the year
                if (!yearFiles.contains(year)) {
                    std::string yearFilePath = output_dir + "/" + year + ".csv";
                    yearFiles[year].open(yearFilePath, std::ios::app);
                    if (!yearFiles[year].is_open()) {
                        std::cerr << "Failed to open output file for year: " << year << std::endl;
                        continue;
                    }
                }

                yearFiles[year] << line << '\n';
            }

            infile.close();
        }
    }

    // Close all output files
    for (auto &pair : yearFiles) {
        pair.second.close();
    }

    std::cout << "CSV files processed and output saved to ./output/" << std::endl;
    return 0;
}