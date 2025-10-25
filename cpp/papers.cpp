#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "decompress.h"
#include "openalex_json.h"
#include "ui.h"
#include "utils.h"
#include <args.hxx>

static std::tuple<std::string, std::string, std::string, std::string> parse_cli(int argc,
                                                                                const char **argv) {
    args::ArgumentParser parser("OpenAlex Collaboration Crawler / authors step");
    args::HelpFlag help(parser, "help", "Show help", {'h', "help"});

    args::ValueFlag<std::string> input_dir(parser, "DIR", "OpenAlex AWS snapshot directory",
                                           {'i', "input-dir"});
    args::ValueFlag<std::string> output(parser, "FILE", "Output file name",
                                        {'o', "output-file-name"});

    args::ValueFlag<std::string> coutry_filter(parser, "COUNTRY_CODE", "Country of affiliation ",
                                               {'c', "country-code-filter"});

    args::ValueFlag<std::string> topic_id(
        parser, "NUMBER",
        "ID of targeted topic. Get it from https://openalex.org/fields/. For example, for "
        "\"Computer science\" has Field ID 178",
        {'f', "field"});

    try {
        parser.ParseCLI(argc, argv);
    } catch (const args::Completion &e) {
        std::cout << e.what();
        std::exit(0);
    } catch (const args::Help &) {
        std::cout << parser;
        std::exit(0);
    } catch (const args::ParseError &e) {
        std::cerr << e.what() << "\n" << parser;
        std::exit(1);
    }

    auto country_code_filter = coutry_filter ? args::get(coutry_filter) : "";
    auto input_dir_string    = input_dir ? args::get(input_dir) + "/data/works" : "";
    auto output_file_name    = output ? args::get(output) : "papers.jsonl";

    // TODO: Expand also to subfields
    auto topic_filter = topic_id ? "https://openalex.org/fields/" + args::get(topic_id) : "";

    return {country_code_filter, std::filesystem::canonical(input_dir_string),
            std::filesystem::weakly_canonical(output_file_name), topic_filter};
}

int main(int argc, const char **argv) {
    auto [country_code_filter, input_dir, output_filename, topic_filter] = parse_cli(argc, argv);

    if (input_dir.empty()) {
        error_colored("No input dir for AWS snapshot provided.");
        exit(EXIT_FAILURE);
    }

    if (country_code_filter.empty()) {
        error_colored("No country code filter provided.");
        exit(EXIT_FAILURE);
    }

    if (topic_filter.empty()) {
        error_colored("No Field give. Search for a valid ID at https://openalex.org/fields/");
        exit(EXIT_FAILURE);
    }

    info_colored("Openalex AWS snapshot: " + input_dir);
    info_colored("Output  file: " + output_filename);
    info_colored("Country code: " + country_code_filter);
    info_colored("Topic filter: " + topic_filter);
    warn_colored("=============================");

    const auto paths = find_gz_files(input_dir);

    const unsigned num_threads =
        std::min<unsigned>(get_num_threads(), static_cast<unsigned>(paths.size()));

    std::atomic<std::size_t> next_index{0};
    std::vector<std::jthread> workers;
    workers.reserve(num_threads);

    const auto number_of_files_to_process = paths.size();
    info_colored("Processing " + std::to_string(number_of_files_to_process) + " files");

    auto extract_bar = get_progress_bar("Processing files", paths.size());
    std::mutex bar_mtx;

    for (unsigned t = 0; t < num_threads; ++t) {
        workers.emplace_back([&, t] {
            std::string part_path = "/tmp/paper_extraction.part." + std::to_string(t);
            std::ofstream out(part_path, std::ios::binary | std::ios::trunc);
            if (!out) {
                fmt::print(stderr, "Cannot create {}\n", part_path);
                return;
            }
            while (true) {
                const std::size_t i = next_index.fetch_add(1, std::memory_order_relaxed);
                if (i >= number_of_files_to_process) {
                    break;
                }

                process_single_paper_file(paths.at(i), out, country_code_filter, topic_filter);

                {
                    std::scoped_lock lk(bar_mtx);
                    extract_bar.tick();
                }
            }
        });
    }

    for (auto &th : workers) {
        th.join();
    }

    std::vector<std::string> part_files;

    for (int i = 0; i < num_threads; ++i) {
        part_files.push_back("/tmp/paper_extraction.part." + std::to_string(i));
    }

    merge_files(part_files, output_filename);
}