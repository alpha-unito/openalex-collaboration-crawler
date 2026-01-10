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

static std::tuple<std::string, std::string, std::string> parse_cli(int argc, const char **argv) {
    args::ArgumentParser parser("OpenAlex Collaboration Crawler / authors step");
    args::HelpFlag help(parser, "help", "Show help", {'h', "help"});

    args::ValueFlag<std::string> country(
        parser, "COUNTRY_CODE",
        "Two-letter country code to filter authors who have ever been affiliated there",
        {'c', "country-code-filter"});

    args::ValueFlag<std::string> input_dir(parser, "DIR", "OpenAlex AWS snapshot directory",
                                           {'i', "input-dir"});

    args::ValueFlag<std::string> output(parser, "FILE", "Output file name",
                                        {'o', "output-file-name"});

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

    if (!input_dir) {
        std::cerr << "Missing required -i/--openalex-input-dir\n" << parser;
        std::exit(2);
    }

    auto country_code_filter = country ? args::get(country) : "";
    auto output_file_name    = output ? args::get(output) : "authors.jsonl";
    auto input_dir_string    = args::get(input_dir) + "/data/authors";

    return {country_code_filter, std::filesystem::canonical(input_dir_string),
            std::filesystem::weakly_canonical(output_file_name)};
}

int main(int argc, const char **argv) {
    auto [country_code_filter, openalex_input_dir, output_file_name] = parse_cli(argc, argv);

    auto output_file_name_compress =
        !country_code_filter.empty()
            ? (output_file_name.empty() ? "authors_compressed.jsonl" : output_file_name)
            : "authors_compressed.jsonl";

    info_colored("Starting extractor phase");
    info_colored("Openalex AWS snapshot directory: " + openalex_input_dir);
    info_colored("Output file: " + output_file_name);
    if (!country_code_filter.empty()) {
        info_colored("Apply filter for country code: " + country_code_filter);
    }

    // ---------- Extract phase ----------
    auto paths = find_gz_files(openalex_input_dir);
    const unsigned num_threads =
        std::min<unsigned>(get_num_threads(), static_cast<unsigned>(paths.size()));

    if (paths.empty()) {
        warn_colored("No .gz files found. Exiting.");
        indicators::show_console_cursor(true);
        return 0;
    }

    auto extract_bar = get_progress_bar("Extracted files", paths.size());
    std::mutex bar_mtx;

    std::atomic<std::size_t> next_index{0};
    std::vector<std::jthread> workers;
    workers.reserve(num_threads);

    const auto number_of_files_to_process = paths.size();
    info_colored("Processing " + std::to_string(number_of_files_to_process) + " files");

    for (unsigned t = 0; t < num_threads; ++t) {
        workers.emplace_back([&, t] {
            std::string part_path = "/tmp/extractor.part." + std::to_string(t);
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

                process_single_author_file(paths[i], out);

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

    std::vector<std::string> parts;
    parts.reserve(num_threads);
    for (unsigned t = 0; t < num_threads; ++t) {
        parts.emplace_back("/tmp/extractor.part." + std::to_string(t));
    }
    merge_files(parts, "/tmp/authors.jsonl");

    // ---------- Load + Parse phase (SIMDJSON) ----------
    AffMap affiliation_dataset;
    load_and_compress_authors(affiliation_dataset, country_code_filter);

    auto save_bar = get_progress_bar("Storing affiliations", affiliation_dataset.size());
    std::ofstream out(output_file_name_compress, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Unable to create " + output_file_name_compress);
    }

    std::size_t count = 0;
    for (const auto &[openalex_id, year_map] : affiliation_dataset) {
        out << "{\"id\":\"" << openalex_id << "\",\"affs\":{";

        std::size_t yi = 0;
        for (const auto &[year, countries] : year_map) {
            out << "\"" << year << "\":[";
            for (std::size_t ci = 0; ci < countries.size(); ++ci) {
                out << "\"" << countries[ci] << "\"";
                if (ci + 1 < countries.size()) {
                    out << ",";
                }
            }
            out << "]";
            if (++yi < year_map.size()) {
                out << ",";
            }
        }

        out << "}}\n";

        if (++count % 1000 == 0) {
            save_bar.set_progress(count);
        }
    }

    save_bar.mark_as_completed();
    ok_colored("Completed compress stage");

    return 0;
}