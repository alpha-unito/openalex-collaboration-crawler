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

static std::tuple<std::string, std::string, std::string, std::string, std::string>
parse_cli(int argc, const char **argv) {
    args::ArgumentParser parser("OpenAlex Collaboration Crawler / authors step");
    args::HelpFlag help(parser, "help", "Show help", {'h', "help"});

    args::ValueFlag<std::string> input_dir(parser, "DIR", "OpenAlex AWS snapshot directory",
                                           {'i', "input-dir"});
    args::ValueFlag<std::string> output(parser, "FILE", "Output file name",
                                        {'o', "output-file-name"});

    args::ValueFlag<std::string> author_filter_file(
        parser, "PATH",
        "File produced with the author step. The papers resulted from this step will be authored "
        "from authors contained within this list",
        {'a', "author-file"});

    args::ValueFlag<std::string> coutry_filter(parser, "COUNTRY_CODE", "Country of affiliation ",
                                               {'c', "country-code-filter"});

    args::ValueFlag<std::string> topic(
        parser, "DIR",
        "String containing a topic. If set will filter papers against this given topic.",
        {'t', "topic"});

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

    auto country_code_filter = coutry_filter ? args::get(coutry_filter) : "";
    auto input_dir_string    = args::get(input_dir) + "/data/works";
    auto output_file_name    = output ? args::get(output) : "papers.jsonl";
    auto author_file         = author_filter_file ? args::get(author_filter_file) : "";
    auto topic_filter        = topic ? args::get(topic) : "";

    return {country_code_filter, std::filesystem::canonical(input_dir_string),
            std::filesystem::weakly_canonical(output_file_name),
            std::filesystem::canonical(author_file), topic_filter};
}

int main(int argc, const char **argv) {
    auto [country_code_filter, input_dir, output_filename, author_input_file, topic_filter] =
        parse_cli(argc, argv);

    info_colored("Openalex AWS snapshot directory: " + input_dir);
    info_colored("Output file: " + output_filename);
    if (!country_code_filter.empty()) {
        info_colored("Apply filter for country code: " + country_code_filter);
    }

    if (!author_input_file.empty()) {
        info_colored("Author filter file: " + author_input_file);
    }

    if (!topic_filter.empty()) {
        info_colored("Topic filter: " + topic_filter);
    }

    info_colored("Starting extractor phase");
}