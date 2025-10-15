#include <fstream>
#include <iostream>
#include <simdjson.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "decompress.h"
#include "openalex_json.h"
#include "ui.h"

std::pair<std::string, std::vector<std::pair<std::string, std::string>>>
parse_json_author_line(std::string_view json_line) {
    using Result = std::pair<std::string, std::vector<std::pair<std::string, std::string>>>;
    std::vector<std::pair<std::string, std::string>> affs;
    std::string id = "not found";

    static thread_local simdjson::ondemand::parser parser; // thread-safe reuse per thread

    try {
        simdjson::padded_string padded(json_line);
        auto doc = parser.iterate(padded);

        // Extract id
        auto id_field = doc["id"];
        if (!id_field.error()) {
            id = std::string(id_field.get_string().value());
        }

        // Extract affiliations
        auto affs_field = doc["affiliations"];
        if (!affs_field.error()) {
            auto aff_array = affs_field.get_array();
            for (auto aff : aff_array.value()) {
                std::string country_code = "No institution found";

                auto inst = aff["institution"];
                if (!inst.error()) {
                    auto cc = inst["country_code"];
                    if (!cc.error()) {
                        country_code = std::string(cc.get_string().value());
                    }
                }

                auto years = aff["years"];
                if (!years.error()) {
                    auto years_array = years.get_array();
                    for (auto y : years_array.value()) {
                        std::string year_str;
                        if (y.type() == simdjson::ondemand::json_type::number) {
                            year_str = std::to_string(y.get_int64().value());
                        } else {
                            year_str = "-1";
                        }
                        affs.emplace_back(country_code, year_str);
                    }
                }
            }
        }
    } catch (...) {
    }

    return {id, affs};
}

void load_and_compress_authors(AffMap &affiliation_dataset, std::string &country_code_filter) {
    std::error_code ec;
    auto sz       = std::filesystem::file_size("/tmp/authors.jsonl", ec);
    auto load_bar = get_progress_bar("Filtering data", sz);

    simdjson::ondemand::parser parser;
    std::ifstream file("/tmp/authors.jsonl", std::ios::binary);
    if (!file) {
        throw std::runtime_error("Unable to open /tmp/authors.jsonl");
    }

    std::string line;
    std::uint64_t lines_read            = 0;
    std::uint64_t data_read             = 0;
    const std::string formatted_country = "\"" + country_code_filter + "\"";

    while (std::getline(file, line)) {
        ++lines_read;
        data_read += line.size() + 1;

        if (lines_read % 1000 == 0) {
            load_bar.set_progress(data_read);
        }

        if (!country_code_filter.empty() && line.find(formatted_country) == std::string::npos) {
            continue;
        }

        simdjson::padded_string padded(line);
        auto doc_result = parser.iterate(padded);
        if (doc_result.error()) {
            continue;
        }

        auto &doc = doc_result.value();

        auto id_res = doc["id"].get_string();
        if (id_res.error()) {
            continue;
        }
        std::string id = std::string(id_res.value());

        auto &year_map = affiliation_dataset[id];

        auto affiliation_array = doc["affs"].get_array();
        if (affiliation_array.error()) {
            continue;
        }

        for (auto field : affiliation_array) {
            for (auto itm : field.get_object()) {
                const auto year        = std::string(itm.unescaped_key().value());
                const auto affiliation = std::string(itm.value().get_string().value());

                if (!year_map.contains(year)) {
                    year_map[year] = {};
                }
                if (auto &vec = year_map[year]; std::ranges::find(vec, affiliation) == vec.end()) {
                    year_map[year].emplace_back(affiliation);
                }
            }
        }
    }

    load_bar.mark_as_completed();
}