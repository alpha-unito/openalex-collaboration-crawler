#include "ui.h"

void info_colored(std::string_view msg) {
    fmt::print("[i] {}\n", fmt::styled(msg, fmt::fg(fmt::color::cyan)));
}

void warn_colored(std::string_view msg) {
    fmt::print("[w] {}\n", fmt::styled(msg, fmt::fg(fmt::color::yellow)));
}

void ok_colored(std::string_view msg) {
    fmt::print("[i] {}\n", fmt::styled(msg, fmt::fg(fmt::color::green)));
}

void error_colored(std::string_view msg) {
    fmt::print(stderr, "[e] {}\n", fmt::styled(msg, fmt::fg(fmt::color::red)));
}

indicators::ProgressBar get_progress_bar(const std::string &message, u_int64_t maxValue) {
    return indicators::ProgressBar{indicators::option::BarWidth{40},
                                   indicators::option::Start{"["},
                                   indicators::option::End{"]"},
                                   indicators::option::PrefixText{"[i] " + message},
                                   indicators::option::ShowPercentage{true},
                                   indicators::option::ShowElapsedTime{true},
                                   indicators::option::ShowRemainingTime{true},
                                   indicators::option::ForegroundColor{indicators::Color::magenta},
                                   indicators::option::MaxProgress{maxValue}};
}