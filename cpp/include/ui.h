#ifndef OPENALEX_AUTHORS_CPP_UI_H
#define OPENALEX_AUTHORS_CPP_UI_H
#include <algorithm>
#include <fmt/color.h>
#include <fmt/core.h>
#include <indicators/block_progress_bar.hpp>
#include <indicators/cursor_control.hpp>
#include <indicators/progress_bar.hpp>
#include <string_view>

void info_colored(std::string_view msg);
void warn_colored(std::string_view msg);
void ok_colored(std::string_view msg);
void error_colored(std::string_view msg);
indicators::ProgressBar get_progress_bar(const std::string &message, u_int64_t maxValue);
#endif // OPENALEX_AUTHORS_CPP_UI_H
