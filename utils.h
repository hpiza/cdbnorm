#ifndef UTILS_H_
#define UTILS_H_

#include "common.h"
#include <chrono>
#include <string>

double kilobytes(long long bytes);
double megabytes(long long bytes);
double gigabytes(long long bytes);
long long memory_usage(Dataset &dataset);
void trim(string& str);
Dataset read_tsv(const string& filename, double rows_pct);
void print_memory (bool show_logs, Dataset& dataset);
void print_time (bool show_logs, string label, chrono::steady_clock::time_point begin, chrono::steady_clock::time_point end);
unsigned int count_rows(const string& filename);

#endif /* UTILS_H_ */
