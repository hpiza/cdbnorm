#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include "common.h"

void trim(string& str) {
    str.erase(0, str.find_first_not_of(' '));
    str.erase(str.find_last_not_of(' ') + 1);
}

unsigned int count_rows(const string& filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Error opening file: " << filename << endl;
        return 0;
    }
    string line;
    int rows = 0;
    while (getline(file, line)) {
        rows ++;
    }
    file.close();
    return rows - 1;
}

Dataset read_tsv(const string& filename, double rows_pct) {
    Dataset dataset;
    ifstream file(filename);
    string line;

    if (!file.is_open()) {
        cerr << "Error opening file: " << filename << endl;
        return dataset;
    }
    srand(time(NULL));

    bool first_row = true;
    bool skipped_row = false;
    while (getline(file, line)) {
        vector<string> row;
        stringstream ss(line);
        string cell;
        double r = ((double) rand() / (RAND_MAX));
        if(first_row || r <= rows_pct) {
            while (getline(ss, cell, '\t')) {
                trim(cell);
                if(first_row) dataset.header.push_back(cell);    
                else row.push_back(cell);
            }
            if(!first_row) dataset.rows.push_back(row);
            first_row = false;
        } else {
            skipped_row = true;
        }
    }
    file.close();
    dataset.all_rows_in_dataset = !skipped_row;
    return dataset;
}

double kilobytes(long long bytes) {
	return bytes / 1024.0;
}

double megabytes(long long bytes) {
	return kilobytes(bytes) / 1024.0;
}

double gigabytes(long long bytes) {
	return megabytes(bytes) / 1024.0;
}

long long memory_usage(Dataset &dataset) {
	long long memory = sizeof(dataset.header) + dataset.header.capacity() * sizeof(string) + sizeof(dataset.rows);
	for (auto& field : dataset.header) {
		memory += field.capacity();
	}
    for (auto& row : dataset.rows) {
		memory += /*sizeof(row); */ row.capacity() * sizeof(string);
		for (auto& cell: row) {
        	memory += cell.capacity();
		}
    }
    for(auto& dep: dataset.dependencies) {
        memory += sizeof(dep.first) + dep.second.capacity() * sizeof(int);
    }
    memory += sizeof(dataset.primaryKey) + dataset.primaryKey.capacity() * sizeof(int);
    memory += sizeof(dataset.mv_fields) + dataset.mv_fields.size() * sizeof(int);
    memory += dataset.extra_memory;
	return memory;
}

void print_memory (bool show_logs, Dataset& dataset) {
    if (!show_logs) return;
    long long mem = memory_usage(dataset);
    if (mem < 1024) {
        cout << "Memory = " << mem << " bytes";    
    } else if (mem < 1048576) {
        cout << "Memory = " << kilobytes(mem) << " KB";
    } else /*if (mem < 1073741824)*/ {
        cout << "Memory = " << megabytes(mem) << " MB";
    }/* else {
        cout << "Memory = " << gigabytes(mem) << " GB";
    }*/
    cout << endl << "------------------------" << endl;
}

void print_time (bool show_logs, string label, chrono::steady_clock::time_point begin, chrono::steady_clock::time_point end) {
    if (!show_logs) return;
    cout << label;

    int millis = chrono::duration_cast<chrono::milliseconds>(end - begin).count();
    if(millis < 1000) {
        cout << millis << " milliseconds" << endl;
    } else {
        double seconds = millis / 1000.0;
        if(seconds < 1200) {
            cout << fixed << setprecision(1) << seconds << " seconds" << endl;
        } else {
            double minutes = seconds / 60.0;
            cout << fixed << setprecision(1) << minutes << " minutes" << endl;
        }
    }
}
