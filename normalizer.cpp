#include "common.h"
#include "utils.h"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using namespace std;

#define MIN(a, b) ((a) < (b)? (a): (b))
#define MAX(a, b) ((a) > (b)? (a): (b))

bool contains(vector<int>& vec, int f) {
    return find(vec.begin(), vec.end(), f) != vec.end();
}

bool is_multivalued(Dataset &dataset, int f) {
    return dataset.mv_fields.find(f) != dataset.mv_fields.end();
}

void remove(vector<int>& vec, int f) {
    vec.erase(find(vec.begin(), vec.end(), f));
}

void find_multivalued_fields(Dataset &dataset) {
    for(int r = 0; r < dataset.rows.size(); r ++) {
        vector<string> row = dataset.rows[r];
        for(int f = 0; f < row.size(); f ++) {
            if(row[f].find(',') != string::npos)
                dataset.mv_fields.insert(f);
        }
    }
}

bool is_dependent(Dataset &dataset, int key, int not_key) {
    if(key >= dataset.header.size() || not_key >= dataset.header.size()) return false;
    map<string, string> values_map;
    for(int r = 0; r < dataset.rows.size(); r ++) {
        vector<string> row = dataset.rows[r];
        if(values_map.count(row[key]) > 0) {
            if(row[not_key] != values_map[row[key]])
                return false;
        } else {
            values_map[row[key]] = row[not_key];
        }
    }
    return values_map.size() > 0;
}

bool is_mutually_defined_before(Dataset &dataset, int f) {
    for(int i = 0; i < f; i ++) {
        if(contains(dataset.bi_dependencies[i], f)) return true;
    }
    return false;
}

void find_functional_dependencies(Dataset &dataset) {
    for (int i = 0; i < dataset.header.size(); i++) {
        if(is_multivalued(dataset, i)) continue;
        for (int j = i + 1; j < dataset.header.size(); j++) {
            if(is_multivalued(dataset, j)) continue;
            bool idefinesj = is_dependent(dataset, i, j);
            bool jdefinesi = is_dependent(dataset, j, i);
            if (idefinesj && jdefinesi) {
                if (!is_mutually_defined_before(dataset, j)) {
                    dataset.dependencies[i].push_back(j);
                    dataset.bi_dependencies[i].push_back(j);
                }
            }
            else if(idefinesj) dataset.dependencies[i].push_back(j);
            else if(jdefinesi) dataset.dependencies[j].push_back(i);
        }
    }
}

void build_next_combination(vector<int> &selectable_fields, vector<bool> &selector, vector<int> &current_permutation) {
	bool found_next = next_permutation(selector.begin(), selector.end());
	if(!found_next) {
		for(int i = 0; i < selector.size(); i ++) {
			selector[i] = i >= selector.size() - current_permutation.size() - 1;
		}
	}
	current_permutation.clear();
	for (size_t i = 0; i < selectable_fields.size(); ++i) {
		if (selector[i]) {
			current_permutation.push_back(selectable_fields[selectable_fields.size() - i - 1]);
		}
	}
	sort(current_permutation.begin(), current_permutation.end());
}

void find_primary_key(Dataset& dataset, vector<int> &selectable_fields, vector<bool> &selector, vector<int> &candidate_key) {
    set<vector<string>> values_for_key;
    bool found_duplicates = false;
    for (int r = 0; !found_duplicates && r < dataset.rows.size(); r ++) {
        vector<string> row = dataset.rows[r];
        vector<string> value_for_key;
        for (int I : candidate_key) {
            value_for_key.push_back(row[I]);
        }
        if (!values_for_key.insert(value_for_key).second) {
            found_duplicates = true;
        }
    }
    if(found_duplicates) {
        build_next_combination(selectable_fields, selector, candidate_key);
        find_primary_key(dataset, selectable_fields, selector, candidate_key);
    }
}

void find_primary_key(Dataset& dataset) {
    vector<int> selectable_fields;
    for(int i = 0; i < dataset.header.size(); i ++) {
        if(!is_multivalued(dataset, i)) selectable_fields.push_back(i);
    }
    vector<bool> selector(dataset.header.size() - dataset.mv_fields.size(), false);
    selector[selector.size() - 1] = true;
    vector<int> candidate_key = {0};
    find_primary_key(dataset, selectable_fields, selector, candidate_key);
    for(int f: candidate_key) dataset.primaryKey.push_back(f);
}

void find_dependencies_to_pk(Dataset &dataset) {
    auto& pk = dataset.primaryKey;
    // Si la clave no es compuesta, entonces ya se añadieron sus dependencias funcionales
    if(pk.size() == 1) return;
    // La clave principal definirá a todos los atributos no llave no definidos por un atributo llave
    set<int> banned_fields;
    for(int f: pk) {
        banned_fields.insert(f);
    }
    for(auto& dep: dataset.dependencies) {
        if (!contains(pk, dep.first)) continue;
        for(int f: dep.second) {
            banned_fields.insert(f);
        }
    }
    for (int i = 0; i < dataset.header.size(); i++) {
        if(is_multivalued(dataset, i)) continue;
        if(banned_fields.find(i) == banned_fields.end()) {
            dataset.dependencies[-1].push_back(i);
        }
    }
}

void create_table_from_mvfield(Dataset &dataset, int mvf, string filename, string output_folder) {
    string out_filename = output_folder + "/mvf_" + dataset.header[mvf] + ".tsv";
    ofstream ofs(out_filename);
    if (!ofs.is_open()) {
        throw ios_base::failure("Failed to create file: " + out_filename + ". Make sure folder " + output_folder + " exists.");
    }
    for(int pk: dataset.primaryKey) {
        ofs << dataset.header[pk] << "_PK,";
    }
    ofs << dataset.header[mvf] << "_PK\n";

    ifstream file(filename);
    string line;
    getline(file, line);  // skip header
    while (getline(file, line)) {
        vector<string> row;
        stringstream ss1(line);
        string cell;
        while (getline(ss1, cell, '\t')) {
            trim(cell);
            row.push_back(cell);
        }
        string pk_values = "";
        for(int pk: dataset.primaryKey) {
            pk_values += row[pk] + "\t";
        }
        string segment;
        stringstream ss2(row[mvf]);
        while(getline(ss2, segment, ',')) {
            ofs << pk_values << segment << "\n";
        }
    }
    file.close();
    ofs.close();
}

void create_table_from_mvfield(Dataset &dataset, int mvf, string output_folder) {
    string out_filename = output_folder + "/mvf_" + dataset.header[mvf] + ".tsv";
    ofstream ofs(out_filename);
    if (!ofs.is_open()) {
        throw ios_base::failure("Failed to create file: " + out_filename + ". Make sure folder " + output_folder + " exists.");
    }
    for(int pk: dataset.primaryKey) {
        ofs << dataset.header[pk] << "_PK,";
    }
    ofs << dataset.header[mvf] << "_PK\n";

    for(vector<string> row: dataset.rows) {
        string pk_values = "";
        for(int pk: dataset.primaryKey) {
            pk_values += row[pk] + "\t";
        }
        string segment;
        stringstream ss2(row[mvf]);
        while(getline(ss2, segment, ',')) {
            ofs << pk_values << segment << "\n";
        }
    }
    ofs.close();
}

void create_tables_from_mvfields(Dataset &dataset, string filename, string output_folder) {
    for(int mvf: dataset.mv_fields) {
        if(dataset.all_rows_in_dataset) create_table_from_mvfield(dataset, mvf, output_folder);
        else create_table_from_mvfield(dataset, mvf, filename, output_folder);
    }
}

bool is_defined_by_other(int definer, int defined, Dataset &dataset) {
    for(auto& dep: dataset.dependencies) {
        if(dep.first != definer && contains(dep.second, defined)) {
            return true;
        }
    }
    return false;
}

void delete_transitive_dependencies(Dataset &dataset) {
    if(dataset.dependencies.empty()) return;
    queue<int> definers;
    if(dataset.primaryKey.size() > 1) definers.push(-1);
    for(int f: dataset.primaryKey) {
        definers.push(f);
    }
    while(!definers.empty()) {
        int definer = definers.front();
        definers.pop();
        if(dataset.dependencies.count(definer) == 0) continue;
        set<int> to_remove;
        for(int defined: dataset.dependencies[definer]) {
            if(is_defined_by_other(definer, defined, dataset)) {
                to_remove.insert(defined);
            } else {
                definers.push(defined);
            }
        }
        for(int f: to_remove) {
            remove(dataset.dependencies[definer], f);
        }
        if(definer >= 0 && dataset.dependencies[definer].empty()) {
            dataset.dependencies.erase(dataset.dependencies.find(definer));
        }
    }
}

void create_table_from_dependency_to_pk(Dataset& dataset, string filename, string output_folder) {
    if(dataset.primaryKey.size() == 1) return;
    string out_filename = output_folder + "/main_table.tsv";
    ofstream ofs(out_filename);
    if (!ofs.is_open()) {
        throw ios_base::failure("Failed to create file: " + out_filename + ". Make sure folder " + output_folder + " exists.");
    }
    string header = "";
    for(int f: dataset.primaryKey) {
        header += dataset.header[f] + "_PK\t";
    }
    for(int f: dataset.dependencies[-1]) {
        header += dataset.header[f] + "\t";
    }
    ofs << header << "\n";
    ifstream file(filename);
    string line;
    getline(file, line);  // skip header
    while (getline(file, line)) {
        vector<string> row;
        stringstream ss1(line);
        string cell;
        while (getline(ss1, cell, '\t')) {
            trim(cell);
            row.push_back(cell);
        }
        string out_line = "";
        for(int f: dataset.primaryKey) {
            out_line += row[f] + "\t";
        }
        for(int f: dataset.dependencies[-1]) {
            out_line += row[f] + "\t";
        }
        ofs << out_line << "\n";
    }
    ofs.close();
}

void create_table_from_dependency_to_pk(Dataset& dataset, string output_folder) {
    if(dataset.primaryKey.size() == 1) return;
    string out_filename = output_folder + "/main_table.tsv";
    ofstream ofs(out_filename);
    if (!ofs.is_open()) {
        throw ios_base::failure("Failed to create file: " + out_filename + ". Make sure folder " + output_folder + " exists.");
    }
    string header = "";
    for(int f: dataset.primaryKey) {
        header += dataset.header[f] + "_PK\t";
    }
    for(int f: dataset.dependencies[-1]) {
        header += dataset.header[f] + "\t";
    }
    ofs << header << "\n";
    for(vector<string> row: dataset.rows) {
        string out_line = "";
        for(int f: dataset.primaryKey) {
            out_line += row[f] + "\t";
        }
        for(int f: dataset.dependencies[-1]) {
            out_line += row[f] + "\t";
        }
        ofs << out_line << "\n";
    }
    ofs.close();
}

void create_table_from_dependency(Dataset& dataset, pair<int, vector<int>> dep, string filename, string output_folder) {
    string out_filename;
    bool dependency_to_pk = false;
    if(dataset.primaryKey.size() == 1 && dep.first == dataset.primaryKey[0]) {
        out_filename = output_folder + "/main_table.tsv";
        dependency_to_pk = true;
    } else {
        out_filename = output_folder + "/catalog_" + dataset.header[dep.first] + ".tsv";
    }
    ofstream ofs(out_filename);
    if (!ofs.is_open()) {
        throw ios_base::failure("Failed to create file: " + out_filename + ". Make sure folder " + output_folder + " exists.");
    }
    string header = dataset.header[dep.first] + "_PK\t";
    for(int f: dep.second) {
        header += dataset.header[f] + "\t";
    }
    ofs << header << "\n";
    set<string> keys;
    ifstream file(filename);
    string line;
    getline(file, line);  // skip header
    long long memory = 0;
    while (getline(file, line)) {
        vector<string> row;
        stringstream ss1(line);
        string cell;
        while (getline(ss1, cell, '\t')) {
            trim(cell);
            row.push_back(cell);
        }
        string key = row[dep.first];
        if(!dependency_to_pk) {
            if(keys.find(key) != keys.end()) continue;
            keys.insert(key);
            memory += key.capacity();
        }
        string line = key + "\t";
        for(int f: dep.second) {
            line += row[f] + "\t";
        }
        ofs << line << "\n";
    }
    dataset.extra_memory = MAX(memory, dataset.extra_memory);
    ofs.close();
}

void create_table_from_dependency(Dataset& dataset, pair<int, vector<int>> dep, string output_folder) {
    string out_filename;
    bool dependency_to_pk = false;
    if(dataset.primaryKey.size() == 1 && dep.first == dataset.primaryKey[0]) {
        out_filename = output_folder + "/main_table.tsv";
        dependency_to_pk = true;
    } else {
        out_filename = output_folder + "/catalog_" + dataset.header[dep.first] + ".tsv";
    }
    ofstream ofs(out_filename);
    if (!ofs.is_open()) {
        throw ios_base::failure("Failed to create file: " + out_filename + ". Make sure folder " + output_folder + " exists.");
    }
    string header = dataset.header[dep.first] + "_PK\t";
    for(int f: dep.second) {
        header += dataset.header[f] + "\t";
    }
    ofs << header << "\n";
    set<string> keys;
    long long memory = 0;
    for(vector<string> row: dataset.rows) {
        string key = row[dep.first];
        if(!dependency_to_pk) {
            if(keys.find(key) != keys.end()) continue;
            keys.insert(key);
            memory += key.capacity();
        }
        string line = key + "\t";
        for(int f: dep.second) {
            line += row[f] + "\t";
        }
        ofs << line << "\n";
    }
    dataset.extra_memory = MAX(memory, dataset.extra_memory);
    ofs.close();
}

void create_tables_from_dependencies(Dataset& dataset, string filename, string output_folder) {
    dataset.extra_memory = 0;
    for (auto& dep: dataset.dependencies) {
        if(dep.first < 0) {
            create_table_from_dependency_to_pk(dataset, filename, output_folder);
        } else {
            create_table_from_dependency(dataset, dep, filename, output_folder);
        }
    }
}

void create_tables_from_dependencies(Dataset& dataset, string output_folder) {
    dataset.extra_memory = 0;
    for (auto& dep: dataset.dependencies) {
        if(dep.first < 0) {
            create_table_from_dependency_to_pk(dataset, output_folder);
        } else {
            create_table_from_dependency(dataset, dep, output_folder);
        }
    }
}

void print(bool show_logs, Dataset& dataset) {
    if(!show_logs) return;
    cout << "done" << endl << "- Number of rows: " << dataset.rows.size();
    cout << (dataset.all_rows_in_dataset? ". All the rows have been loaded" : ". A subset of the rows have been loaded");
    cout << endl;
    string fields = "- Fields: ";
    for(int i = 0; i < dataset.header.size(); i ++) {
        fields += dataset.header[i];
        if(i < dataset.header.size() - 1) fields += ", ";
    }
    cout << fields << endl;
    
    string pk = "(";
    for(int i = 0; i < dataset.primaryKey.size(); i ++) {
        pk += dataset.header[dataset.primaryKey[i]];
        if(i < dataset.primaryKey.size() - 1) pk += ", ";
    }
    pk += ")";
    cout << "- Primary key: " << pk << endl;

    string mvf = "";
    for(int f: dataset.mv_fields) {
        mvf += dataset.header[f] + " ";
    }
    cout << "- Multi-valued fields: " << mvf << endl;

    string deps = "- Dependencies: ";
    for(auto& dep: dataset.dependencies) {
        deps += (dep.first >= 0? dataset.header[dep.first]: pk) + "->[";
        for(int i = 0; i < dep.second.size(); i ++) {
            deps += dataset.header[dep.second[i]];
            if(i < dep.second.size() - 1) deps += ", ";
        }
        deps += "] ";
    }
    cout << deps << endl;
}

void normalize(string filename, double rows_pct, string output_folder, bool show_logs) {
    auto begin = chrono::steady_clock::now();
    if (show_logs) cout << "Loading file " << filename << " ...";
    Dataset dataset = read_tsv(filename, rows_pct);
    if (show_logs) cout << "done" << endl << "Running normalization steps ...";
    auto begin_norm = chrono::steady_clock::now();
    find_multivalued_fields(dataset);
    find_functional_dependencies(dataset);
    find_primary_key(dataset);
    find_dependencies_to_pk(dataset);
    delete_transitive_dependencies(dataset);
    print(show_logs, dataset);
    auto end_norm = chrono::steady_clock::now();
    print_time(show_logs, "Normalization duration = ", begin_norm, end_norm);
    if (show_logs) cout << "Creating tables from multi-valued fields ... " << endl;
    create_tables_from_mvfields(dataset, filename, output_folder);
    if (show_logs) cout << "done" << endl << "Creating tables from dependencies ... " << endl;
    if (dataset.all_rows_in_dataset) create_tables_from_dependencies(dataset, output_folder);
    else create_tables_from_dependencies(dataset, filename, output_folder);
    auto end = chrono::steady_clock::now();
    print_time(show_logs, "done\nGlobal duration = ", begin, end);
    print_memory(show_logs, dataset);
}
