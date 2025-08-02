#ifndef COMMON_H_
#define COMMON_H_

#include <map>
#include <set>
#include <string>
#include <vector>

using namespace std;

typedef struct {
   vector<string> header;
   vector<vector<string>> rows; 
   vector<int> primaryKey;
   set<int> mv_fields;
   map<int, vector<int>> dependencies;
   map<int, vector<int>> bi_dependencies;
   long long extra_memory;
   bool all_rows_in_dataset;
} Dataset;


#endif /* COMMON_H_ */
