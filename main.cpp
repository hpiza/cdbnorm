#include "normalizer.h"
#include "utils.h"
#include <iostream>

using namespace std;

int main() {
	string small_datasets[] = {"beer", "client_rental", "invoice", "report", "staff_property", "teachers", "wellmeadows"};
	string large_datasets[] = {"name.basics", "title.akas", "title.basics", "title.crew", "title.episode", "title.ratings", "title.principals"};
	for(string ds: small_datasets) {
		normalize("datasets/" + ds + ".tsv", 1.0, "datasets-norm", true);
	}
	return 0;
}
