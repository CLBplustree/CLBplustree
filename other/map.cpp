#include <iostream>
#include <string>
#include <map>

using namespace std;

typedef int record_type;

int main(int argc, char const *argv[])
{
	char instr;
	int key, ukey;

	map<int, record_type> Map;
	map<int, record_type>::iterator it, uit;

	// test data from clbptbench
#include <cstdio>
#include <cstdlib>
#include <time.h>
#define CLBPT_STOP_TYPE 0
#define CLBPT_INSERT_TYPE 1
#define CLBPT_SEARCH_TYPE 2
#define CLBPT_DELETE_TYPE 3
#define CLBPT_RANGE_TYPE 4
#define CL_SUCCESS 0

	// read test data
	FILE *input_data ;
	input_data = fopen("input", "r");
	if (input_data == NULL) {
		cout << "Can't open file input" << endl;
		return 0;
	}

	int bfsize = 128;
	int *data_buffer = (int *)calloc(128, sizeof(int));
	int *rec_buffer = (int *)calloc(128, sizeof(int));

	time_t start_time;
	time_t end_time;

	while(1) {
		int i;
		int err = CL_SUCCESS;
		int data_info[2] = {0,0};
		fscanf(input_data, "%d %d", &data_info[0], &data_info[1]);
		//fread(data_info, 2, sizeof(int), input_data);
		printf("Info %d %d\n", data_info[0], data_info[1]);
		getchar();
		if (!data_info[0]) break;
		if (data_info[1] > bfsize) {
			bfsize = data_info[1];
			data_buffer = (int *)realloc(data_buffer, bfsize * sizeof(int));
			rec_buffer = (int *)realloc(rec_buffer, bfsize * sizeof(int));
		}

		switch (data_info[0]) {
		case  CLBPT_SEARCH_TYPE:
			//fread(data_buffer, data_info[1], sizeof(int), input_data);
			for(i = 0; i < data_info[1]; i++) {
				fscanf(input_data, "%d", &data_buffer[i]);
				//printf("%d key : %d.\n", i, data_buffer[i]);
			}
			time(&start_time);
			for(i = 0; i < data_info[1]; i++) {
				int key = data_buffer[i];
				int rec = 0;
				it = Map.find(key);
				if (it != Map.end()) {
					cout << "FOUND: key " << key << endl;
					rec_buffer[i] = it->second;
				}
				else {
					cout << "NOT FOUND: key " << key << endl;
				}
			}
			break;
		case  CLBPT_INSERT_TYPE:
			//fread(data_buffer, data_info[1], sizeof(int), input_data);
			//fread(rec_buffer, data_info[1], sizeof(int), input_data);
			for(i = 0; i < data_info[1]; i++) {
				fscanf(input_data, "%d", &data_buffer[i]);
				fscanf(input_data, "%d", &rec_buffer[i]);
				//printf("%d key : %d.\n", i, data_buffer[i]);
			}
			time(&start_time);
			for(i = 0; i < data_info[1]; i++) {
				int key = data_buffer[i];
				int rec = rec_buffer[i];
				cout << "Insert <key,value>: " << "<" << key << "," << rec << ">" << endl;
				//Map[key] = rec;
				Map.insert(map<int, record_type>::value_type(key, rec));
			}
			break;
		case  CLBPT_DELETE_TYPE:
			//fread(data_buffer, data_info[1], sizeof(int), input_data);
			for(i = 0; i < data_info[1]; i++) {
				fscanf(input_data, "%d", &data_buffer[i]);
				//printf("%d key : %d.\n", i, data_buffer[i]);
			}
			time(&start_time);
			for(i = 0; i < data_info[1]; i++) {
				int key = data_buffer[i];
				it = Map.find(key);
				if (it != Map.end()) {
					cout << "Delete key: " << key << endl;
					Map.erase(it);
				}
				else {
					cout << "NOT EXISTED: key " << key << endl;
				}
			}
			break;
		case  CLBPT_RANGE_TYPE:
			//cin >> key >> ukey;
			//while((it = Map.find(key)) == Map.end() && key <= ukey) key++;
			//uit = Map.find(ukey);
			//for(; it->first <= ukey && it != Map.end(); it++) {
			//	cout << "key " << it->first << " => value " << it->second << endl;
			//}
			break;
		default: exit(-1);
		}
		if (err != CL_SUCCESS)
		{
			fprintf(stderr, "EnqueInsertions ERROR\n");
			exit(err);
		}
		if (data_info[0] == CLBPT_SEARCH_TYPE)
			for (i = 0; i < data_info[1]; i++)
				printf("key(%d) : %d\n", data_buffer[i], rec_buffer[i]);
		time(&end_time);
		int delay = difftime(end_time, start_time);
		printf("Map Cost time : %d seconds\n", delay);
	}
/*
	while(cin >> instr) {
		switch(instr) {
			case 'f':
				cin >> key;
				it = Map.find(key);
				if (it != Map.end()) {
					cout << "FOUND: key " << key << endl;
				}
				else {
					cout << "NOT FOUND: key " << key << endl;
				}
				break;
			case 'r':
				cin >> key >> ukey;
				while((it = Map.find(key)) == Map.end() && key <= ukey) key++;
				uit = Map.find(ukey);
				for(; it->first <= ukey && it != Map.end(); it++) {
					cout << "key " << it->first << " => value " << it->second << endl;
				}
				break;
			case 'i':
				cin >> key;
				cout << "Insert key: " << key << endl;
				//Map[key] = key;
				Map.insert(map<int, record_type>::value_type(key, key));
				break;
			case 'd':
				cin >> key;
				it = Map.find(key);
				if (it != Map.end()) {
					cout << "Delete key: " << key << endl;
					Map.erase(it);
				}
				else {
					cout << "NOT EXISTED: key " << key << endl;
				}
				break;
			default:
				break;
		}
	}
*/
	//cout << Map.size() << endl;


	return 0;
}