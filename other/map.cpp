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

	//cout << Map.size() << endl;


	return 0;
}