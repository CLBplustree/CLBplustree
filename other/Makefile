all:
	clang++ --std=c++11 clbptbench.cpp -o bench
	clang bpt.c -o bpt
	clang++ map.cpp -o map
run:
	./bench -i 10 -m 0 -M 10 -o in_insert > in.txt
	./bench -s 10 -m 0 -M 10 -o in_search >> in.txt
	./bpt < in.txt
	./map < in.txt
clean:
	rm bench bpt map in_insert in_search in.txt
