all:
	clang++ --std=c++11 ../CLBPTbench/clbptbench.cpp -o ../CLBPTbench/bench
	clang bpt.c -o bpt
	clang++ map.cpp -o map
run:
	#../CLBPTbench/bench -i 10 -m 0 -M 10 -o ../other/input
	#./bpt input
	./map input
clean:
	rm ../CLBPTbench/bench
	rm bpt
	rm map
	rm input
