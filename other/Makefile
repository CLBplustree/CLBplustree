all:
	g++ --std=c++11 ../CLBPTbench/clbptbench.cpp -o ../CLBPTbench/bench
	gcc bpt.c -o bpt
	g++ -O3 -march=native map.cpp -o map
run:
	#../CLBPTbench/bench -i 10 -m 0 -M 10 -o ../other/input
	#./bpt input
	./map input
clean:
	rm ../CLBPTbench/bench
	rm bpt
	rm map
	rm input
