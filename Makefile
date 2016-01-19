CC=gcc
CFLAGS=-I ${AMDAPPSDKROOT}/include -I src/ -I src/KMA/ -O3 -std=c11 -march=native 
LDFLAGS=-L ${AMDAPPSDKROOT}/lib/x86_64 -lOpenCL -lpthread
DEPS=src/clbpt_type.h src/clbpt.h src/clbpt_core.h src/KMA/kma.h # src/KMA/clheap.h src/KMA/clIndexedQueue.h 
OBJS=obj/main.o obj/clbpt.o obj/clbpt_core.o obj/kma.o
CPS=bin/clbpt.cl bin/kma.cl bin/kma.h #bin/clIndexedQueue.h bin/clheap.h bin/clIndexedQueue.cl 

all: bin

obj/kma.o: src/KMA/kma.c ${DEPS} 
	${CC} ${CFLAGS} -c -o $@ $<

obj/%.o: src/%.c ${DEPS} 
	${CC} ${CFLAGS} -c -o $@ $<
 
bin/clbpt: ${OBJS} 
	${CC} ${CFLAGS} -o $@ $^ ${LDFLAGS}

bin/%.cl: src/%.cl 
	cp $^ $@

bin/%.cl: src/KMA/%.cl
	cp $^ $@

bin/%.h: src/KMA/%.h
	cp $^ $@

bin: bin/clbpt ${CPS} 

bin/input:
	cp CLBPTbench/simple bin/input

run: bin bin/input
	cd bin; ./clbpt

clean:
	rm -f obj/*
	rm -f bin/* 

