CC=gcc
CFLAGS=-I ${AMDAPPSDKROOT}/include -I src/ -I src/KMA/ -g -std=c11 
LDFLAGS=-L ${AMDAPPSDKROOT}/lib/x86_64 -lOpenCL -lpthread
DEPS=src/clbpt_type.h src/clbpt.h src/clbpt_core.h src/KMA/kma.h src/KMA/clheap.h src/KMA/clIndexedQueue.h 
OBJS=obj/main.o obj/clbpt.o obj/clbpt_core.o obj/kma.o
CPS=bin/clbpt.cl bin/kma.cl bin/clIndexedQueue.cl bin/kma.h bin/clIndexedQueue.h bin/clheap.h

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

run: bin
	cp CLBPTbench/simple bin/input
	cd bin; ./clbpt

clean:
	rm -f obj/*
	rm -f bin/* 

