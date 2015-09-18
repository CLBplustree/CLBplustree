CC=gcc
CFLAGS=-I ${AMDAPPSDKROOT}/include -I src/ -I src/KMA/ -g -std=c11 
LDFLAGS=-L ${AMDAPPSDKROOT}/lib/x86_64 -lOpenCL -lpthread
DEPS=src/clbpt_type.h src/clbpt.h src/clbpt_core.h src/clbpt.cl src/KMA/kma.cl src/KMA/kma.h src/KMA/clheap.h src/KMA/clIndexedQueue.h src/KMA/clIndexedQueue.cl
OBJS=obj/main.o obj/clbpt.o obj/clbpt_core.o obj/kma.o

all: clbpt

obj/kma.o: src/KMA/kma.c ${DEPS}
	${CC} ${CFLAGS} -c -o $@ $< 

obj/%.o: src/%.c ${DEPS}
	${CC} ${CFLAGS} -c -o $@ $<
 
clbpt: ${OBJS}
	${CC} ${CFLAGS} -o $@ $^ ${LDFLAGS}

run:
	./clbpt

clean:
	rm -f obj/*
	rm -f clbpt

