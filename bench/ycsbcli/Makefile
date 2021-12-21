LEVELDB_SRC=../leveldb

CC=g++
CFLAGS=-O3 -I${LEVELDB_SRC}/include
CFLAGS_EXTRA=-Wall -Werror
CSTATIC=${LEVELDB_SRC}/build/libleveldb.a
CDYLIBS=-lpthread

TARGET=ycsbcli


.PHONY: all
all: $(TARGET)


$(TARGET): $(TARGET).cpp cxxopts.hpp
	$(CC) $(CFLAGS) $(CFLAGS_EXTRA) -o $(TARGET) $^ $(CSTATIC) $(CDYLIBS)


.PHONY: clean
clean:
	rm -f $(TARGET)

