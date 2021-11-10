CC = mpicc
CXX = mpic++  
# CC = gcc
# CXX = g++  
CFLAGS = -Wall -Wextra -Wno-unused-result -Wno-unused-parameter -Wno-unused-but-set-variable -O3 -g
CXXFLAGS = $(CFLAGS) -std=c++11

all:	mts

mts: mts.o mmio.o matrix.o
	$(CXX) $(CFLAGS) -o $@ $^


mmio.o:	mmio.c
	$(CC) $(CFLAGS) -c mmio.c

%.o:	%.cc
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f *o
	rm -f mts
