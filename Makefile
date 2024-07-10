
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall

all: correctness persistence mytest

correctness: kvstore.o correctness.o

persistence: kvstore.o persistence.o

mytest: kvstore.o mytest.o

clean:
	-rm -f correctness persistence mytest *.o
