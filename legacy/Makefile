SOURCES=main.cpp cdindex.cpp
OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=bin/cdindex

ifdef DEBUG
	CFLAGS=-g -O0
	CXXFLAGS=-g -O0
else
	CFLAGS=-O3
	CXXFLAGS=-O3
endif

all: $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	mkdir -p bin
	$(CXX) $(LDFLAGS) $(OBJECTS) -o $@

.PHONY: clean test

clean:
	rm -f *.o $(EXECUTABLE)

# Regression test
test:
	bin/cdindex | diff bin.ok -
	python tests.py | diff py.ok -

