CXX = g++
CXXFLAGS = -std=c++17 -O2
TARGET = run_experience

SRCS = experience.cpp db.cpp MemTable.cpp SSTable.cpp operation.cpp
OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f *.o $(TARGET)
