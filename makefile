CXX = g++
CXXFLAGS = -std=c++20 -O3 -Wall -Wextra -pthread
BOOST_ROOT = /root/boost_1_86_0
BOOST_INCLUDE = -I$(BOOST_ROOT)
BOOST_LIB = -L$(BOOST_ROOT)/stage/lib
LDFLAGS = $(BOOST_LIB) -lboost_iostreams -lboost_system -lcurl -pthread -Wl,-rpath,$(BOOST_ROOT)/stage/lib

catl_extract: ctxr.cpp
	$(CXX) $(CXXFLAGS) $(BOOST_INCLUDE) $< -o $@ $(LDFLAGS)

clean:
	rm -f catl_extract

.PHONY: clean
