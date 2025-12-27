#include "third_party/inlineskiplist.h"
#include <iostream>
#include <vector>
#include <thread>

using Key = uint64_t;
using Value = uint64_t;

static Key Decode(const char* key){
    Key rv;
    memcpy(&rv, key, sizeof(Key));
    return rv;
}

struct Comparator{
    typedef Key DecodedType;

    static DecodedType decode_key(const char* b) { return Decode(b); }

    int operator()(const char* a, const char* b) const{
	if(Decode(a) < Decode(b))
	    return -1;
	else if(Decode(a) > Decode(b))
	    return +1;
	else
	    return 0;
    }

    int operator()(const char* a, const DecodedType b) const{
	if(Decode(a) < b)
	    return -1;
	else if(Decode(a) > b)
	    return +1;
	else
	    return 0;
    }

};

using SkipList = InlineSkipList<Comparator>;
Key* keys = nullptr;
SkipList* list = nullptr;

void load(int numData, int numThreads){
    auto func = [numData, numThreads](int tid){
	size_t chunk = numData / numThreads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	if(to > numData) to = numData;
	for(int i=from; i<to; i++){
	    auto buf = list->AllocateKey(sizeof(Key));
	    *(Key*)buf = keys[i];
	    bool ret = list->InsertConcurrently(buf);
	    (void) ret;
	}
    };

    std::vector<std::thread> threads;
    struct timespec start, end;
    std::cout << "Insertion starts" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numThreads; i++)
        threads.push_back(std::thread(func, i));
    for(auto& t: threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000000000.0 << " sec" << std::endl;
    std::cout << "throughput: " << numData / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;
}

void find(int numData, int numThreads){
    std::vector<Key> notFound[numThreads];
    auto func = [numData, numThreads, &notFound](int tid){
        size_t chunk = numData / numThreads;
        int from = chunk * tid; 
        int to = chunk * (tid + 1);
        if(to > numData) to = numData;

        for(int i=from; i<to; i++){
	    SkipList::Iterator iter(list);
	    iter.Seek((char*)&keys[i]);
	    if(iter.Valid()){
		Key k = *(Key*)iter.key();
		if(k != keys[i])
		    notFound[tid].push_back(keys[i]);
	    }
	    else{
		std::cout << "Invalid key " << keys[i] << std::endl;
		notFound[tid].push_back(keys[i]);
	    }
	}
    };
    
    std::vector<std::thread> threads;
    struct timespec start, end;
    std::cout << "Search starts" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numThreads; i++)
        threads.push_back(std::thread(func, i));
    for(auto& t: threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);

    int notfound = 0;
    for(int i=0; i<numThreads; i++)
	notfound += notFound[i].size();

    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000000000.0 << " sec" << std::endl;
    std::cout << "throughput: " << numData / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;
    if(notfound)
	std::cout << "[" << notfound << " Keys are NOT FOUND]" << std::endl;
}   

int main(int argc, char* argv[]){
    if(argc < 3){
	std::cerr << "Usage: " << argv[0] << " numData numThreads" << std::endl;
	exit(0);
    }

    int numData = atoi(argv[1]);
    int numThreads = atoi(argv[2]);

    keys = new Key[numData];
    for(int i=0; i<numData; i++)
	keys[i] = i+1;
    std::random_shuffle(keys, keys+numData);

    Allocator alloc;
    Comparator cmp;
    list = new SkipList(cmp, &alloc, 21);

    load(numData, numThreads);
    find(numData, numThreads);

    delete keys;
    delete list;
    return 0;
}
