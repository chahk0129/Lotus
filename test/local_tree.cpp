#include "index/twosided/btreeolc/tree.h"
#include <iostream>
#include <vector>
#include <thread>

using Key = uint64_t;
using Value = uint64_t;
BTree<Value>* tree = nullptr;
Key* keys = nullptr;

void load(int numData, int numThreads){
    auto func = [numData, numThreads](int tid){
        size_t chunk = numData / numThreads;
        int from = chunk * tid;
        int to = chunk * (tid + 1);
        if(to > numData) to = numData;
        for(int i=from; i<to; i++)
            tree->insert(keys[i], (Value)keys[i]);
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

void update(int numData, int numThreads){
    std::vector<Key> notFound[numThreads];
    auto func = [numData, numThreads, &notFound](int tid){
        size_t chunk = numData / numThreads;
        int from = chunk * tid; 
        int to = chunk * (tid + 1);
        if(to > numData) to = numData;
        for(int i=from; i<to; i++){
            Value v;
            bool ret = tree->update(keys[i], (Value)keys[i]);
            if(!ret) notFound[tid].push_back(keys[i]);
	    }
    };
    
    std::vector<std::thread> threads;
    struct timespec start, end;
    std::cout << "Update starts" << std::endl;
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


void find(int numData, int numThreads){
    std::vector<Key> notFound[numThreads];
    auto func = [numData, numThreads, &notFound](int tid){
        size_t chunk = numData / numThreads;
        int from = chunk * tid; 
        int to = chunk * (tid + 1);
        if(to > numData) to = numData;
        for(int i=from; i<to; i++){
            Value v;
            bool ret = tree->lookup(keys[i], v);
            if(!ret || v != (Value)keys[i])
		        notFound[tid].push_back(keys[i]);
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

void remove(int numData, int numThreads){
    std::vector<Key> notFound[numThreads];
    auto func = [numData, numThreads, &notFound](int tid){
        size_t chunk = numData / numThreads;
        int from = chunk * tid;
        int to = chunk * (tid + 1);
        if(to > numData) to = numData;
        for(int i=from; i<to; i++){
            Value v; 
            bool ret = tree->remove(keys[i]);
            if(!ret)
                notFound[tid].push_back(keys[i]);
        }   
    };

    std::vector<std::thread> threads;
    struct timespec start, end;
    std::cout << "Remove starts" << std::endl;
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
        keys[i] = i + 1;
    std::random_shuffle(keys, keys+numData);

    tree = new BTree<Value>();

    load(numData, numThreads);
    find(numData, numThreads);
    //update(numData, numThreads);
    //remove(numData, numThreads);
    //find(numData, numThreads);

    delete keys;
    delete tree;
    return 0;
}
