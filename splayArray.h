#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <cstring>
#include <string>
constexpr int DEFAULT_LENGTH = 20 ; 
// this class can save some data while maintain the size automatically . 
class splayArray
{
private:
    /* data */
    char * data ; 
    int maxSize ; 
    uint64_t n ;
    void splay() ; 
public:
    splayArray(/* args */);
    ~splayArray();
    // return the data ; 
    char * access() ; 
    void push(uint64_t num) ; 
    void push(uint32_t num) ; 
    void push(uint16_t num) ; 
    void push(char num) ; 
    uint64_t size() ; 
    char access(int index) ; 
    // add a string at the tail . 
    void append(const std::string & str) ; 
    // reset to the default state
    void reset () ; 
    void PRINT_STATUS() ; 

};
