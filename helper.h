#pragma once
#include<iostream>
#include <cstdint>
#include <cstring>
#include<string>
using namespace std;


// Helper.h should contain the structs, functions, constants, etc. that are used more than one time in the project. For example, the structs that are used in both server and client, the constants that are used in all three files. 

struct FuncEntry
{
    string funcName;
    int64_t byteOffsetInResolveBin; // where this function's FUNC header record sits
};
struct PendingPatch
{
    int64_t byteOffsetOfOffsetField; // where in resolve.bin to seek back and overwrite
    string targetFuncName;
};      

inline uint32_t checksumUpdate(uint32_t sum, const char *data, size_t n)
{
    for (size_t i = 0; i < n; i++)
        sum += (unsigned char)data[i];   
    return sum;
}
inline void encodeU32(uint32_t v, unsigned char out[4])
{
    out[0] = v & 0xFF;
    out[1] = (v >> 8) & 0xFF;
    out[2] = (v >> 16) & 0xFF;
    out[3] = (v >> 24) & 0xFF;
}

inline uint32_t decodeU32(const unsigned char in[4])
{
    return (uint32_t)in[0] | ((uint32_t)in[1] << 8) | ((uint32_t)in[2] << 16) | ((uint32_t)in[3] << 24);
}

class TrailerChecker
{
    unsigned char held[4];   
    size_t heldLen;
    uint32_t sum;            

public:
    TrailerChecker() : heldLen(0), sum(0) { memset(held, 0, sizeof(held)); }

    size_t feed(const char *data, size_t n, char *out)
    {
        size_t total = heldLen + n;
        if (total <= 4)                    
        {
            memcpy(held + heldLen, data, n);
            heldLen = total;
            return 0;
        }
        size_t release = total - 4;        
        size_t fromHeld = (heldLen < release) ? heldLen : release;
        size_t fromData = release - fromHeld;

        memcpy(out, held, fromHeld);
        if (fromData > 0)
            memcpy(out + fromHeld, data, fromData);

       
        unsigned char nh[4];
        size_t k = 0;
        for (size_t i = fromHeld; i < heldLen; i++)
            nh[k++] = held[i];
        for (size_t i = fromData; i < n; i++)
            nh[k++] = (unsigned char)data[i];
        memcpy(held, nh, 4);
        heldLen = 4;

        sum = checksumUpdate(sum, out, release);
        return release;
    }

    
    bool verify() const
    {
        return heldLen == 4 && decodeU32(held) == sum;
    }
    const unsigned char *trailer() const { return held; }  
};