#include <limits.h>
#include <stdio.h>
#include <math.h>

// Simple program to print out maximum int and unsigned long long.
// Compile with: gcc showsize.c -o showsize

int main(int arg, char** argv)
{
    printf("Hi\n");
    printf("Max int = %d\n", INT_MAX);
    printf("Max unsigned long long = %llu\n", ULLONG_MAX);
    return 0;
}
