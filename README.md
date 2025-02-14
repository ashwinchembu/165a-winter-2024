# -165a-winter-2024.

# TODO
- [x]Page.cpp: Replace availability array with number of slots being used.
- [x]General: Change most of parameters to const reference.
- [x]General: Config
- [ ]Compiler: Find the optimal flags.
- [ ]Compiler: Look into buildin functions and attributes. e.g. (alloc_size, hot, __builtin_prefetch)
- [ ]General: Use multi threaded algorithm if possible. https://gcc.gnu.org/onlinedocs/libstdc++/manual/parallel_mode_using.html
- [ ]General: Delete redundant variable and function calls.
- [ ]General: Make documentation up-to-date.
- [ ]General: Create exceptions on unexpected parameters or return value.
- [ ]General: Windows support
- [ ]General: Use Array over Vector

For milestone 3, we can use standard thread library.

# Makefile usage
## No compiler optimization
Use ```make```

## Compiler optimization
Use ```make optimized```

## Compiler optimization and profile guided optimizations
Use ```make profiling``` then run a python file. This run will be slower because gcc gathers information about our program. Use one that is a good representation of general use of the database.
Then use ```make _profiled```
