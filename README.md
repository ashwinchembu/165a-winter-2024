# -165a-winter-2024.

# TODO
- [x]Page.cpp: Replace availability array with number of slots being used.
- [x]General: Change most of parameters to const reference.
- [ ]Compiler: Find the optimal flags.
- [ ]Compiler: Look into buildin functions and attributes. e.g. (alloc_size, hot, __builtin_prefetch)
- [ ]General: Delete redundant variable and function calls.
- [ ]General: Make documentation up-to-date.
- [ ]General: Create exceptions on unexpected parameters or return value.
- [ ]Index: Explore alternative for unordered multimap
- [ ]General: Config
- [ ]General: Windows support

# Makefile usage
## No compiler optimization
Use ```make```

## Compiler optimization
Use ```make optimized```

## Compiler optimization and profile guided optimizations
Use ```make profiling```
Run a python file. This run will be slower because gcc gathers information about our program.
Then use ```make _profiled```
