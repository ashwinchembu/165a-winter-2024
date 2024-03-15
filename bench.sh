if [ "$1" -eq "1" ]
then
    make clean
    make optimized
fi

make a
python3 concurrent_bench.py
make a
python3 concurrent_bench.py
make a
python3 concurrent_bench.py
make a
python3 concurrent_bench.py
make a
python3 concurrent_bench.py
