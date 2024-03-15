if [ "$1" -eq "1" ]
then
    make clean
    make optimized
fi

make a
python3 concurrent_bench.py 1
make a
python3 concurrent_bench.py 2
make a
python3 concurrent_bench.py 4
make a
python3 concurrent_bench.py 8
make a
python3 concurrent_bench.py 12
make a
python3 concurrent_bench.py 16
make a
python3 concurrent_bench.py 20
make a
python3 concurrent_bench.py 24
make a
python3 concurrent_bench.py 28
make a
python3 concurrent_bench.py 32
make a
python3 concurrent_bench.py 36
make a
python3 concurrent_bench.py 40
make a
python3 concurrent_bench.py 44
make a
python3 concurrent_bench.py 46
make a
python3 concurrent_bench.py 50
