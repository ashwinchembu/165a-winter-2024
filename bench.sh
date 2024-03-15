if [ "$2" -eq "1" ]
then
    make clean
    make optimized
fi

make a
python3 concurrent_bench.py $1
make a
python3 concurrent_bench.py $1
make a
python3 concurrent_bench.py $1
make a
python3 concurrent_bench.py $1
make a
python3 concurrent_bench.py $1
make a
