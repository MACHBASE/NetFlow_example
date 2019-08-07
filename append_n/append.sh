make clean
make

machsql -s localhost -u SYS -p manager -P 5656 -f crt.sql

./append 100000 1000
