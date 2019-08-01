machsql -s localhost -u SYS -p manager -P 5656 -f crt_ex.sql
date
machloader -i -f flowfmt.fmt -d data_ex.csv -H -F "starttime YYYY/MM/DD HH24:MI:SS.mmmuuu"
date
