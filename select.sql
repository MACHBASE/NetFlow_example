
============================
1.
select count(*) from netflow_data;

============================
2.
select min(starttime),max(starttime) from netflow_data;

============================
3.

SELECT saddr, daddr, dport, COUNT(dport)
FROM netflow_data
WHERE starttime between to_date('2018-09-08 15:30:00') AND to_date('2018-09-08 15:40:00')
GROUP BY saddr, daddr, dport
ORDER BY 4 desc limit 20;


============================
4.

** FULL SCAN : SUM(BYTES)

SELECT saddr, daddr, dport, SUM(totbytes)
FROM netflow_data
WHERE starttime between to_date('2018-09-08 15:30:00') AND to_date('2018-09-08 15:40:00')
GROUP BY saddr, daddr, dport
ORDER BY 4 desc limit 20;


=============================================================================
5.
    
** SUBNET

SELECT saddr, daddr, dport, SUM(srcbytes)
FROM netflow_data
WHERE saddr contained '147.32.86.0/24' AND daddr not contained '147.32.86.0/24' AND
starttime between to_date('2019-03-11') AND to_date('2019-03-12')
GROUP BY saddr, daddr, dport
ORDER BY 3 desc
LIMIT 10;


=============================================================================
6.
** IP-address based query

SELECT saddr, daddr, dport, SUM(totbytes)
FROM netflow_data
WHERE starttime between to_date('2018-09-08') AND to_date('2018-09-09') and saddr = '147.32.86.194'
GROUP BY saddr, daddr, dport
ORDER BY 4 desc limit 20;

