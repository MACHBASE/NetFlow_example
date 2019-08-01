drop table netflow_data;
create table netflow_data 
(
	StartTime datetime property (MINMAX_CACHE_SIZE=2097152),
	Dur double,
    Protocol varchar(7),
	SAddr ipv4,
	Sport integer,
	DAddr ipv4,
	Dport integer,
	State Varchar(15),
	sTos int,
	dTos int,
	TotPkts int,
	TotBytes int,
	SrcBytes int
);

create bitmap index idx_src_port2 on netflow_data(Sport);
create bitmap index idx_dst_port2 on netflow_data(Dport);
create index idx_src_ip2 on netflow_data(SAddr); 
create index idx_dst_ip2 on netflow_data(DAddr);
