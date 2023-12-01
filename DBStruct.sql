create table artists_balance
(
    id        int auto_increment
        primary key,
    artist_id bigint null,
    balance   int    null
);

create table s3_files
(
    id               int auto_increment
        primary key,
    name             varchar(255)  null,
    last_modified_at datetime      null,
    is_processed     int default 0 null
);

