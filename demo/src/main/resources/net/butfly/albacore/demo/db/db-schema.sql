create table users (
    id char(24) not null,
    name varchar(80) not null,
    login_name varchar(80) not null,
    password varchar(80) not null,
,constraint pk_user primary key (id)
);
