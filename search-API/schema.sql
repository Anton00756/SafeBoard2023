drop table if exists SearchRequest;
drop table if exists PathToFile;

create table SearchRequest (
    data_index integer primary key autoincrement,
    search_id varchar(40) not null,
    status bool DEFAULT(FALSE)
);

create table PathToFile (
    data_index integer primary key autoincrement,
    parent_index int not null,
    path text not null,
    FOREIGN KEY(parent_index) REFERENCES SearchRequest(data_index)
);