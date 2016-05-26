drop table if exists material_view;

/*==============================================================*/
/* Table: material_view                                         */
/*==============================================================*/
create table material_view
(
   viewName             varchar(50) not null,
   build                varchar(50),
   refresh              varchar(50),
   rewrite              bit,
   sqlStr               text,
   factTable            varchar(256),
   sqlDataStruct       blob,
   store        bit,
   createSQL        text,
   primary key (viewName),
   KEY `material_view_factTable_index` (`factTable`)
);