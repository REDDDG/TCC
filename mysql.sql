create table global_transaction
(
    xid          varchar(128)       not null
        primary key,
    status       tinyint default 0  not null comment '0:Trying,1:Confirming,2:Cancelling,3:Completed,4:Failed',
    service_name varchar(255)       not null,
    create_time  datetime           not null,
    update_time  datetime           not null,
    timeout      int     default 30 not null comment '超时秒数',
    retry_count  int     default 0  not null,
    content      json               not null comment '参与者信息JSON数组'
);

create table branch_transaction
(
    branch_id   bigint auto_increment
        primary key,
    xid         varchar(128)      not null,
    resource_id varchar(255)      null,
    status      tinyint default 0 not null comment '0:Init,1:TryDone,2:ConfirmDone,3:CancelDone',
    try_data    text              null,
    create_time datetime          not null,
    update_time datetime          not null,
    constraint fk_branch_global
        foreign key (xid) references global_transaction (xid)
            on delete cascade
);

create index idx_xid
    on branch_transaction (xid);

create index idx_status_update_time
    on global_transaction (status, update_time);

create table inventory_stock
(
    id         bigint auto_increment
        primary key,
    product_id varchar(64)                        not null comment '商品编号',
    total      bigint   default 0                 not null comment '总库存',
    version    int      default 0                 not null comment '版本号，乐观锁用',
    created_at datetime default CURRENT_TIMESTAMP not null,
    updated_at datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint uk_product_id
        unique (product_id)
)
    comment '商品库存表（备份）';

create table order_main
(
    id         bigint auto_increment
        primary key,
    user_id    varchar(64)                        not null comment '用户ID',
    product_id varchar(64)                        not null comment '商品ID',
    quantity   int                                not null comment '数量',
    amount     decimal(10, 2)                     not null comment '金额',
    status     tinyint  default 0                 not null comment '0=初始化 1=已确认 2=已取消',
    created_at datetime default CURRENT_TIMESTAMP not null,
    updated_at datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    timeout    int                                null
)
    comment '订单表（备份）';

create index idx_status
    on order_main (status);

create index idx_user_id
    on order_main (user_id);

create table points_account
(
    id         bigint auto_increment
        primary key,
    user_id    varchar(64)                        not null comment '用户ID',
    balance    bigint   default 0                 not null comment '当前总积分',
    version    int      default 0                 not null comment '版本号，乐观锁用',
    created_at datetime default CURRENT_TIMESTAMP not null,
    updated_at datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint uk_user_id
        unique (user_id)
)
    comment '用户积分账户表（备份）';

