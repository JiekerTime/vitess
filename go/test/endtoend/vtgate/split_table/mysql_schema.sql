CREATE TABLE `t_user`
(
    `id`         bigint,
    `f_key`      char(32) NOT NULL DEFAULT '',
    `col`        char(32) NOT NULL DEFAULT '',
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `name`       varchar(128)      DEFAULT NULL,
    `predef1`    varchar(128)      DEFAULT NULL,
    `predef2`    varchar(128)      DEFAULT NULL,
    `textcol1`   varchar(128)      DEFAULT NULL,
    `intcol`     int(16)           DEFAULT NULL,
    `textcol2`   varchar(128)      DEFAULT NULL,
    `someColumn` varchar(128)      DEFAULT NULL,
    `col1`       int(16)           DEFAULT NULL,
    `col2`       int(16)           DEFAULT NULL,
    `a`          int(16)           DEFAULT NULL,
    `b`          int(16)           DEFAULT NULL,
    `c`          int(16)           DEFAULT NULL,
    `foo`        int(16)           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;