CREATE TABLE `t_user_1`
(
    `id`        bigint,
    `f_key`     char(32) NOT NULL DEFAULT '',
    `col`       char(32) NOT NULL DEFAULT '',
    `f_tinyint` tinyint           DEFAULT NULL,
    `f_bit`     bit(1)            DEFAULT NULL,
    `f_midint`  mediumint         DEFAULT NULL,
    `f_int`     int               DEFAULT NULL,
    primary key (id)
) ENGINE=InnoDB;

CREATE TABLE `t_user_2`
(
    `id`        bigint,
    `f_key`     char(32) NOT NULL DEFAULT '',
    `col`       char(32) NOT NULL DEFAULT '',
    `f_tinyint` tinyint           DEFAULT NULL,
    `f_bit`     bit(1)            DEFAULT NULL,
    `f_midint`  mediumint         DEFAULT NULL,
    `f_int`     int               DEFAULT NULL,
    primary key (id)
) ENGINE=InnoDB;

