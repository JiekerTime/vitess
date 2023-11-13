CREATE TABLE `t_user_0`
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
    `d` int(16) DEFAULT NULL,
    `foo`        int(16)           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB
  CHARSET = utf8mb4;

CREATE TABLE `t_user_1`
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
    `d` int(16) DEFAULT NULL,
    `foo`        int(16)           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB
  CHARSET = utf8mb4;

CREATE TABLE `t_1_0`
(
    `id`         bigint,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_1_1`
(
    `id`         bigint,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_seq` (
                           `id` int NOT NULL,
                           `next_id` bigint DEFAULT NULL,
                           `cache` bigint DEFAULT NULL,
                           PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='vitess_sequence';

CREATE TABLE `t_2_0`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    varchar(64)       NOT NULL COMMENT '分片键',
    `f_table`    varchar(64)       NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_2_1`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    varchar(64)       NOT NULL COMMENT '分片键',
    `f_table`    varchar(64)       NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_3_0`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_3_1`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_4_0`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    varchar(64)       NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_4_1`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    varchar(64)       NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_5_0`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    varchar(64)       NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_5_1`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    varchar(64)       NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_6_0`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_6_1`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    int               NOT NULL COMMENT '分片键',
    `f_table`    int               NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_7_0`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    varchar(64)       NOT NULL COMMENT '分片键',
    `f_table`    varchar(64)       NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_7_1`
(
    `id`         bigint            AUTO_INCREMENT,
    `f_shard`    varchar(64)       NOT NULL COMMENT '分片键',
    `f_table`    varchar(64)       NOT NULL COMMENT '分表键',
    `f_date`     date              DEFAULT NULL,
    `f_tinyint`  tinyint           DEFAULT NULL,
    `f_bit`      bit(1)            DEFAULT NULL,
    `f_midint`   mediumint         DEFAULT NULL,
    `f_int`      int               DEFAULT NULL,
    `f_bool`     BOOLEAN           DEFAULT NULL,
    primary key (id)
) ENGINE = InnoDB;

CREATE TABLE `t_user_extra_0`
(
    `id`       bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id`  bigint(20) NOT NULL,
    `extra_id` bigint(20) NOT NULL,
    `email`    varchar(200)        DEFAULT NULL,
    `bar`      int                 DEFAULT NULL,
    `baz`      int                 DEFAULT NULL,
    `col`      char(32)   NOT NULL DEFAULT '',
    `foo`      int(16)             DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `t_user_extra_1`
(
    `id`       bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id`  bigint(20) NOT NULL,
    `extra_id` bigint(20) NOT NULL,
    `email`    varchar(200)        DEFAULT NULL,
    `bar`      int                 DEFAULT NULL,
    `baz`      int                 DEFAULT NULL,
    `col`      char(32)   NOT NULL DEFAULT '',
    `foo`      int(16)             DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `t_music_0`
(
    `id`      bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) NOT NULL,
    `col`     varchar(100) DEFAULT NULL,
    `a`       int(16)      DEFAULT NULL,
    `bar`     int          DEFAULT NULL,
    `foo` varchar(16) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE `t_music_1`
(
    `id`      bigint(20) NOT NULL AUTO_INCREMENT,
    `user_id` bigint(20) NOT NULL,
    `col`     varchar(100) DEFAULT NULL,
    `a`       int(16)      DEFAULT NULL,
    `bar`     int          DEFAULT NULL,
    `foo` varchar(16) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1
  DEFAULT CHARSET = utf8mb4;