create table if not exists product(
  sku varbinary(128),
  description varbinary(128),
  price bigint,
  primary key(sku)
) ENGINE=InnoDB;
create table if not exists customer(
  customer_id bigint not null auto_increment,
  email varbinary(128),
  primary key(customer_id)
) ENGINE=InnoDB;
create table if not exists corder(
  order_id bigint not null auto_increment,
  customer_id bigint,
  sku varbinary(128),
  price bigint,
  primary key(order_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS messages (
  page BIGINT(20) UNSIGNED,
  time_created_ns BIGINT(20) UNSIGNED,
  message VARCHAR(10000),
  PRIMARY KEY (page, time_created_ns)
) ENGINE=InnoDB
