CREATE TABLE user_features(
               user_id          INTEGER  PRIMARY KEY  NOT NULL,
               u0               FLOAT             NOT NULL,
               u1               FLOAT             NOT NULL,
               u2               FLOAT             NOT NULL,
               u3               FLOAT             NOT NULL,
               u4               FLOAT             NOT NULL,
               u5               FLOAT             NOT NULL,
               u6               FLOAT             NOT NULL
            );

CREATE TABLE product_features(
               product_id       INTEGER  PRIMARY KEY  NOT NULL,
               p0               FLOAT             NOT NULL,
               p1               FLOAT             NOT NULL,
               p2               FLOAT             NOT NULL,
               p3               FLOAT             NOT NULL,
               p4               FLOAT             NOT NULL,
               p5               FLOAT             NOT NULL,
               p6               FLOAT             NOT NULL
            );

CREATE TABLE user_product_features(
               id             TEXT PRIMARY KEY NOT NULL,
               user_id       INTEGER   NOT NULL,
               product_id    INTEGER   NOT NULL,
               f0               FLOAT             NOT NULL,
               f1               FLOAT             NOT NULL,
               f2               FLOAT             NOT NULL,
               f3               FLOAT             NOT NULL,
               f4               FLOAT             NOT NULL,
               f5               FLOAT             NOT NULL,
               f6               FLOAT             NOT NULL,
               f7               FLOAT             NOT NULL,
               Y             BOOLEAN  NOT NULL
            );

CREATE TABLE dept_features(
               dept_id             INTEGER PRIMARY KEY NOT NULL,
               d0               FLOAT             NOT NULL,
               d1               FLOAT             NOT NULL,
               d2               FLOAT             NOT NULL,
               d3               FLOAT             NOT NULL,
               d4               FLOAT             NOT NULL,
               d5               FLOAT             NOT NULL,
               d6               FLOAT             NOT NULL,
               d7               FLOAT             NOT NULL,
               d8               FLOAT             NOT NULL,
               d9               FLOAT             NOT NULL,
               d10              FLOAT             NOT NULL,
               d11              FLOAT             NOT NULL
            );

CREATE TABLE aisle_features(
               aisle_id             INTEGER PRIMARY KEY NOT NULL,
               a0               FLOAT             NOT NULL,
               a1               FLOAT             NOT NULL,
               a2               FLOAT             NOT NULL,
               a3               FLOAT             NOT NULL,
               a4               FLOAT             NOT NULL,
               a5               FLOAT             NOT NULL,
               a6               FLOAT             NOT NULL,
               a7               FLOAT             NOT NULL,
               a8               FLOAT             NOT NULL,
               a9               FLOAT             NOT NULL,
               a10              FLOAT             NOT NULL,
               a11              FLOAT             NOT NULL
               a12              FLOAT             NOT NULL
               a13              FLOAT             NOT NULL
               a14              FLOAT             NOT NULL
               a15              FLOAT             NOT NULL
            );

CREATE TABLE purchases_user_train(
               id             TEXT PRIMARY KEY NOT NULL,
               user_id       INTEGER   NOT NULL,
               order_id      INTEGER   NOT NULL,
               product_id    INTEGER   NOT NULL,
               add_order      INTEGER   NOT NULL,
               reordered      INTEGER   NOT NULL
            );

CREATE TABLE orders(
               order_id      INTEGER PRIMARY KEY NOT NULL,
               user_id       INTEGER   NOT NULL,
               eval_set      INTEGER   NOT NULL,
               order_number  INTEGER   NOT NULL,
               order_dow     INTEGER   NOT NULL,
               order_hour    INTEGER   NOT NULL,
               days_since    INTEGER
            );


COPY orders(order_id,user_id,eval_set,order_number,order_dow,order_hour,days_since) FROM '/home/constance/Bureau/Constance/Kaggle/Instacart/Data/orders_formatted.csv' DELIMITER ',' CSV HEADER;




