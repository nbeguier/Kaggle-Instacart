# Features

## Hardware

|     |            |       |
|----------|-------------:|------:|
| PC |    LENOVO T460p   |    |
| OS | Debian 9 | | 
| CPUx2 (8 threads) |  Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz |  |
| RAM |    15.11 GB   |    |

http://blenchmark.com/cpu-benchmarks

## Prerequisites

### PostgresQL Server
    sudo apt-get install postgresql-9.6

### Python 2.7

 - numpy (1.13.1)
 - xgboost (0.6a2)
 - psycopg2 (2.7.1)
 - sklearn  (0.18-5)

## Old features (features used for the first learning)

### User features
u0: mean(days between two consecutive orders)
u1: nb of orders
u2: nb of days between the first and the last orders
u3: rate of reordered products
u4: mean(nb of products per order)
u5: nb of distinct products for the overall orders
u6: nb of products (with duplicates) for the overall orders
u7: std(nb of products per order)
u8: std(nb of reordered product per order)
u9: std(days between two consecutive orders)
u10: mean nb of products per order when dow = (day of last order)
u11: mean nb of products per order when hour in (category of hour of the last order)

> Reminder: order categories: 0h-8h, 9h-12h, 13h-17h, 18h-23h

u12: idem pour day and hour
u13: days since last order
u14: nb of products purchased at least twice / nb of different product purchased 
u15: mean(nb of reordered products per order) without first order

### Product features
p0: nb of orders with the product * 100 / nb of orders (3214874)
p1: nb of users who have ordered it (at least one) * 100 / nb of users
p2: nb of reorders
p3: nb of users who have reordered it / nb of users who have ordered it
p4: proba to purchase it = nb of orders with the products  * 100 / nb of orders
p5: aisle id
p6: department id
p7: mean add to cart order
p8: std(add to cart order)
p9: days frequency (mean nb of days between two consecutive orders of this product)
p10: order frequency (mean nb orders between two consecutive orders of this product)

### User product features
f0: percent of ordering this product
f1: mean number of orders between two orders with the product
f2: mean position of the product in the cart (with add_to_cart_order)
f3: number of orders since the last order with the product
f4: (n-1)/n where n is nb of orders with the product
f5: nb of days since last purchase with the product

### Department features
d0: nb of purchases the day 0 in this dep / nb of overall purchases in this dep
d1: nb of purchases the day 1 in this dep / nb of overall purchases in this dep
d2: nb of purchases the day 2 in this dep / nb of overall purchases in this dep
d3: nb of purchases the day 3 in this dep / nb of overall purchases in this dep
d4: nb of purchases the day 4 in this dep / nb of overall purchases in this dep
d5: nb of purchases the day 5 in this dep / nb of overall purchases in this dep
d6: nb of purchases the day 6 in this dep / nb of overall purchases in this dep
d7: nb of purchases 0h-8h in this dep /nb of purchases in this dep
d8: nb of purchases 9h-12h in this dep /nb of purchases in this dep
d9: nb of purchases 13h-17h in this dep /nb of purchases in this dep
d10: nb of purchases 18h-23h in this dep /nb of purchases in this dep
d11: rate of products in this dep
d12: rate of users in this dep
d13: nb of orders with at least one product in this dep / nb of orders
d14: mean add to cart order
d15: std(14)
d16: mean days between two consecutive orders in this dep
d17: mean nb of orders between two consecutive orders in this dep

Into features:
d0: d0-d6 according to the day of last order
d1: d7-d10 according to the day of last order
d2 = d11
d12 to d17

### Features Importances

-----
Train : 131209
Test : 4209
F1 SCORE : 0.440594823594
-----
f5 : 8.17071151733
u13 : 6.78771448135
f3 : 6.06651973724
u0 : 4.83624649048
f1 : 4.25080633163
u3 : 4.06414365768
f0 : 3.9538435936
u14 : 3.84354329109
p3 : 3.55506539345
p1 : 3.43628048897
f4 : 3.33446455002
u5 : 3.23264884949
u9 : 2.94417095184
p9 : 2.91023254395
u7 : 2.84235525131
u2 : 2.48600029945
u15 : 2.24843025208
p10 : 2.22297644615
p7 : 2.18055319786
p2 : 2.12116074562
f2 : 2.0872220993
u10 : 2.0872220993
u4 : 1.9769217968
p0 : 1.93449854851
u11 : 1.80722880363
d0 : 1.79874432087
u8 : 1.65450537205
u12 : 1.48481249809
d2 : 1.39148139954
p8 : 1.38299679756
d1 : 1.35754287243
u6 : 1.19633460045
d16 : 1.13694214821
d12 : 0.916341423988
d15 : 0.899372160435
d14 : 0.856948912144
d13 : 0.390293568373
p4 : 0.110300347209
d17 : 0.0424232147634

## New and all features
New features are created from all but the last two orders and are used to guess the penultimate orders. 
All features are created from all but the last one orders and are used to guess the last orders.

### User features
u0: mean(days between two consecutive orders)
u1: nb of orders
u2: nb of days between the first and the last orders
u3: rate of reordered products
u4: mean(nb of products per order)
u5: nb of distinct products for the overall orders * 100.0 / nb products (49688)
u6: nb of distinct products for the overall orders / nb of purchases (with duplicates)
u7: std(nb of products per order)
u8: std(nb of reordered product per order)
u9: std(days between two consecutive orders)
u10: mean nb of products per order when dow = (day of last order)
u11: mean nb of products per order when hour in (category of hour of the last order)

> Reminder: order categories: 0h-8h, 9h-12h, 13h-17h, 18h-23h

u12: idem for day and hour
u13: days since last order
u14: nb of products purchased at least twice / nb of different product purchased 
u15: mean(nb of reordered products per order) without first order

### Product features
p0: nb of orders with the product * 100 / nb of orders (3214874)
p1: nb of users who have ordered it (at least one) * 100 / nb of users
p2: nb of reorders
p3: mean add to cart order
p4: std(add to cart order)
p5: nb of users who have reordered it / nb of users who have ordered it
p6: aisle id
p7: department id
p8: days frequency (mean nb of days between two consecutive orders of this product)
p9: order frequency (mean nb orders between two consecutive orders of this product)

### User product features 
f0: percent of ordering this product
f1: mean number of orders between two orders with the product
f2: mean position of the product in the cart (with add_to_cart_order)
f3: number of orders since the last order with the product
f4: nb of days since last purchase with the product
f5: mean number of days between two orders with the product
f6: (n-1)/n where n is nb of orders with the product

### Department features
d0: nb of purchases the day 0 in this dep / nb of overall purchases in this dep
d1: nb of purchases the day 1 in this dep / nb of overall purchases in this dep
d2: nb of purchases the day 2 in this dep / nb of overall purchases in this dep
d3: nb of purchases the day 3 in this dep / nb of overall purchases in this dep
d4: nb of purchases the day 4 in this dep / nb of overall purchases in this dep
d5: nb of purchases the day 5 in this dep / nb of overall purchases in this dep
d6: nb of purchases the day 6 in this dep / nb of overall purchases in this dep
d7: nb of purchases 0h-8h in this dep /nb of purchases in this dep
d8: nb of purchases 9h-12h in this dep /nb of purchases in this dep
d9: nb of purchases 13h-17h in this dep /nb of purchases in this dep
d10: nb of purchases 18h-23h in this dep /nb of purchases in this dep
d11: nb of products in this dep / NB_PRODUCTS
d12: nb of users in this dep / NB_USERS
d13: nb of orders with at least one product in this dep / nb of orders
d14: mean add to cart order
d15: std(14)
d16: mean days between two consecutive orders in this dep
d17: mean nb of orders between two consecutive orders in this dep

Into features:
d0: d0-d6 according to the day of last order
d1: d7-d10 according to the day of last order
d11 to d17

### Aisle features
a0: nb of purchases the day 0 in this aisle / nb of overall purchases in this aisle
a1: nb of purchases the day 1 in this aisle / nb of overall purchases in this aisle
a2: nb of purchases the day 2 in this aisle / nb of overall purchases in this aisle
a3: nb of purchases the day 3 in this aisle / nb of overall purchases in this aisle
a4: nb of purchases the day 4 in this aisle / nb of overall purchases in this aisle
a5: nb of purchases the day 5 in this aisle / nb of overall purchases in this aisle
a6: nb of purchases the day 6 in this aisle / nb of overall purchases in this aisle
a7: nb of purchases 0h-8h in this aisle /nb of purchases in this aisle
a8: nb of purchases 9h-12h in this aisle /nb of purchases in this aisle
a9: nb of purchases 13h-17h in this aisle /nb of purchases in this aisle
a10: nb of purchases 18h-23h in this aisle /nb of purchases in this aisle
a11: nb of products in this aisle / NB_PRODUCTS
a12: nb of users in this aisle / NB_USERS
a13: nb of orders with at least one product in this aisle / nb of orders
a14: mean add to cart order
a15: std(14)
a16: mean days between two consecutive orders in this aisle
a17: mean nb of orders between two consecutive orders in this aisle

Into features:
a0: a0-a6 according to the day of last order
a1: a7-a10 according to the day of last order
a11 to a17

### User dept features
ud0: nb of purchases in this dept / nb of overall purchases
ud1: nb of orders with at least one purchase in this dept / nb of orders
ud2: mean purchases in this dept per order

### User aisle features
ua0: nb of purchases in this aisle / nb of overall purchases
ua1: nb of orders with at least one purchase in this aisle / nb of orders
ua2: mean purchases in this aisle per order

### Features Importances
XGB feature importance
f4 481
f3 336
f0 292
u13 278
p5 241
f6 201
p8 189
u14 184
f5 181
p1 181
f1 170
u0 160
u3 159
u6 149
u7 138
p2 136
f2 124
u5 114
u8 108
u2 104
p9 100
u12 97
u9 97
u15 93
u4 89
p3 80
a0 79
ud0 75
u11 75
a11 75
ua1 74
p0 74
ud2 71
ua0 62
a1 62
a16 58
u10 58
ua2 57
p4 55
d0 54
ud1 52
a12 49
u1 41
a14 39
a17 34
d1 29
-----
LGB feature importance
f4 : 579
u13 : 577
f3 : 448
f0 : 379
u3 : 306
f5 : 305
u14 : 299
p8 : 295
u5 : 291
p5 : 290
u0 : 281
u6 : 278
u7 : 238
u2 : 236
f6 : 235
f1 : 234
f2 : 233
u8 : 230
u4 : 217
u9 : 196
u12 : 191
u15 : 191
p1 : 179
u11 : 177
u10 : 168
ua1 : 166
p3 : 159
p9 : 158
ua2 : 151
p2 : 143
a0 : 143
a1 : 134
a16 : 127
ud2 : 116
p0 : 110
u1 : 109
ua0 : 107
a11 : 106
p4 : 100
d0 : 99
ud1 : 99
ud0 : 98
a14 : 97
a12 : 85
d1 : 78
a17 : 62
-----



## Others
https://www.analyticsvidhya.com/blog/2016/03/complete-guide-parameter-tuning-xgboost-with-codes-python/

docker run -d --rm -p 5432:5432 --name postgres-docker -e POSTGRES_PASSWORD=postgres postgres:latest
psql -h localhost -U postgres postgres

