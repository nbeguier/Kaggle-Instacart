#!/usr/bin/python2
#-*- coding: utf-8 -*-
""" Create features """

from psycopg2 import connect, OperationalError
from numpy import array
from pdb import set_trace as st

NB_USERS = 206209
NB_PRODUCTS = 49688
NB_DEPTS = 21
NB_AISLES = 134

# Fill new features created from all but the last two orders and are used to guess the penultimate orders.
SUFFIX = "new"
EVAL_SET_COL = "eval_set_new"
MAX_EVAL_SET_NEW = 2

# Fill all features created from all but the last one orders and are used to guess the last orders.
# SUFFIX = "all"
# EVAL_SET_COL = "eval_set"
# MAX_EVAL_SET_NEW = 4

#########################################################
# pg connection                                         #
#########################################################
def pg_connection(dbname='postgres', user='postgres', host='localhost',\
    password='postgres'):
    """
    Return a connection to the db
    """
    try:
        pg_conn = connect("dbname='%s' user='%s' host='%s' password='%s'"\
            % (dbname, user, host, password))
    except OperationalError:
        print('I am unable to connect to the database')
        pg_conn = None
    return pg_conn

#########################################################
# replace None by 0                                     #
#########################################################
def replacedNoneByZero(l):
    res = []
    for el in l:
        if el == None:
            res.append(0)
        else:
            res.append(el)
    return res

#########################################################
# eval_set_new                                          #
#########################################################
def update_eval_set_new():
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    for user_id in range(1, NB_USERS+1):
        cur.execute("""
            SELECT MAX(order_number), MAX(eval_set) 
            FROM orders WHERE user_id = %s
        """ % user_id)
        resp = cur.fetchone()
        max_order_number = resp[0]
        max_eval_set = resp[1]
        cur.execute("""
            UPDATE orders SET eval_set_new = %s WHERE user_id = %s AND order_number = %s
        """ % (max_eval_set, user_id, max_order_number-1))
        cur.execute("""
            UPDATE orders SET eval_set_new = %s WHERE user_id = %s AND order_number = %s
        """ % (max_eval_set+2, user_id, max_order_number))
    cur.execute("""
        UPDATE purchases_user_prior_orders 
        SET eval_set_new = orders.eval_set_new 
        FROM orders 
        WHERE purchases_user_prior_orders.order_id = orders.order_id
    """)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# User features                                         #
#########################################################
def create_user_features_table():
    """
    Create user_features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE user_features_%s (
        user_id          INTEGER  PRIMARY KEY  NOT NULL,
        u0               FLOAT             NOT NULL,
        u1               FLOAT             NOT NULL,
        u2               FLOAT             NOT NULL,
        u9               FLOAT             NOT NULL,
        u3               FLOAT             NOT NULL,
        u4               FLOAT             NOT NULL,
        u5               FLOAT             NOT NULL,
        u6               FLOAT             NOT NULL,
        u7               FLOAT             NOT NULL,
        u8               FLOAT             NOT NULL,
        u10              FLOAT             NOT NULL,
        u11              FLOAT             NOT NULL,
        u12              FLOAT             NOT NULL,
        u13              FLOAT             NOT NULL,
        u14              FLOAT             NOT NULL,
        u15              FLOAT             NOT NULL)
    """ % SUFFIX )
    pg_conn.commit()
    cur.close()
    pg_conn.close()

def user_features():
    """
    u0: mean(days between two consecutive orders)
    u1: nb of orders
    u2: nb of days between the first and the last orders
    u3: rate of reordered products
    u4: mean(nb of products per order)
    u5: nb of distinct products for the overall orders / NB_PRODUCTS
    u6: nb of distinct products for the overall orders / nb of purchases (with duplicates)
    u7: std(nb of products per order)
    u8: std(nb of reordered product per order)
    u9: std(days between two consecutive orders)
    u10: mean nb of products per order when dow = (day of last order)
    u11: mean nb of products per order when hour in (category of hour of the last order)
    # Reminder: order categories: 0h-8h, 9h-12h, 13h-17h, 18h-23h
    u12: idem for day and hour
    u13: days since last order
    u14: nb of products purchased at least twice / nb of different product purchased 
    u15: mean(nb of reordered products per order) without first order
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    for user_id in range(1, NB_USERS+1):
        features = [user_id]
        # u0: mean(days between two consecutive orders)
        # u1: nb of orders
        # u2: nb of days between the first and the last orders
        # u9: std(days between two consecutive orders)
        cur.execute("""
            SELECT AVG(days_since), MAX(order_number), SUM(days_since), STDDEV(days_since) 
            FROM orders 
            WHERE user_id = %s AND order_number > 1 AND %s = 0
        """ % (user_id, EVAL_SET_COL))
        features += map(float, replacedNoneByZero(cur.fetchone()))
        # u3: rate of reordered products
        cur.execute("""
            SELECT AVG(reordered)
            FROM purchases_user_prior_orders
            WHERE user_id = %s AND order_number > 1 AND %s = 0
        """ % (user_id, EVAL_SET_COL))
        features.append(float(cur.fetchone()[0]))
        # u4: mean(nb of products per order)
        # u5: nb of distinct products for the overall orders
        # u6: nb of products (with duplicates) for the overall orders
        cur.execute("""
            SELECT 1.0 * COUNT(*) / MAX(order_number), COUNT(DISTINCT product_id), COUNT(*)
            FROM purchases_user_prior_orders
            WHERE user_id = %s AND %s = 0
        """ % (user_id, EVAL_SET_COL))
        u4_u5_u6 = map(float, cur.fetchone())
        features.append(u4_u5_u6[0])
        features.append(u4_u5_u6[1] / NB_PRODUCTS)
        features.append(u4_u5_u6[1] / u4_u5_u6[2])
        # features += map(float, cur.fetchone())
        # u7: std(nb of products per order)
        # u8: std(nb of reordered product per order)
        cur.execute("""
            SELECT STDDEV(res.nb), STDDEV(res.re) FROM (
                SELECT COUNT(*) AS nb, SUM(reordered) as re FROM purchases_user_prior_orders 
                WHERE user_id = %s AND %s = 0 GROUP BY order_number
            ) AS res
        """ % (user_id, EVAL_SET_COL))
        features += map(float, replacedNoneByZero(cur.fetchone()))
        # u10: mean nb of products per order when dow = (day of last order)
        # u11: mean nb of products per order when hour in (category of hour of the last order)
        cur.execute("""
            SELECT order_dow, order_hour, days_since FROM orders WHERE user_id = %s AND %s IN (1, 2)
        """ %  (user_id, EVAL_SET_COL))
        dow_hour_u13 = cur.fetchone()
        order_dow = dow_hour_u13[0]
        order_hour = dow_hour_u13[1]
        # u13: days since last order
        u13 = dow_hour_u13[2]
        # u10: mean nb of products per order when dow = (day of last order)
        cur.execute("""
            SELECT COUNT(*), COUNT(DISTINCT order_number) FROM purchases_user_prior_orders 
            WHERE user_id = %s AND order_dow = %s AND %s = 0
        """ % (user_id, order_dow, EVAL_SET_COL))
        u10_num_den = map(float, cur.fetchone())
        if u10_num_den[1] == 0:
            u10 = 0
        else:
            u10 = u10_num_den[0] / u10_num_den[1]
        features.append(u10)
        # u11: mean nb of products per order when hour in (category of hour of the last order)
        # Reminder: order categories: 0h-8h, 9h-12h, 13h-17h, 18h-23h
        if order_hour < 9:
            min_hour = 0
            max_hour = 8
        elif order_hour < 13:
            min_hour = 9
            max_hour = 12
        elif order_hour < 18:
            min_hour = 13
            max_hour = 17
        else:
            min_hour = 18
            max_hour = 23
        cur.execute("""
            SELECT COUNT(*), COUNT(DISTINCT order_number) FROM purchases_user_prior_orders 
            WHERE user_id = %s AND order_hour BETWEEN %s AND %s AND %s = 0
        """ % (user_id, min_hour, max_hour, EVAL_SET_COL))
        u11_num_den = map(float, cur.fetchone())
        if u11_num_den[1] == 0:
            u11 = 0
        else:
            u11 = u11_num_den[0] / u11_num_den[1]
        features.append(u11)
        # u12: idem for day and hour
        cur.execute("""
            SELECT COUNT(*), COUNT(DISTINCT order_number) FROM purchases_user_prior_orders 
            WHERE user_id = %s AND order_dow = %s AND order_hour BETWEEN %s AND %s AND %s = 0
        """ % (user_id, order_dow, min_hour, max_hour, EVAL_SET_COL))
        u12_num_den = map(float, cur.fetchone())
        if u12_num_den[1] == 0:
            u12 = 0
        else:
            u12 = u12_num_den[0] / u12_num_den[1]
        features.append(u12)
        # u13: days since last order
        features.append(u13)
        # u14: nb of products purchased at least twice / nb of different product purchased (u5) 
        cur.execute("""
            SELECT COUNT(DISTINCT product_id) FROM purchases_user_prior_orders 
            WHERE user_id = %s AND reordered = 1 and %s = 0
        """ % (user_id, EVAL_SET_COL))
        features.append(float(cur.fetchone()[0]) / features[7])
        # u15: mean(nb of reordered products per order) without first order
        cur.execute("""
            SELECT SUM(reordered) FROM purchases_user_prior_orders 
            WHERE user_id = %s and %s = 0
        """ % (user_id, EVAL_SET_COL))
        features.append(float(cur.fetchone()[0]) / (features[2]-1))
        cur.execute("""INSERT INTO user_features_%s VALUES %S""" % (SUFFIX, str(tuple(features))))
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# Product features                                      #
#########################################################
def create_product_features_table():
    """
    Create product_features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE product_features_%s(
        product_id          INTEGER  PRIMARY KEY  NOT NULL,
        p0               FLOAT             NOT NULL,
        p1               FLOAT             NOT NULL,
        p2               FLOAT             NOT NULL,
        p3               FLOAT             NOT NULL,
        p4               FLOAT             NOT NULL,
        p5               FLOAT             NOT NULL,
        p6               FLOAT             NOT NULL,
        p7               FLOAT             NOT NULL,
        p8               FLOAT             NOT NULL,
        p9               FLOAT             NOT NULL)
    """ % SUFFIX)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

def product_features():
    """
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
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT order_id) FROM orders WHERE %s = 0
    """ % EVAL_SET_COL)
    nb_orders = float(cur.fetchone()[0]) 
    for product_id in range(1, NB_PRODUCTS+1):
        features = [product_id]
        # p0: nb of orders with the product * 100 / nb of orders (3214874)
        # p1: nb of users who have ordered it (at least one) * 100 / nb of users
        # p2: nb of reorders
        # p3: mean add to cart order
        # p4: std(add to cart order)
        cur.execute("""
            SELECT COUNT(DISTINCT order_id), COUNT(DISTINCT user_id), SUM(reordered), AVG(add_order), STDDEV(add_order) 
            FROM purchases_user_prior_orders 
            WHERE product_id = %s AND %s = 0
        """ % (product_id, EVAL_SET_COL))
        res = map(float, replacedNoneByZero(cur.fetchone()))
        features.append(100.0 * res[0] / nb_orders)
        features.append(100.0 * res[1] / NB_USERS)
        features += res[2:]
        # p5: nb of users who have reordered it / nb of users who have ordered it
        cur.execute("""
            SELECT COUNT(DISTINCT user_id) 
            FROM purchases_user_prior_orders 
            WHERE product_id = %s AND reordered = 1 AND %s = 0
        """ % (product_id, EVAL_SET_COL))
        if res[1] == 0:
            features.append(0)
        else:
            features.append(float(cur.fetchone()[0]) / res[1])
        # p6: aisle id
        # p7: department id
        cur.execute("""
            SELECT aisle_id, department_id  
            FROM products WHERE product_id = %s
        """ % product_id)
        features += map(float, cur.fetchone())
        # p8: days frequency (mean nb of days between two consecutive orders of this product)
        # p9: order frequency (mean nb orders between two consecutive orders of this product)
        p8_num = 0
        p8_den = 0
        p9_num = 0
        p9_den = 0
        cur.execute("""
            SELECT DISTINCT user_id FROM purchases_user_prior_orders 
            WHERE product_id = %s AND %s = 0
        """ % (product_id, EVAL_SET_COL))
        for user_id in cur.fetchall():
            user_id = user_id[0]
            cur.execute("""
                SELECT MIN(order_number), MAX(order_number), COUNT(*) 
                FROM purchases_user_prior_orders 
                WHERE user_id = %s AND product_id = %s AND %s = 0
            """ % (user_id,product_id, EVAL_SET_COL))
            min_max_order_nb = cur.fetchall()[0]
            min_order_nb_up = min_max_order_nb[0] 
            max_order_nb_up = min_max_order_nb[1] 
            nb_orders_up = int(min_max_order_nb[2])
            if nb_orders_up == 1:
                cur.execute("""
                    SELECT MAX(order_number) FROM orders 
                    WHERE user_id = %s AND %s = 0
                """ % (user_id, EVAL_SET_COL))
                max_order_nb_u = cur.fetchone()[0]
                if max_order_nb_u != max_order_nb_up:
                    cur.execute("""
                        SELECT SUM(days_since) FROM orders
                        WHERE user_id = %s AND order_number > %s AND %s = 0
                    """ % (user_id, min_order_nb_up, EVAL_SET_COL))
                    days_up_after = int(cur.fetchone()[0])
                else:
                    days_up_after = 0
                if min_order_nb_up > 1:
                    cur.execute("""
                        SELECT SUM(days_since) FROM orders
                        WHERE user_id = %s AND order_number BETWEEN 2 AND %s AND %s = 0
                    """ % (user_id, min_order_nb_up, EVAL_SET_COL))
                    days_up_before = int(cur.fetchone()[0])
                else:
                    days_up_before = 0
                days_up = max(days_up_before, days_up_after)
                diff_order_nb = max(max_order_nb_u - min_order_nb_up, min_order_nb_up-1) + 1 
            else:
                cur.execute("""
                    SELECT SUM(days_since) FROM orders
                    WHERE user_id = %s AND order_number BETWEEN %s AND %s AND %s = 0
                """ % (user_id,min_order_nb_up+1, max_order_nb_up, EVAL_SET_COL))
                days_up = int(cur.fetchone()[0])
                diff_order_nb = max_order_nb_up - min_order_nb_up 
            # p8: days frequency (mean nb of days between two consecutive orders of this product)
            # p9: order frequency (mean nb orders between two consecutive orders of this product)
            p8_num +=  days_up
            p8_den += max(nb_orders_up - 1, 1)
            p9_num += diff_order_nb
            p9_den += max(nb_orders_up - 1, 1)
        if p8_den == 0:
            features.append(0)
        else:
            features.append(float(p8_num) / p8_den)
        if p9_den == 0:
            features.append(0)
        else:
            features.append(float(p9_num) / p9_den)
        cur.execute("""INSERT INTO product_features_%s VALUES %s""" % (SUFFIX, str(tuple(features))))
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# Department features                                   #
#########################################################
def create_dept_features_table():
    """
    Create dept_features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE dept_features_%s(
        dept_id          INTEGER  PRIMARY KEY  NOT NULL,
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
        d11              FLOAT             NOT NULL,
        d12              FLOAT             NOT NULL,
        d13              FLOAT             NOT NULL,
        d14              FLOAT             NOT NULL,
        d15              FLOAT             NOT NULL,
        d16              FLOAT             NOT NULL,
        d17              FLOAT             NOT NULL)
    """ % SUFFIX)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

def dept_features():
    """
    List of features:
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
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        select COUNT(DISTINCT order_id) from orders WHERE %s = 0
    """ % EVAL_SET_COL)
    nb_orders_prior = float(cur.fetchone()[0])
    for dept_id in range(1, NB_DEPTS+1):
        features = [dept_id]
        # d0: nb of purchases the day 0 in this dep / nb of overall purchases in this dep
        # d1: nb of purchases the day 1 in this dep / nb of overall purchases in this dep
        # d2: nb of purchases the day 2 in this dep / nb of overall purchases in this dep
        # d3: nb of purchases the day 3 in this dep / nb of overall purchases in this dep
        # d4: nb of purchases the day 4 in this dep / nb of overall purchases in this dep
        # d5: nb of purchases the day 5 in this dep / nb of overall purchases in this dep
        # d6: nb of purchases the day 6 in this dep / nb of overall purchases in this dep
        cur.execute("""
            SELECT COUNT(order_dow) FROM purchases_user_prior_orders
            WHERE department_id = %s AND %s = 0
            GROUP BY order_dow ORDER BY order_dow
        """ % (dept_id, EVAL_SET_COL))
        sum_purchases_per_dow = map(int, array(cur.fetchall()).T[0].tolist())
        nb_purchases_per_dept = float(sum(sum_purchases_per_dow))
        features += map(lambda x: x/nb_purchases_per_dept, sum_purchases_per_dow)
        # d7: nb of purchases 0h-8h in this dep /nb of purchases in this dep
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE department_id = %s AND order_hour < 9 AND %s = 0
        """ % (dept_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_dept)
        # d8: nb of purchases 9h-12h in this dep /nb of purchases in this dep
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE department_id = %s AND order_hour BETWEEN 9 AND 12 AND %s = 0
        """ % (dept_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_dept)
        # d9: nb of purchases 13h-17h in this dep /nb of purchases in this dep
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE department_id = %s AND order_hour BETWEEN 13 AND 17 AND %s = 0
        """ % (dept_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_dept)
        # d10: nb of purchases 18h-23h in this dep /nb of purchases in this dep
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE department_id = %s AND order_hour > 17 AND %s = 0
        """ % (dept_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_dept)
        # d11: rate of products in this dep
        cur.execute("""
            SELECT COUNT(*) FROM products WHERE department_id = %s
        """ % (dept_id))
        nb_products_per_dept = float(cur.fetchone()[0])
        features.append(nb_products_per_dept / NB_PRODUCTS)
        # d12: rate of users in this dep
        # d13: nb of orders with at least one product in this dep / nb of orders
        # d14: mean add to cart order
        # d15: std(14)
        cur.execute("""
            SELECT COUNT(DISTINCT user_id), COUNT(DISTINCT order_id), AVG(add_order), STDDEV(add_order)
            FROM purchases_user_prior_orders
            WHERE department_id = %s AND %s = 0
        """ % (dept_id, EVAL_SET_COL))
        d12_d15 = cur.fetchall()[0]
        features.append(float(d12_d15[0]) / NB_USERS)
        features.append(float(d12_d15[1]) / nb_orders_prior)
        features.append(float(d12_d15[2]))
        features.append(float(d12_d15[3]))
        # d16: mean days between two consecutive orders in this dep
        # d17: mean nb of orders between two consecutive orders in this dep
        d16_num = 0
        d16_den = 0
        d17_num = 0
        d17_den = 0
        cur.execute("""
            SELECT DISTINCT user_id
            FROM purchases_user_prior_orders
            WHERE department_id = %s AND %s = 0
        """ % (dept_id, EVAL_SET_COL))
        for user_id in cur.fetchall():
            user_id = user_id[0]
            cur.execute("""
                SELECT MIN(order_number), MAX(order_number), COUNT(DISTINCT order_number)
                FROM purchases_user_prior_orders
                WHERE user_id = %s AND department_id = %s AND %s = 0
            """ % (user_id,dept_id, EVAL_SET_COL))
            min_max_order_nb = cur.fetchall()[0]
            min_order_nb_up = min_max_order_nb[0]
            max_order_nb_up = min_max_order_nb[1]
            nb_orders_up = int(min_max_order_nb[2])
            if nb_orders_up == 1:
                cur.execute("""
                    SELECT MAX(order_number) FROM orders WHERE user_id = %s AND %s = 0
                """ % (user_id, EVAL_SET_COL))
                max_order_nb_u = cur.fetchone()[0]
                # Evaluate days_up_after
                if max_order_nb_u != max_order_nb_up:
                    cur.execute("""
                        SELECT SUM(days_since) FROM orders
                        WHERE user_id = %s AND order_number > %s AND eval_set_new < = %s
                    """ % (user_id, max_order_nb_up, MAX_EVAL_SET_NEW))
                    days_up_after = int(cur.fetchone()[0])
                else:
                    days_up_after = 0
                # Evaluate days_up_before
                if min_order_nb_up > 1:
                    cur.execute("""
                        SELECT SUM(days_since) FROM orders
                        WHERE user_id = %s AND order_number BETWEEN 2 AND %s
                    """ % (user_id, min_order_nb_up))
                    days_up_before = int(cur.fetchone()[0])
                else:
                    days_up_before = 0
                days_up = max(days_up_before, days_up_after)
                diff_order_nb = max(max_order_nb_u - max_order_nb_up, min_order_nb_up-1) + 1
            else:
                cur.execute("""
                    SELECT SUM(days_since) FROM orders
                    WHERE user_id = %s AND order_number BETWEEN %s AND %s
                """ % (user_id,min_order_nb_up+1, max_order_nb_up))
                days_up = int(cur.fetchone()[0])
                diff_order_nb = max_order_nb_up - min_order_nb_up
            d16_num +=  days_up
            d16_den += max(nb_orders_up - 1, 1)
            d17_num += diff_order_nb
            d17_den += max(nb_orders_up - 1, 1)
        features.append(float(d16_num) / d16_den)
        features.append(float(d17_num) / d17_den) 
        cur.execute("""INSERT INTO dept_features_%s VALUES %s""" % (SUFFIX, str(tuple(features))))
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# Aisle features                                        #
#########################################################
def create_aisle_features_table():
    """
    Create aisle_features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE aisle_features_%s(
        aisle_id          INTEGER  PRIMARY KEY  NOT NULL,
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
        a11              FLOAT             NOT NULL,
        a12              FLOAT             NOT NULL,
        a13              FLOAT             NOT NULL,
        a14              FLOAT             NOT NULL,
        a15              FLOAT             NOT NULL,
        a16              FLOAT             NOT NULL,
        a17              FLOAT             NOT NULL)
    """ % SUFFIX)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

def aisle_features():
    """
    List of features:
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
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        select COUNT(DISTINCT order_id) from orders WHERE %s = 0
    """ % EVAL_SET_COL)
    nb_orders_prior = float(cur.fetchone()[0])
    for aisle_id in range(1, NB_AISLES+1):
        features = [aisle_id]
        # a0: nb of purchases the day 0 in this aisle / nb of overall purchases in this aisle 
        # a1: nb of purchases the day 1 in this aisle / nb of overall purchases in this aisle 
        # a2: nb of purchases the day 2 in this aisle / nb of overall purchases in this aisle 
        # a3: nb of purchases the day 3 in this aisle / nb of overall purchases in this aisle 
        # a4: nb of purchases the day 4 in this aisle / nb of overall purchases in this aisle 
        # a5: nb of purchases the day 5 in this aisle / nb of overall purchases in this aisle 
        # a6: nb of purchases the day 6 in this aisle / nb of overall purchases in this aisle 
        cur.execute("""
            SELECT COUNT(order_dow) FROM purchases_user_prior_orders
            WHERE aisle_id = %s AND %s = 0
            GROUP BY order_dow ORDER BY order_dow
        """ % (aisle_id, EVAL_SET_COL))
        sum_purchases_per_dow = map(int, array(cur.fetchall()).T[0].tolist())
        nb_purchases_per_aisle = float(sum(sum_purchases_per_dow))
        features += map(lambda x: x/nb_purchases_per_aisle, sum_purchases_per_dow)
        # a7: nb of purchases 0h-8h in this aisle /nb of purchases in this aisle
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE aisle_id = %s AND order_hour < 9 AND %s = 0
        """ % (aisle_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_aisle)
        # a8: nb of purchases 9h-12h in this aisle /nb of purchases in this aisle
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE aisle_id = %s AND order_hour BETWEEN 9 AND 12 AND %s = 0
        """ % (aisle_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_aisle)
        # a9: nb of purchases 13h-17h in this aisle /nb of purchases in this aisle
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE aisle_id = %s AND order_hour BETWEEN 13 AND 17 AND %s = 0
        """ % (aisle_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_aisle)
        # a10: nb of purchases 18h-23h in this aisle /nb of purchases in this aisle
        cur.execute("""
            SELECT COUNT(*) FROM purchases_user_prior_orders
            WHERE aisle_id = %s AND order_hour > 17 AND %s = 0
        """ % (aisle_id, EVAL_SET_COL))
        features.append(int(cur.fetchone()[0]) / nb_purchases_per_aisle)
        # a11: rate of products in this aisle
        cur.execute("""
            SELECT COUNT(*) FROM products WHERE aisle_id = %s
        """ % (aisle_id))
        nb_products_per_aisle = float(cur.fetchone()[0])
        features.append(nb_products_per_aisle / NB_PRODUCTS)
        # a12: rate of users in this aisle
        # a13: nb of orders with at least one product in this aisle / nb of orders
        # a14: mean add to cart order
        # a15: std(14)
        cur.execute("""
            SELECT COUNT(DISTINCT user_id), COUNT(DISTINCT order_id), AVG(add_order), STDDEV(add_order)
            FROM purchases_user_prior_orders
            WHERE aisle_id = %s AND %s = 0
        """ % (aisle_id,EVAL_SET_COL))
        a12_a15 = cur.fetchall()[0]
        features.append(float(a12_a15[0]) / NB_USERS)
        features.append(float(a12_a15[1]) / nb_orders_prior)
        features.append(float(a12_a15[2]))
        features.append(float(a12_a15[3]))
        # a16: mean days between two consecutive orders in this aisle
        # a17: mean nb of orders between two consecutive orders in this aisle
        a16_num = 0
        a16_den = 0
        a17_num = 0
        a17_den = 0
        cur.execute("""
            SELECT DISTINCT user_id
            FROM purchases_user_prior_orders
            WHERE aisle_id = %s AND %s = 0
        """ % (aisle_id, EVAL_SET_COL))
        for user_id in cur.fetchall():
            user_id = user_id[0]
            cur.execute("""
                SELECT MIN(order_number), MAX(order_number), COUNT(DISTINCT order_number)
                FROM purchases_user_prior_orders
                WHERE user_id = %s AND aisle_id = %s AND %s = 0
            """ % (user_id,aisle_id, EVAL_SET_COL))
            min_max_order_nb = cur.fetchall()[0]
            min_order_nb_up = min_max_order_nb[0]
            max_order_nb_up = min_max_order_nb[1]
            nb_orders_up = int(min_max_order_nb[2])
            if nb_orders_up == 1:
                cur.execute("""
                    SELECT MAX(order_number) FROM orders WHERE user_id = %s AND %s = 0
                """ % (user_id, EVAL_SET_COL))
                max_order_nb_u = cur.fetchone()[0]
                # Evaluate days_up_after
                if max_order_nb_u != max_order_nb_up:
                    cur.execute("""
                        SELECT SUM(days_since) FROM orders
                        WHERE user_id = %s AND order_number > %s AND eval_set <= %s
                    """ % (user_id, max_order_nb_up, MAX_EVAL_SET_NEW))
                    days_up_after = int(cur.fetchone()[0])
                else:
                    days_up_after = 0
                # Evaluate days_up_before
                if min_order_nb_up > 1:
                    cur.execute("""
                        SELECT SUM(days_since) FROM orders
                        WHERE user_id = %s AND order_number BETWEEN 2 AND %s
                    """ % (user_id, min_order_nb_up))
                    days_up_before = int(cur.fetchone()[0])
                else:
                    days_up_before = 0
                days_up = max(days_up_before, days_up_after)
                diff_order_nb = max(max_order_nb_u - max_order_nb_up, min_order_nb_up-1) + 1
            else:
                cur.execute("""
                    SELECT SUM(days_since) FROM orders
                    WHERE user_id = %s AND order_number BETWEEN %s AND %s
                """ % (user_id,min_order_nb_up+1, max_order_nb_up))
                days_up = int(cur.fetchone()[0])
                diff_order_nb = max_order_nb_up - min_order_nb_up
            a16_num +=  days_up
            a16_den += max(nb_orders_up - 1, 1)
            a17_num += diff_order_nb
            a17_den += max(nb_orders_up - 1, 1)
        features.append(float(a16_num) / a16_den)
        features.append(float(a17_num) / a17_den) 
        cur.execute("""INSERT INTO aisle_features_%s VALUES %s""" % (SUFFIX, str(tuple(features))))
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# User dept features                                    #
#########################################################
def create_user_dept_features_table():
    """
    Create user_dept_features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE user_dept_features_%s(
        id              TEXT  PRIMARY KEY  NOT NULL,
        user_id          INTEGER           NOT NULL,
        dept_id          INTEGER           NOT NULL,
        ud0               FLOAT             NOT NULL,
        ud1               FLOAT             NOT NULL,
        ud2               FLOAT             NOT NULL)
    """ % SUFFIX)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

def user_dept_features():
    """
    List of features:
    ud0: nb of purchases in this dept / nb of overall purchases
    ud1: nb of orders with at least one purchase in this dept / nb of orders
    ud2: mean purchases in this dept per order
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    for user_id in range(1, NB_USERS+1):
        cur.execute("""
            SELECT COUNT(*), COUNT(DISTINCT order_id) 
            FROM purchases_user_prior_orders 
            WHERE user_id = %s AND %s = 0
        """ % (user_id, EVAL_SET_COL))
        nb_purchases_orders_user = map(float, cur.fetchone())
        for dept_id in range(1, NB_DEPTS+1):
            features = [str(user_id) + "_" + str(dept_id), user_id, dept_id]
            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT order_id) 
                FROM purchases_user_prior_orders 
                WHERE user_id = %s AND department_id = %s AND %s = 0
            """ % (user_id, dept_id, EVAL_SET_COL))
            nb_purchases_orders_ud = map(float, cur.fetchone())
            if nb_purchases_orders_ud[1] == 0:
                continue
            features.append(nb_purchases_orders_ud[0] / nb_purchases_orders_user[0])
            features.append(nb_purchases_orders_ud[1] / nb_purchases_orders_user[1])
            features.append(nb_purchases_orders_ud[0] / nb_purchases_orders_ud[1]) 
            cur.execute("""INSERT INTO user_dept_features_%s VALUES %s""" % (SUFFIX, str(tuple(features))))
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# User aisle features                                   #
#########################################################
def create_user_aisle_features_table():
    """
    Create user_aisle_features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE user_aisle_features_%s(
        id              TEXT  PRIMARY KEY  NOT NULL,
        user_id          INTEGER           NOT NULL,
        aisle_id          INTEGER           NOT NULL,
        ua0               FLOAT             NOT NULL,
        ua1               FLOAT             NOT NULL,
        ua2               FLOAT             NOT NULL)
    """ % SUFFIX)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

def user_aisle_features():
    """
    List of features:
    ua0: nb of purchases in this aisle / nb of overall purchases
    ua1: nb of orders with at least one purchase in this aisle / nb of orders
    ua2: mean purchases in this aisle per order
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    for user_id in range(1, NB_USERS+1):
        cur.execute("""
            SELECT COUNT(*), COUNT(DISTINCT order_id) 
            FROM purchases_user_prior_orders 
            WHERE user_id = %s AND %s = 0
        """ % (user_id, EVAL_SET_COL))
        nb_purchases_orders_user = map(float, cur.fetchone())
        for aisle_id in range(1, NB_AISLES+1):
            features = [str(user_id) + "_" + str(aisle_id), user_id, aisle_id]
            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT order_id) 
                FROM purchases_user_prior_orders 
                WHERE user_id = %s AND aisle_id = %s AND %s = 0
            """ % (user_id, aisle_id, EVAL_SET_COL))
            nb_purchases_orders_ua = map(float, cur.fetchone())
            if nb_purchases_orders_ua[1] == 0:
                continue
            features.append(nb_purchases_orders_ua[0] / nb_purchases_orders_user[0])
            features.append(nb_purchases_orders_ua[1] / nb_purchases_orders_user[1])
            features.append(nb_purchases_orders_ua[0] / nb_purchases_orders_ua[1]) 
            cur.execute("""INSERT INTO user_aisle_features_%s VALUES %s""" % (SUFFIX, str(tuple(features))))
    pg_conn.commit()
    cur.close()
    pg_conn.close()
            
# def update_y_all():
#     pg_conn = pg_connection()
#     cur = pg_conn.cursor()
#     cur.execute("""
#         SELECT user_id, product_id FROM purchases_user_train_orders WHERE reordered = 1;
#     """)
#     for user_product_id in cur.fetchall():
#         cur.execute("""
#             UPDATE features_all SET y = 1 WHERE user_id = %s AND product_id = %s
#         """ % (int(user_product_id[0]), int(user_product_id[1])))
#     pg_conn.commit()
#     cur.close()
#     pg_conn.close()
# 
# update_y_all()


#########################################################
# User product features                                 #
#########################################################
def create_user_product_features_table():
    """
    Create user_product_features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE user_product_features_%s(
        id               TEXT  PRIMARY KEY  NOT NULL,
        user_id          INTEGER           NOT NULL,
        product_id       INTEGER           NOT NULL,
        f0               FLOAT             NOT NULL,
        f1               FLOAT             NOT NULL,
        f2               FLOAT             NOT NULL,
        f3               FLOAT             NOT NULL,
        f4               FLOAT             NOT NULL,
        f5               FLOAT             NOT NULL,
        f6               FLOAT             NOT NULL,
        d0               FLOAT             NOT NULL,
        d1               FLOAT             NOT NULL,
        a0               FLOAT             NOT NULL,
        a1               FLOAT             NOT NULL,
        ud0              FLOAT             NOT NULL,
        ud1              FLOAT             NOT NULL,
        ud2              FLOAT             NOT NULL,
        ua0              FLOAT             NOT NULL,
        ua1              FLOAT             NOT NULL,
        ua2              FLOAT             NOT NULL,
        y               INTEGER            NOT NULL,
        is_train        BOOLEAN            NOT NULL  DEFAULT FALSE)
    """ % SUFFIX)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

def user_product_features():
    """
    f0: percent of ordering this product
    f1: mean number of orders between two orders with the product
    f2: mean position of the product in the cart (with add_to_cart_order)
    f3: number of orders since the last order with the product
    f4: nb of days since last purchase with the product
    f5: mean number of days between two orders with the product
    f6: (n-1)/n where n is nb of orders with the product
    d0: d0, d1, d2, d3, d4, d5 or d6 according to order_dow and department_id
    d1: d7, d8, d9 or d10 according to order_hour and department_id
    a0: a0, a1, a2, a3, a4, a5 or a6 according to order_dow and aisle_id
    a1: a7, a8, a9 or a10 according to order_hour and aisle_id
    y: purchased in the next order
    is_train: train or test data
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    for user_id in range(1,206210):
        cur.execute("""
            SELECT max(order_number) FROM purchases_user_prior_orders 
            WHERE user_id = %s AND %s = 0
        """ % (user_id, EVAL_SET_COL))
        nb_orders_user = float(cur.fetchone()[0]) 
        cur.execute("""
            SELECT DISTINCT product_id FROM purchases_user_prior_orders 
            WHERE user_id = %s AND %s = 0
        """ % (user_id, EVAL_SET_COL))
        for product_id in cur.fetchall():
            product_id = product_id[0]
            features = [str(user_id) + '_' + str(product_id), user_id, product_id]
            # f0: percent of ordering this product
            # f1: mean number of orders between two orders with the product
            # f2: mean position of the product in the cart (with add_to_cart_order)
            # f3: number of orders since the last order with the product
            cur.execute("""
                SELECT COUNT(*), MIN(order_number), MAX(order_number), AVG(add_order) 
                FROM purchases_user_prior_orders 
                WHERE user_id = %s AND product_id = %s AND %s = 0
            """ % (user_id, product_id, EVAL_SET_COL))
            res = map(float, cur.fetchone())
            features.append(res[0] / nb_orders_user)
            if res[0] == 1:
                features.append(max(nb_orders_user - res[1] +1, res[1]))
            else:
                features.append((res[2]-res[1]) / (res[0]-1))
            features.append(res[3])
            features.append(nb_orders_user - res[2])
            # f4: nb of days since last purchase with the product
            cur.execute("""
                SELECT SUM(days_since) 
                FROM orders 
                WHERE user_id = %s AND order_number > %s AND eval_set_new <= %s
            """ % (user_id, res[2], MAX_EVAL_SET_NEW))
            features.append(float(cur.fetchone()[0]))
            # f5: mean number of days between two orders with the product
            if res[0] == 1:
                cur.execute("""
                    SELECT SUM(days_since) 
                    FROM orders 
                    WHERE user_id = %s AND order_number BETWEEN 2 AND %s 
                """ % (user_id, res[1]))
                days_before = cur.fetchone()[0]
                if days_before == None:
                    days_before = 0
                else:
                    days_before = float(days_before)
                cur.execute("""
                    SELECT SUM(days_since) 
                    FROM orders 
                    WHERE user_id = %s AND order_number BETWEEN %s AND %s 
                """ % (user_id, res[1]+1, nb_orders_user+1))
                days_after = cur.fetchone()[0]
                if days_after == None:
                    days_after = 0
                else:
                    days_after = float(days_after)
                features.append(max(days_before, days_after))
            else:            
                cur.execute("""
                    SELECT SUM(days_since) 
                    FROM orders 
                    WHERE user_id = %s AND order_number BETWEEN %s AND %s 
                """ % (user_id, res[1]+1, res[2]))
                features.append(float(cur.fetchone()[0]) / (res[0]-1))
            # f6: (n-1)/n where n is nb of orders with the product
            features.append((res[0]-1) / res[0])
            # d0: d0, d1, d2, d3, d4, d5 or d6 according to order_dow and department_id
            # d1: d7, d8, d9 or d10 according to order_hour and department_id
            # a0: a0, a1, a2, a3, a4, a5 or a6 according to order_dow and aisle_id
            # a1: a7, a8, a9 or a10 according to order_hour and aisle_id
            cur.execute("""
                SELECT order_dow, order_hour 
                FROM orders WHERE user_id = %s AND %s IN (1,2)
            """ % (user_id, EVAL_SET_COL))
            order_dow_hour = cur.fetchone()
            order_dow = order_dow_hour[0]
            order_hour = order_dow_hour[1]
            if order_hour < 9:
                col_order_hour = 7
            elif order_hour < 13:
                col_order_hour = 8
            elif order_hour < 18:
                col_order_hour = 9
            else:
                col_order_hour = 10
            cur.execute("""
                SELECT department_id, aisle_id 
                FROM products WHERE product_id = %s
            """ % (product_id))
            dept_aisle = cur.fetchone()
            dept_id = dept_aisle[0]
            aisle_id = dept_aisle[1]
            cur.execute("""
                SELECT d%s, d%s 
                FROM dept_features_%s WHERE dept_id = %s
            """ % (order_dow, col_order_hour,SUFFIX,dept_id))
            features += map(float, cur.fetchone())
            cur.execute("""
                SELECT a%s, a%s 
                FROM aisle_features_%s WHERE aisle_id = %s
            """ % (order_dow,col_order_hour ,SUFFIX,aisle_id))
            features += map(float, cur.fetchone())
            # ud0: nb of purchases in this dept / nb of overall purchases
            # ud1: nb of orders with at least one purchase in this dept / nb of orders
            # ud2: mean purchases in this dept per order
            cur.execute("""
                SELECT ud0, ud1, ud2 
                FROM user_dept_features_%s WHERE user_id = %s AND dept_id = %s
            """ % (SUFFIX, user_id, dept_id))
            features += map(float, cur.fetchone())
            # ua0: nb of purchases in this aisle / nb of overall purchases
            # ua1: nb of orders with at least one purchase in this aisle / nb of orders
            # ua2: mean purchases in this aisle per order
            cur.execute("""
                SELECT ua0, ua1, ua2 
                FROM user_aisle_features_%s WHERE user_id = %s AND aisle_id = %s
            """ % (SUFFIX, user_id, aisle_id))
            features += map(float, cur.fetchone())
            # y: purchased in the next order
            if SUFFIX == "all":
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM purchases_user_train_orders 
                    WHERE user_id = %s AND product_id = %s 
                """ % (user_id, product_id))
                features.append(int(cur.fetchone()[0]))
            else:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM purchases_user_prior_orders 
                    WHERE user_id = %s AND product_id = %s AND order_number = %s
                """ % (user_id, product_id, nb_orders_user + 1))
                features.append(int(cur.fetchone()[0]))
            # is_train: train or test user_id
            cur.execute("""
                SELECT %s 
                FROM orders 
                WHERE user_id = %s AND order_number = %s
            """ % (EVAL_SET_COL,user_id, nb_orders_user + 1))
            features.append(int(cur.fetchone()[0]) == 1)
            cur.execute("""INSERT INTO user_product_features_%s VALUES %s""" % (SUFFIX,str(tuple(features))))
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# Create features table                             #
#########################################################
def create_features_table():
    """
    Create features table
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE features_%s AS (
            SELECT up.*, pda.p0, pda.p1, pda.p2, pda.p3, pda.p4, pda.p5, pda.p6, pda.p7, pda.p8, pda.p9,
            pda.d11, pda.d12, pda.d13, pda.d14, pda.d15, pda.d16, pda.d17,
            pda.a11, pda.a12, pda.a13, pda.a14, pda.a15, pda.a16, pda.a17 FROM (
                SELECT f.*, u.u0, u.u1, u.u2, u.u3, u.u4, u.u5, u.u6, u.u7, u.u8, u.u9, 
                u.u10, u.u11, u.u12, u.u13, u.u14, u.u15 
                FROM user_product_features_%s AS f 
                LEFT JOIN user_features_%s  AS u ON f.user_id = u.user_id
                ) AS up 
            LEFT JOIN (
                SELECT pd.*, a.a11, a.a12, a.a13, a.a14, a.a15, a.a16, a.a17 FROM (
                    SELECT p.*, d.d11, d.d12, d.d13, d.d14, d.d15, d.d16, d.d17
                    FROM product_features_%s AS p 
                    LEFT JOIN dept_features_%s AS d
                    ON p.p7 = d.dept_id
                ) AS pd
                LEFT JOIN aisle_features_%s AS a
                ON pd.p6 = a.aisle_id
            ) AS pda
        ON up.product_id = pda.product_id
        )
    """ % (SUFFIX, SUFFIX, SUFFIX, SUFFIX, SUFFIX, SUFFIX))
    cur.execute("""
        CREATE INDEX up_%s_idx ON features_%s (user_id, product_id) 
    """ % (SUFFIX, SUFFIX))
    pg_conn.commit()
    cur.close()
    pg_conn.close()

#########################################################
# Main                                                  #
#########################################################
# update_eval_set_new()
# create_user_features_table()
# user_features()
# create_product_features_table()
# product_features()
# create_dept_features_table()
# dept_features()
# create_aisle_features_table()
# aisle_features()
# create_user_dept_features_table()
# user_dept_features()
# create_user_aisle_features_table()
# user_aisle_features()
# create_user_product_features_table()
# user_product_features()
# create_features_table()

