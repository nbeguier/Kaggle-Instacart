#!/usr/bin/env python
#-*- coding: utf-8 -*-
""" Guess the next order for some users """

# Standard library imports
from datetime import datetime
from pdb import set_trace as st
from pickle import dump, load
from random import shuffle

# Third party library imports
import lightgbm as lgb
from numpy import array, std, sqrt, concatenate
from psycopg2 import connect, OperationalError
from sklearn.metrics import f1_score
import xgboost as xgb

# Personnal library imports
from f1_opt import F1Optimizer

LGB_ROUNDS = 400
PARAMETERS_LGB = {
    'task'            : 'train',
    'boosting_type'   : 'gbdt',
    'objective'       : 'binary',
    'metric'          : {'binary_logloss'},
    'num_leaves'      : 96,
    'max_depth'       : 10,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.95,
    'bagging_freq'    : 5,
    'verbose'         : 0
}
XGB_ROUNDS = 500
PARAMETERS_XGB = {
    'objective'        : 'reg:logistic',
    'eval_metric'      : 'logloss',
    'eta'              : 0.1,
    'max_depth'        : 6,
    'min_child_weight' : 10,
    'gamma'            : 0.70,
    'subsample'        : 0.76,
    'colsample_bytree' : 0.95,
    'alpha'            : 2e-05,
    'lambda'           : 10,
    'silent'           : 1
}

PICKLES_DIR = './pickles'
PICK_TEST = 37500
PICK_TRAIN = 65604
NUMBER_TEST = 1000 # Should be lower than PICK_TRAIN
NUMBER_TRAIN = PICK_TEST + PICK_TRAIN
FEATURES = ','.join([
    'f0,f1,f2,f3,f4,f5,f6',
    'u0,u1,u2,u3,u4,u5,u6,u7,u8,u9,u10,u11,u12,u13,u14,u15',
    'p0,p1,p2,p3,p4,p5,p8,p9',
    'a0,a1,a11,a16',
    'd0',
    'ua0,ua1,ua2',
    'ud0,ud2'
    ])

# Only use when select_product_ceil is call
CEIL = 0.22

#########################################################
# Pickles                                               #
#########################################################

def save_pickle(name, user_features):
    """
    Save a list into pickles
    """
    print 'Start saving %s...' % name
    pickle_file = open('%s/%s.txt' % (PICKLES_DIR, name), 'wb')
    dump(user_features, pickle_file)
    pickle_file.close()

def load_pickle(name):
    """
    Load pickle file
    """
    print 'Start loading %s...' % name
    pickle_file = open('%s/%s.txt' % (PICKLES_DIR, name), 'rb')
    result = load(pickle_file)
    pickle_file.close()
    return result

#########################################################
# Features                                              #
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
        print 'I am unable to connect to the database'
        pg_conn = None
        exit(1)
    return pg_conn

def gen_features_equals(user_id):
    """
    Generate features from users id
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT product_id FROM features_all WHERE user_id = %s ORDER BY product_id
    """ % user_id)
    p_var = cur.fetchall()
    cur.execute("""
        SELECT %s FROM features_all WHERE user_id = %s ORDER BY product_id 
    """ % (FEATURES, user_id))
    x_var = array(cur.fetchall())
    cur.execute("""
        SELECT y FROM features_all WHERE user_id = %s ORDER BY product_id
    """ % user_id)
    y_var = array(cur.fetchall()).T[0]
    cur.close()
    pg_conn.close()
    return p_var, x_var, y_var

def gen_features_in(users_id, table):
    """
    Generate features from users tuple
    """
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    cur.execute("""SELECT %s FROM %s WHERE user_id IN """ % (FEATURES, table) + str(users_id))
    x_var = array(cur.fetchall())
    cur.execute("""
        SELECT y FROM %s WHERE user_id IN """ % table + str(users_id))
    y_var = array(cur.fetchall()).T[0]
    cur.close()
    pg_conn.close()
    return x_var, y_var

#########################################################
# Evaluate learning                                     #
#########################################################

def generate_train_features(users_id_train, users_id_test):
    """
    WARNING:
    There is multiple features by user
    """
    print 'Generating Train features (%s)...' % (len(users_id_train) + len(users_id_test))

    time_start = datetime.now()
    x_res_train, y_res_train = gen_features_in(users_id_train, 'features_all')
    x_res_test, y_res_test = gen_features_in(users_id_test, 'features_new')
    x_res = concatenate((x_res_train, x_res_test), axis=0)
    y_res = concatenate((y_res_train, y_res_test), axis=0)
    del x_res_train, y_res_train, x_res_test, y_res_test
    print divmod((datetime.now() - time_start).days * 86400 + \
        (datetime.now() - time_start).seconds, 60)
    return x_res, y_res


def generate_test_features(user_id_test):
    """
    Generate the 'y_test' array
    It contains the real prediction
    """
    print 'Generating Test features (%s)...' % len(user_id_test)
    time_start = datetime.now()

    y_test = []
    x_test = []
    p_test = []
    for user_id in user_id_test:
        p_test_, x_test_, y_test_ = gen_features_equals(user_id)
        y_test.append(y_test_)
        x_test.append(x_test_)
        p_test.append(p_test_)

    print divmod((datetime.now() - time_start).days * 86400 + \
        (datetime.now() - time_start).seconds, 60)
    return p_test, x_test, y_test



def select_product_ceil(y_pred_raw, ceil=0.5):
    """
    Valid a production when proba > ceil
    """
    return (y_pred_raw > ceil).astype(int)

def select_product_n(y_pred_raw, nb_product_keep):
    """
    Valid the n which have the best proba
    """
    ceil_cheated = 1
    if nb_product_keep != 0:
        ceil_cheated = min(sorted(y_pred_raw, reverse=True)[:nb_product_keep])
    y_pred = (y_pred_raw >= ceil_cheated).astype(int)
    return y_pred

def select_product_optimf1_mix(y_pred_raw):
    """
    Select the k best products according to an F1 optimization
    """
    best_k, pred_none, _ = array(F1Optimizer.maximize_expectation(array(y_pred_raw)))
    if pred_none:
        best_k = 0
    return select_product_n(array(y_pred_raw), int(best_k))

def train(x_train, y_train, enable_xgb=True):
    """
    Build a new classifier which only train the "Train" Dataset
    """
    print 'Learning in progress (%s)...' % len(y_train)
    time_start = datetime.now()

    if enable_xgb:
        dtrain_xgb = xgb.DMatrix(x_train, label=y_train)
        cls = xgb.train(PARAMETERS_XGB, dtrain_xgb, XGB_ROUNDS)
    else:
        dtrain_lgb = lgb.Dataset(x_train, label=y_train)
        cls = lgb.train(PARAMETERS_LGB, dtrain_lgb, LGB_ROUNDS)

    print divmod((datetime.now() - time_start).days * 86400 + \
    (datetime.now() - time_start).seconds, 60)

    return cls

def verify(classifier, enable_xgb=True, stable=True, n_range=5):
    """
    Verify classifier with the "Test" dataset
    """
    train_users_id = array(USERS_ID_TRAIN[:PICK_TRAIN])

    f1_scores = []
    print 'Verify classifier (CEIL=%s)' % CEIL
    if stable:
        n_range = 1
    for _ in range(n_range):
        if not stable:
            shuffle(train_users_id)
        _, x_test, y_test = generate_test_features(train_users_id[:NUMBER_TEST])
        y_preds = []
        for k in range(NUMBER_TEST):
            if enable_xgb:
                d_test = xgb.DMatrix(x_test[k])
            else:
                d_test = x_test[k]
            predictions = classifier.predict(d_test)
            y_pred = select_product_optimf1_mix(predictions)
            # y_pred = select_product_ceil(predictions, ceil=CEIL)
            y_preds += y_pred.tolist()

        y_test_all = []
        for i in y_test:
            y_test_all += i.tolist()
        f1_scores.append(f1_score(y_test_all, y_preds))

    print '-----'
    print 'Train : %s' % len(USER_ID_TRAIN_SAMPLE)
    print 'Test : %s' % NUMBER_TEST
    print 'F1 SCORE : %s' % (sum(f1_scores)*100 / n_range)
    if not stable:
        print 'F1 VAR: %s' % std(f1_scores)
    print '-----'

def verify_2(classifiers, stable=True, n_range=5):
    """
    Verify classifier with the "Test" dataset
    """
    train_users_id = array(USERS_ID_TRAIN[:PICK_TRAIN])

    [cls_xgb, cls_lgb] = classifiers

    f1_scores = []
    print 'Verify classifiers (CEIL=%s)' % CEIL
    if stable:
        n_range = 1
    for _ in range(n_range):
        if not stable:
            shuffle(train_users_id)
        _, x_test, y_test = generate_test_features(train_users_id[:NUMBER_TEST])
        y_preds = []
        for k in range(NUMBER_TEST):
            d_test = xgb.DMatrix(x_test[k])
            predict = []
            xgb_pred = cls_xgb.predict(d_test)
            lgb_pred = cls_lgb.predict(x_test[k])
            for i in range(len(x_test[k])):
                predict.append(max(xgb_pred[i], lgb_pred[i]))
                # predict.append((xgb_pred[i] + lgb_pred[i])/2)
                # predict.append(sqrt(xgb_pred[i]*lgb_pred[i]))
            # y_pred = select_product_ceil(array(predict), ceil=CEIL)
            y_pred = select_product_optimf1_mix(predict)
            y_preds += y_pred.tolist()

        y_test_all = []
        for i in y_test:
            y_test_all += i.tolist()
        f1_scores.append(f1_score(y_test_all, y_preds))

    print '-----'
    print 'Train : %s' % len(USER_ID_TRAIN_SAMPLE)
    print 'Test : %s' % NUMBER_TEST
    print 'F1 SCORE : %s' % (sum(f1_scores)*100 / n_range)
    if not stable:
        print 'F1 VAR: %s' % std(f1_scores)
    print '-----'


def generate_csv(classifiers, x_test, p_test, user_id_test, mix='max'):
    """
    Generate CSV
    classifiers = [XGB, LGB]
    """
    print '-----'
    print 'Train : %s' % len(USER_ID_TRAIN_SAMPLE)
    print 'Test : %s' % len(USER_ID_TEST_SAMPLE)
    print 'Mix method: %s' % mix
    print '-----'

    [cls_xgb, cls_lgb] = classifiers

    csv_file = open('submission.csv', 'w')
    csv_file.write('order_id,products\n')
    pg_conn = pg_connection()
    cur = pg_conn.cursor()
    for j, user_id in enumerate(user_id_test):
        d_test = xgb.DMatrix(x_test[j])
        predict = []
        xgb_pred = cls_xgb.predict(d_test)
        lgb_pred = cls_lgb.predict(x_test[j])
        for i in range(len(x_test[j])):
            if mix is 'mean':
                predict.append((xgb_pred[i] + lgb_pred[i])/2)
            elif mix is 'sqrt_mean':
                predict.append(sqrt(xgb_pred[i]*lgb_pred[i]))
            else:
                predict.append(max(xgb_pred[i], lgb_pred[i]))
        # y_pred = select_product_ceil(array(predict), ceil=CEIL)
        y_pred = select_product_optimf1_mix(predict)

        product_id = array(p_test[j]).T[0]
        product_id_filtered = [i for indx, i in enumerate(product_id) if y_pred[indx]]
        cur.execute("""
            SELECT order_id FROM orders WHERE user_id = %s AND eval_set > 0""" % user_id)
        order_id = cur.fetchone()[0]
        if len(product_id_filtered) == 0:
            csv_file.write('%s,None\n' % order_id)
        else:
            csv_file.write('%s,%s\n' % (order_id, ' '.join(map(str, product_id_filtered))))
    cur.close()
    pg_conn.close()
    csv_file.close()

#########################################################
# Main                                                  #
#########################################################

PG_CONN = pg_connection()
CUR = PG_CONN.cursor()
print 'Start selecting data into database...'
CUR.execute("""SELECT user_id FROM orders WHERE eval_set = 1""")
USERS_ID_TRAIN = tuple(array(CUR.fetchall()).T[0])
CUR.execute("""SELECT user_id FROM orders WHERE eval_set = 2""")
USERS_ID_TEST = tuple(array(CUR.fetchall()).T[0])
CUR.close()
PG_CONN.close()

USER_ID_TRAIN_SAMPLE = USERS_ID_TRAIN + USERS_ID_TEST
USER_ID_TEST_SAMPLE = USERS_ID_TEST

# Optional : Save pickles
# P_TEST, X_TEST, Y_TEST = generate_test_features(USER_ID_TEST_SAMPLE)
# save_pickle('P_TEST', P_TEST)
# save_pickle('X_TEST', X_TEST)
# save_pickle('Y_TEST', Y_TEST)
# del P_TEST, X_TEST, Y_TEST
# 
# X_TRAIN, Y_TRAIN = generate_train_features(USERS_ID_TRAIN[:43736],
#                                            USERS_ID_TEST[:25000])
# save_pickle('X_TRAIN_1', X_TRAIN)
# save_pickle('Y_TRAIN_1', Y_TRAIN)
# del X_TRAIN, Y_TRAIN
# 
# X_TRAIN, Y_TRAIN = generate_train_features(USERS_ID_TRAIN[43736:87472],
#                                            USERS_ID_TEST[25000:50000])
# save_pickle('X_TRAIN_2', X_TRAIN)
# save_pickle('Y_TRAIN_2', Y_TRAIN)
# del X_TRAIN, Y_TRAIN
# 
# 
# X_TRAIN, Y_TRAIN = generate_train_features(USERS_ID_TRAIN[87472:],
#                                            USERS_ID_TEST[50000:])
# save_pickle('X_TRAIN_3', X_TRAIN)
# save_pickle('Y_TRAIN_3', Y_TRAIN)
# del X_TRAIN, Y_TRAIN
# 
# exit(0)

X_TRAIN_1 = load_pickle('X_TRAIN_1')
Y_TRAIN_1 = load_pickle('Y_TRAIN_1')
X_TRAIN_2 = load_pickle('X_TRAIN_2')
Y_TRAIN_2 = load_pickle('Y_TRAIN_2')
X_TRAIN_3 = load_pickle('X_TRAIN_3')
Y_TRAIN_3 = load_pickle('Y_TRAIN_3')

X_TRAIN = concatenate((X_TRAIN_1, X_TRAIN_2, X_TRAIN_3), axis=0)
Y_TRAIN = concatenate((Y_TRAIN_1, Y_TRAIN_2, Y_TRAIN_3), axis=0)
del X_TRAIN_1, X_TRAIN_2, X_TRAIN_3, Y_TRAIN_1, Y_TRAIN_2, Y_TRAIN_3

# Learning with XGBoost
CLS_XGB = train(X_TRAIN, Y_TRAIN)
verify(CLS_XGB)

# Learning with LGBoost
CLS_LGB = train(X_TRAIN, Y_TRAIN, enable_xgb=False)
verify(CLS_LGB, enable_xgb=False)

### Create submission
del X_TRAIN
del Y_TRAIN

P_TEST = load_pickle('P_TEST')
X_TEST = load_pickle('X_TEST')
Y_TEST = load_pickle('Y_TEST')

generate_csv([CLS_XGB, CLS_LGB], X_TEST, P_TEST, USER_ID_TEST_SAMPLE, mix='max')

# Display features importances
print '-----'
print 'XGB feature importance'
FEATS_IMP = (CLS_XGB.get_fscore())
FEATS_IMP_SORTED = sorted(FEATS_IMP, key=FEATS_IMP.get, reverse=True)
for r in FEATS_IMP_SORTED:
    print FEATURES.split(',')[int(r[1:])], FEATS_IMP[r]
print '-----'
print 'LGB feature importance'
FEATS_IMP = (CLS_LGB.feature_importance()).tolist()
INDICES = sorted(range(len(FEATS_IMP)), key=lambda k: FEATS_IMP[k], reverse=True)
for idx in INDICES:
    print '%s : %s' % (FEATURES.split(',')[idx], FEATS_IMP[idx])
print '-----'
