#!/usr/bin/env python
#-*- coding: utf-8 -*-
""" Some graphs on the database """

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sys import argv

#########################################################
# Load data                                             #
#########################################################
def load_data():
    """
    Load data
    """
    nb_users = int(argv[1])
    if nb_users != 0:
        file_orders = '../Data/' + str(nb_users) + '_orders.csv'
        file_prior = '../Data/' + str(nb_users) + '_order_products__prior.csv'
        file_train = '../Data/' + str(nb_users) + 'order_products__train.csv'
    else:
        file_orders = '../Data/orders.csv'
        file_prior = '../Data/order_products__prior.csv'
        file_train = '../Data/order_products__train.csv'

    orders = pd.read_csv(file_orders, header=0)
    order_prior = pd.read_csv(file_prior, header=0)
    order_train = pd.read_csv(file_train, header=0)
    return nb_users, orders, order_prior, order_train


#########################################################
# General information on data                           #
#########################################################

def general_info(data_df):
    """ Print general information on the database
    - type of columns and number of non-null elements
    - first lines of the database  """
    # Print for each column the type and the number of non-null element
    print "##### Column infos with info #####"
    print data_df.info()
    print
    # Print first lines of each csv files
    print "##### First lines of the database #####"
    print data_df.head()

# general_info(orders)
# general_info(order_prior)
# general_info(order_main)

def list_user_id_per_eval_set(eval_set, orders):
    return orders.groupby('eval_set')['user_id'].unique()[eval_set]

def nb_user_id_per_eval_set(orders):
    print "Nb prior user_id: " + str(len(list_user_id_per_eval_set('prior', orders)))
    print "Nb train user_id: " + str(len(list_user_id_per_eval_set('train', orders)))
    print "Nb test user_id: " + str(len(list_user_id_per_eval_set('test', orders)))

def products_per_userid(user_id):
    list_orders = ORDERS[ORDERS['user_id']==user_id]['order_id']
    for order in list_orders:
        print "order_id " + str(order)
        print ORDER_PRIOR[ORDER_PRIOR['order_id'] == order]

# products_per_user_id(3)

#########################################################
# General graphs                                        #
#########################################################

def nb_orders():
    list_nb_orders = []
    for i in range(1, NB_USERS+1):
        list_nb_orders.append(max(ORDERS[ORDERS['user_id'] == i]['order_number']))
    plt.hist(list_nb_orders)
    plt.savefig("nb_orders.png")

def scatter_nb_products():
    prior_orders = ORDERS.dropna()[ORDERS['eval_set'] == 'prior']
    prior_orders = prior_orders.sample(n=1000)
    labels = ['nb_products', 'days_since_prior_order', 'order_dow', 'order_hour_of_day']
    data = []
    for index, row in prior_orders.iterrows():
        order_id = row['order_id']
        nb_products = max(ORDER_PRIOR[ORDER_PRIOR['order_id'] == order_id]['add_to_cart_order'])
        data.append((row['days_since_prior_order'], nb_products, row['order_dow'], row['order_hour_of_day']))
    df = pd.DataFrame.from_records(data, columns=labels)
    df.head()
    plt.clf()
    sns.pairplot(df, kind='scatter')
    plt.savefig("scatter_nb_products.png")

#########################################################
# Main                                                  #
#########################################################
NB_USERS, ORDERS, ORDER_PRIOR, ORDER_TRAIN = load_data()


# def split_df_according_to_col(data_df, col):
#     """ Split the database according to the value of one column """
#     list_df = []
#     values_col = list(data_df[col].unique())
#     for val in values_col:
#         list_df.append(data_df[data_df[col] == val])
#     returst_orders = ORDERS[ORDERS['user_id'==user_id]].['order_id']n list_df
# 
# # LIST_DF_TYPE = split_df_according_to_col(TRAIN_DF, "type")
# # for df in LIST_DF_TYPE:
# #     print df[COLUMNS_WITH_TYPE].head(50)
# 
# def save_several_boxplots(data_df, cols):
#     """ Create one boxplot graph per numerical column """
#     for col in cols:
#         plt.clf()
#         sns.boxplot(x="type", y=col, data=data_df)
#         sns.swarmplot(x="type", y=col, data=data_df, color=".25")
#         plt.savefig("Graphs/boxplot_" + col + ".png")
# 
# # save_several_boxplots(TRAIN_DF, COLUMNS_WO_TYPE)
# 
# def save_one_bloxplot(data_df, cols):
#     """ Create one big boxplot graph with all numerical columns """
#     plt.clf()
#     data_df_melt = pd.melt(data_df, id_vars="type", value_vars=cols)
#     sns.boxplot(x="variable", y="value", hue="type", data=data_df_melt)
#     plt.legend(loc='best')
#     plt.savefig("Graphs/boxplot_all.png")
# 
# # save_one_bloxplot(TRAIN_DF[COLUMNS_WITH_TYPE], COLUMNS_WO_TYPE)
# 
# def save_pairplot(data_df, cols):
#     """ Create a pairplot with all numerical columns """
#     plt.clf()
#     sns.pairplot(data_df, vars=cols, kind='scatter', diag_kind='kde',
#                  hue="type")
#     plt.savefig("Graphs/pairplot.png")
# 
# # save_pairplot(TRAIN_DF, COLUMNS_WO_TYPE)
# 
# def save_scatter_two_cols(data_df, col1, col2, labels):
#     """ Create one scatter graph with col1 and col2 """
#     plt.clf()
#     for key, val in data_df.groupby(labels):
#         plt.plot(val[col1], val[col2], label=key, linestyle="_", marker='.')
#     plt.xlabel(col1)
#     plt.ylabel(col2)
#     plt.legend(loc='best')
#     plt.savefig("Graphs/scatter-" + col1 + "-" + col2 + ".png")
# 
# # save_scatter_two_cols(TRAIN_DF, "hair_length", "has_soul", "type")
# 
# #########################################################
# # Modify string color columns                           #
# #########################################################
# 
# def add_color_int_col(data_df):
#     """ Create a new column into the database where colors are
#     replaced by intergers """
#     print "##### List of colors #####"
#     print data_df['color'].unique()
#     print
#     list_colors = list(data_df['color'].unique())
#     data_df['color_int'] = data_df['color'].map(list_colors.index)
#     return data_df, list_colors
# 
# # TRAIN_DF, LIST_COLORS = add_color_int_col(TRAIN_DF)
# # print LIST_COLORS
# # print TRAIN_DF.head(10)
# 
# def save_countplot_color(data_df):
#     """ Create a bar graph with color column """
#     plt.clf()
#     sns.countplot(x="color", hue="type", data=data_df)
#     plt.savefig("Graphs/countplot_color.png")
# 
# # save_countplot_color(TRAIN_DF)
