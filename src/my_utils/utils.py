# encoding: utf-8

import numpy as np
from scipy.stats import boxcox
from scipy.special import inv_boxcox
# Apply Box-Cox in a number and scale it to [0..1]

def bc_n_scale(number, lbd, mib, mia, maa):
    
    number = np.float(number)
    lbd = np.float(lbd)
    mib = np.float(mib)
    mia = np.float(mia)
    maa = np.float(maa)
    
    # box-cox only for positive numbers
    if mib <= 0:
        number = number - mib + 1
        
    # box-cox
    if lbd == 0:
        number = np.log(number)
    else:
        number = (number**lbd - 1) / np.float(lbd)
        
    # scale
    number = (number - mia) / np.float(maa - mia)
    
    return number


# Apply Box-Cox in a list and scale it to [0..1]
def bc_n_scale_list(ls):
    mib = np.min(ls)

    if mib <= 0:
        ls = [l - mib + 1 for l in ls]
        
    ls, lbd = boxcox(ls)
    
    mia = np.min(ls)
    maa = np.max(ls)
    
    ls = [(l - mia) / np.float(maa - mia) for l in ls]
    
    return [ls, lbd, mib, mia, maa]


# invert Scale and Box-Cox from a list
def inv_bc_scale(number, lbd, mib, mia, maa):
    
    n = number * (maa - mia) + mia

    n = inv_boxcox(n, lbd)
    
    if mib <= 0:
        n = n + mib - 1

    return n


# invert Scale and Box-Cox from a list
def inv_bc_scale_list(ls, lbd, mib, mia, maa):
    
    ls = [l * (maa - mia) + mia for l in ls]

    ls = inv_boxcox(ls, lbd)
    
    if mib <= 0:
        ls = [l + mib - 1 for l in ls]

    return ls


def do_spark_query(spark, query, path='../data/cache/', do_cache=True):
    import hashlib, os, shutil

    file_name = hashlib.md5(query).hexdigest()
    
    has_folder = os.path.exists(path + file_name)
    
    if has_folder and do_cache == True:
        df = spark.read.format('csv') \
                  .option('sep', ',') \
                  .option('header', 'True') \
                  .option('inferSchema', True) \
                  .load(path + file_name + '/*.csv')
    else:
        df = spark.sql(query)
        if has_folder:
            shutil.rmtree(path + file_name)
        df.repartition(1).write.csv(path + file_name, header=True)
    
    return df