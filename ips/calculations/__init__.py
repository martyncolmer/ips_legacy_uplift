import multiprocessing
import pandas as pd
import numpy as np


def parallelise_dataframe(df, func):
    num_cores = multiprocessing.cpu_count() - 1  # leave one free to not freeze machine
    num_partitions = num_cores  # number of partitions to split dataframe
    df_split = np.array_split(df, num_partitions)
    pool = multiprocessing.Pool(num_cores)

    df = pd.concat(pool.map(func, df_split))

    pool.close()
    pool.join()
    return df
