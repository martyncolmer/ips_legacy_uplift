import multiprocessing

import numpy as np
import pandas as pd


def parallelise_dataframe(df, func):
    num_partitions = multiprocessing.cpu_count()
    df_split = np.array_split(df, num_partitions)
    pool = multiprocessing.Pool(num_partitions)

    df = pd.concat(pool.map(func, df_split))

    pool.close()
    pool.join()
    return df
