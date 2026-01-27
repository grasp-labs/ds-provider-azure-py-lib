# temporary file
import pandas as pd


def concat(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    list of DataFrames to concatenate.
    :param dfs: DataFrames to concatenate.
    :return: Concatenated DataFrame
    """
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)
