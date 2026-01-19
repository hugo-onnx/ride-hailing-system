import pandas as pd


def time_split(df: pd.DataFrame, split_ratio: float = 0.8) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split dataset by time (preserves temporal order).
    
    Args:
        df: DataFrame sorted by time
        split_ratio: Fraction of data for training (default 0.8)
    
    Returns:
        Tuple of (train_df, val_df)
    """
    split_idx = int(len(df) * split_ratio)
    train = df.iloc[:split_idx]
    val = df.iloc[split_idx:]
    return train, val