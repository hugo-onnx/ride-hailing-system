import numpy as np
import pandas as pd
import xgboost as xgb


def evaluate(
    model: xgb.XGBRegressor, 
    val_df: pd.DataFrame, 
    feature_cols: list[str], 
    label_col: str
) -> dict:
    """
    Evaluate model performance on validation set.
    
    Args:
        model: Trained XGBoost model
        val_df: Validation dataset
        feature_cols: List of feature column names
        label_col: Label column name
    
    Returns:
        Dictionary with evaluation metrics
    """
    preds = model.predict(val_df[feature_cols])
    truth = val_df[label_col].values

    errors = np.abs(preds - truth)
    
    mae = np.mean(errors)
    p50 = np.percentile(errors, 50)
    p90 = np.percentile(errors, 90)
    p99 = np.percentile(errors, 99)
    mape = np.mean(errors / truth) * 100

    return {
        "mae_seconds": round(mae, 1),
        "mae_minutes": round(mae / 60, 2),
        "mape_percent": round(mape, 2),
        "p50_error_seconds": round(p50, 1),
        "p90_error_seconds": round(p90, 1),
        "p99_error_seconds": round(p99, 1),
        "n_samples": len(val_df),
    }