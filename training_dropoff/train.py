import joblib
import pandas as pd
import xgboost as xgb

from pathlib import Path

from data.schema import FEATURE_COLUMNS, LABEL_COLUMN


def train_dropoff_model(
    train_df: pd.DataFrame, 
    val_df: pd.DataFrame, 
    output_path: str
) -> xgb.XGBRegressor:
    """
    Train XGBoost dropoff ETA model.
    
    Args:
        train_df: Training dataset
        val_df: Validation dataset
        output_path: Path to save trained model
    
    Returns:
        Trained XGBRegressor model
    """
    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[LABEL_COLUMN]

    X_val = val_df[FEATURE_COLUMNS]
    y_val = val_df[LABEL_COLUMN]

    model = xgb.XGBRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42,
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, output_path)
    print(f"Model saved to {output_path}")
    
    return model