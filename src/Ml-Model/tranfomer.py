import joblib
import logging


logging.basicConfig(level=logging.INFO)


def tranfomer(df):
    df = df.sort_values(by="time_period_start", ascending=True)

    df.drop(columns=["time_open", "time_close"], inplace=True)

    df.reset_index(inplace=True, drop=True)

    numeric_cols = [
        "price_high",
        "price_low",
        "volume_traded",
        "trades_count",
        "fear_greed_value",
    ]

    scaler = joblib.load("scaler.pkl")

    df.drop(
        columns=["time_period_start", "time_period_end", "fear_greed_classification"],
        inplace=True,
    )

    X = df.drop(columns=["price_open", "price_close"])

    X[numeric_cols] = scaler.transform(X[numeric_cols])

    return X
