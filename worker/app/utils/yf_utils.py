from typing import cast

import pandas as pd


def normalize_download(tickers: list[str], df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Normalize yf.download result into {symbol: DataFrame}.

    yf.download returns flat columns for 1 ticker but MultiIndex for 2+.
    This normalizes both cases into a consistent dict.
    """
    if len(tickers) == 1:
        return {tickers[0]: df}
    return {
        sym: cast(pd.DataFrame, df[sym])
        for sym in tickers
        if sym in df.columns.get_level_values(0)
    }
