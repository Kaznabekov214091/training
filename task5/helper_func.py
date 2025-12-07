import pandas as pd
import numpy as np
from scipy import stats


def compute_stats(df, iqr_mult=1.5, z_thresh=3, ma_window=5, ma_percent=20, grubbs_alpha=0.05):
    numeric_cols = df.columns[1:]
    outlier_data = {}

    for mine in numeric_cols:
        series = df[mine].dropna()

        # Basic stats
        mean = series.mean()
        std = series.std()
        median = series.median()
        q1, q3 = np.percentile(series, [25, 75])
        iqr = q3 - q1
        total_output = series.sum()

        # Detect outliers
        iqr_out = detect_iqr_outliers(series, multiplier=iqr_mult)
        z_out = detect_zscore_outliers(series, threshold=z_thresh)
        ma_out = detect_moving_avg_deviation(series, window=ma_window, percent_threshold=ma_percent)
        grubbs_out = grubbs_test(series, alpha=grubbs_alpha)

        # Store all stats and outliers
        outlier_data[mine] = {
            "Mean": mean,
            "Std Dev": std,
            "Median": median,
            "IQR": iqr,
            "Total": total_output,
            "IQR_Outliers": iqr_out.tolist() if hasattr(iqr_out, 'tolist') else iqr_out,
            "Zscore_Outliers": z_out.tolist() if hasattr(z_out, 'tolist') else z_out,
            "MA_Outliers": ma_out.tolist() if hasattr(ma_out, 'tolist') else ma_out,
            "Grubbs_Outliers": grubbs_out.tolist() if hasattr(grubbs_out, 'tolist') else grubbs_out,
            "IQR_Outliers_Count": int(iqr_out.sum()) if hasattr(iqr_out, 'sum') else int(sum(iqr_out)),
            "Zscore_Outliers_Count": int(z_out.sum()) if hasattr(z_out, 'sum') else int(sum(z_out)),
            "MA_Outliers_Count": int(ma_out.sum()) if hasattr(ma_out, 'sum') else int(sum(ma_out)),
            "Grubbs_Outliers_Count": int(grubbs_out.sum()) if hasattr(grubbs_out, 'sum') else int(sum(grubbs_out))
        }

    return outlier_data



def detect_iqr_outliers(series, multiplier=1.5):
    s = series.dropna()
    q1, q3 = np.percentile(s, [25, 75])
    iqr = q3 - q1
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    return (series < lower) | (series > upper)


def detect_zscore_outliers(series, threshold=3):
    z = np.abs(stats.zscore(series, nan_policy='omit'))
    return z > threshold


def detect_moving_avg_deviation(series, window=5, percent_threshold=20):
    ma = series.rolling(window=window).mean()
    deviation = np.abs(series - ma) / ma * 100
    return deviation > percent_threshold


def grubbs_test(series, alpha=0.05):
    s = series.copy()
    mask = pd.Series(False, index=s.index)
    while True:
        mean = s.mean()
        std = s.std()
        n = len(s)
        if n < 3:
            break
        g = np.max(np.abs(s - mean)) / std
        t_crit = stats.t.ppf(1 - alpha / (2 * n), n - 2)
        numerator = (n - 1) * np.sqrt(t_crit ** 2)
        denominator = np.sqrt(n) * np.sqrt(n - 2 + t_crit ** 2)
        g_crit = numerator / denominator
        if g > g_crit:
            outlier_index = np.argmax(np.abs(s - mean))
            mask.iloc[outlier_index] = True
            s = s.drop(s.index[outlier_index])
        else:
            break
    return mask
