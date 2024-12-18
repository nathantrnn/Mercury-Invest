import pandas as pd
import numpy as np
import os


def calculate_portfolio_volatility(weights, covariance_matrix):
    return np.sqrt(np.dot(weights.T, np.dot(covariance_matrix, weights)))


def calculate_sharpe_ratio(portfolio_returns, risk_free_rate, portfolio_volatility):
    return (portfolio_returns.mean() - risk_free_rate) / portfolio_volatility


def calculate_max_drawdown(df):
    cumulative = (1 + df).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak
    max_drawdown = drawdown.min()
    return max_drawdown


def calculate_and_save_portfolio_metrics(stocks_path, save_path):
    # Load data
    stocks_df = pd.read_csv(stocks_path)

    # Portfolio metrics
    portfolio_weights = np.array([0.1] * len(stocks_df["Symbol"].unique()))  # Example: Equal weights
    cov_matrix = stocks_df.pivot(index="Date", columns="Symbol", values="Daily Returns").cov()
    portfolio_volatility = calculate_portfolio_volatility(portfolio_weights, cov_matrix)

    portfolio_returns = stocks_df.groupby("Date")["Daily Returns"].mean()
    sharpe_ratio = calculate_sharpe_ratio(portfolio_returns, 0.02, portfolio_volatility)
    max_drawdown = calculate_max_drawdown(portfolio_returns)

    # Save results
    metrics = pd.DataFrame({
        "Portfolio Volatility": [portfolio_volatility],
        "Sharpe Ratio": [sharpe_ratio],
        "Max Drawdown": [max_drawdown]
    })
    metrics.to_csv(os.path.join(save_path, "portfolio_metrics.csv"), index=False)
