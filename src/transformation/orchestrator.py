import sys
import os

# Ensure the project root is included in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
sys.path.append(project_root)

# Debugging: Verify sys.path configuration
if __name__ == "__main__":
    print(f"Current Working Directory: {os.getcwd()}")
    print(f"sys.path: {sys.path}")

# Direct imports from the project structure
from src.transformation.stock_metrics import calculate_and_save_stock_metrics
from src.transformation.sector_metrics import calculate_and_save_sector_metrics
from src.transformation.portfolio_metrics import calculate_and_save_portfolio_metrics
from src.transformation.market_metrics import calculate_and_save_market_metrics
import pandas as pd

if __name__ == "__main__":
    # Paths
    data_lake_path = "../data-lake/silver"
    analytics_path = os.path.join(data_lake_path, "analytics")

    # Stock Metrics
    calculate_and_save_stock_metrics(
        input_path=os.path.join(data_lake_path, "stocks/cleaned_sp500_stocks.csv"),
        save_path=os.path.join(analytics_path, "stock"),
        market_data=pd.read_csv(os.path.join(data_lake_path, "stocks/cleaned_sp500_index.csv"))
    )

    # Sector Metrics
    calculate_and_save_sector_metrics(
        stocks_path=os.path.join(data_lake_path, "stocks/cleaned_sp500_stocks.csv"),
        companies_path=os.path.join(data_lake_path, "stocks/cleaned_sp500_companies.csv"),
        save_path=os.path.join(analytics_path, "sector")
    )

    # Portfolio Metrics
    calculate_and_save_portfolio_metrics(
        stocks_path=os.path.join(data_lake_path, "stocks/cleaned_sp500_stocks.csv"),
        save_path=os.path.join(analytics_path, "portfolio")
    )

    # Market Metrics
    calculate_and_save_market_metrics(
        market_path=os.path.join(data_lake_path, "stocks/cleaned_sp500_index.csv"),
        save_path=os.path.join(analytics_path, "market")
    )
