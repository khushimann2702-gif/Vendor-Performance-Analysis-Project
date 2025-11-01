import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

# ----------------------------
# Logging Configuration
# ----------------------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ----------------------------
# Create Vendor Summary
# ----------------------------
def create_vendor_summary(conn):
    """Merge different tables to get overall vendor summary."""
    vendor_sales_summary = pd.read_sql_query("""
        WITH FreightSummary AS (
            SELECT
                VendorNumber,
                SUM(Freight) AS FreightCost
            FROM vendor_invoice
            GROUP BY VendorNumber
        ),
        PurchaseSummary AS (
            SELECT
                p.VendorNumber,
                p.VendorName,
                p.Brand,
                p.Description,
                p.PurchasePrice,
                pp.Price AS ActualPrice, 
                pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp
                ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
        ),
        SalesSummary AS (
            SELECT
                VendorNo,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        )
        SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalExciseTax,
            fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary


# ----------------------------
# Clean Data Function
# ----------------------------
def clean_data(df):
    """Clean and enrich the vendor summary data."""
    # Change datatype to float
    df['Volume'] = df['Volume'].astype('float64')

    # Fill missing values
    df.fillna(0, inplace=True)

    # Strip spaces from text columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Create new derived columns
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'].replace(0, pd.NA)) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, pd.NA)
    df['SalesPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, pd.NA)

    # Replace infinities and NaNs after division
    df.replace([float('inf'), -float('inf')], 0, inplace=True)
    df.fillna(0, inplace=True)

    return df


# ----------------------------
# Main Execution
# ----------------------------
if __name__ == '__main__':
    try:
        # Connect to DB
        conn = sqlite3.connect('inventory.db')
        logging.info('Creating Vendor Summary Table.....')

        # Create summary
        summary_df = create_vendor_summary(conn)
        logging.info('Vendor Summary created successfully')
        logging.debug(summary_df.head())

        # Clean data
        logging.info('Cleaning Data.....')
        clean_df = clean_data(summary_df)
        logging.info('Data cleaned successfully')
        logging.debug(clean_df.head())

        # Ingest into DB
        logging.info('Ingesting data into database.....')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Data ingestion completed successfully.')

    except Exception as e:
        logging.exception("Error occurred during vendor summary ETL process.")
    finally:
        conn.close()
