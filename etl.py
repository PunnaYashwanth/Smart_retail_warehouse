
import pandas as pd
import json
import sqlite3
import re
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"

positive_words = {"good","excellent","love","fresh","best","great","happy","value","loved","freshness"}
negative_words = {"bad","poor","worst","disappointed","salty","not","stale","expired"}

def simple_sentiment(text):
    if pd.isna(text):
        return "neutral"
    text_clean = re.sub(r'[^a-zA-Z0-9 ]', ' ', str(text).lower())
    tokens = text_clean.split()
    pos = sum(1 for t in tokens if t in positive_words)
    neg = sum(1 for t in tokens if t in negative_words)
    if pos > neg: return "positive"
    if neg > pos: return "negative"
    return "neutral"

def extract(target_type=None):
    # Read Superstore CSV (structured)
    csv_path = DATA_DIR / "Sample - Superstore.csv"
    if (not target_type or "csv" in target_type.lower()) and csv_path.exists():
        df = pd.read_csv(csv_path, encoding='latin1', low_memory=False)
    else:
        df = pd.DataFrame() # Return empty if not requested or not found

    # Read orders.json (semi-structured)
    json_path = DATA_DIR / "orders.json"
    if (not target_type or "json" in target_type.lower()) and json_path.exists():
        with open(json_path, 'r', encoding='utf-8', errors='ignore') as f:
            orders = json.load(f)
    else:
        orders = []

    # Read reviews.txt (unstructured)
    reviews_path = DATA_DIR / "reviews.txt"
    reviews = []
    if (not target_type or "txt" in target_type.lower()) and reviews_path.exists():
        with open(reviews_path, 'r', encoding='latin1', errors='ignore') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if line:
                    reviews.append({"review_id": i+1, "review_text": line})
    
    return df, orders, pd.DataFrame(reviews)

def transform(df, orders, reviews):
    # Normalize dates and ids
    if "Order Date" in df.columns:
        df["Order Date"] = pd.to_datetime(df["Order Date"], errors='coerce')
    if "Row ID" in df.columns:
        df["Sale_ID"] = df["Row ID"].astype(str)
    # Products dimension
    products_cols = [c for c in ["Product ID","Product Name","Category","Sub-Category"] if c in df.columns]
    products = df[products_cols].drop_duplicates().rename(columns={
        "Product ID":"product_id","Product Name":"product_name","Category":"category","Sub-Category":"subcategory"
    }) if products_cols else pd.DataFrame(columns=["product_id","product_name","category","subcategory"])
    # Customers dimension
    cust_cols = [c for c in ["Customer ID","Customer Name","City","State","Region"] if c in df.columns]
    customers = df[cust_cols].drop_duplicates().rename(columns={
        "Customer ID":"customer_id","Customer Name":"customer_name","City":"city","State":"state","Region":"region"
    }) if cust_cols else pd.DataFrame(columns=["customer_id","customer_name","city","state","region"])
    # Sales fact
    sales = df.rename(columns={
        "Order ID":"order_id","Customer ID":"customer_id","Product ID":"product_id",
        "Order Date":"sale_date","Quantity":"quantity","Sales":"sales","Profit":"profit"
    })
    # Orders: flatten semi-structured orders into DataFrame
    order_rows = []
    for o in orders or []:
        for it in o.get("items", []):
            order_rows.append({
                "order_id": o.get("order_id"),
                "customer_id": o.get("customer_id"),
                "order_date": o.get("order_date"),
                "product_id": it.get("product_id"),
                "qty": it.get("qty"),
                "platform": o.get("platform")
            })
    orders_df = pd.DataFrame(order_rows)
    if not orders_df.empty:
        orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], errors='coerce')
    # Sentiment on reviews
    if not reviews.empty:
        reviews = reviews.copy()
        reviews["sentiment"] = reviews["review_text"].apply(simple_sentiment)
        reviews["review_date"] = datetime.now().strftime("%Y-%m-%d")
    # Aggregation summary
    if "product_id" in sales.columns:
        agg = sales.groupby("product_id", dropna=False).agg(
            total_qty=pd.NamedAgg(column="quantity", aggfunc="sum"),
            total_sales=pd.NamedAgg(column="sales", aggfunc="sum")
        ).reset_index()
    else:
        agg = pd.DataFrame(columns=["product_id","total_qty","total_sales"])
    return sales, products, customers, orders_df, reviews, agg

def load_to_dw(sales, products, customers, orders_df, reviews, sales_agg, db_path=None):
    db_path = db_path or (Path(__file__).resolve().parent / "warehouse.db")
    conn = sqlite3.connect(db_path)
    # Write tables
    if not products.empty:
        products.to_sql("dim_product", conn, if_exists="replace", index=False)
    if not customers.empty:
        customers.to_sql("dim_customer", conn, if_exists="replace", index=False)
    # Dates dimension from sales.sale_date
    if "sale_date" in sales.columns and not sales.empty:
        dates = pd.DataFrame({"sale_date": pd.to_datetime(sales["sale_date"], errors='coerce').dt.date.unique()})
        dates = dates.dropna().reset_index(drop=True)
        if not dates.empty:
            dates["date_id"] = dates["sale_date"].astype(str)
            dates["year"] = pd.to_datetime(dates["sale_date"]).dt.year
            dates["month"] = pd.to_datetime(dates["sale_date"]).dt.month
            dates["day"] = pd.to_datetime(dates["sale_date"]).dt.day
            dates.to_sql("dim_date", conn, if_exists="replace", index=False)
    # Reviews, sales facts, orders
    if not reviews.empty:
        reviews.to_sql("dim_review", conn, if_exists="replace", index=False)
    if not sales.empty:
        sales.to_sql("fact_sales", conn, if_exists="replace", index=False)
    if not orders_df.empty:
        orders_df.to_sql("fact_orders", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()

def export_outputs(sales_agg, reviews, out_dir=None):
    out_dir = Path(out_dir or Path(__file__).resolve().parent)
    sales_agg.to_csv(out_dir / "output_product_sales_summary.csv", index=False)
    if not reviews.empty:
        reviews[["review_id","review_text","sentiment","review_date"]].to_csv(out_dir / "output_reviews_sentiment.csv", index=False)

if __name__ == "__main__":
    df, orders, reviews = extract()
    sales, products, customers, orders_df, reviews, sales_agg = transform(df, orders, reviews)
    load_to_dw(sales, products, customers, orders_df, reviews, sales_agg)
    export_outputs(sales_agg, reviews)
