# %%
import pandas as pd
import glob
import time
import duckdb
# %%
conn = duckdb.connect() # create an in-memory database
# %%
# with pandas
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob('dataset/*.csv')])
print(f"time: {(time.time() - cur_time)}")
print(df.head(10))
# %%
# with DuckDb you can process data faster than pandas
cur_time = time.time()
df = conn.execute("""
                    SELECT * 
                    FROM read_csv_auto('dataset/*.csv', header=True)
                    LIMIT 10
                  """).df()
print(f"time: {(time.time() - cur_time)}")
print(df)               
# %%
df = conn.execute("""
                    SELECT *
                    FROM read_csv_auto('dataset/*.csv', header=True)
                  """).df()
conn.register("df_view", df)
conn.execute("DESCRIBE df_view").df()
# %%
conn.execute("SELECT COUNT(*) FROM df_view").df()

# %%
df.isnull().sum()
df = df.dropna(how='all')
# %%
# with duckDB you can run SQL queries on top of pandas data frame
conn.execute("SELECT COUNT(*) FROM df").df()
# %%
conn.execute("""SELECT * FROM df WHERE "Order ID"='295667'""").df()
# %%
## Views are great for abstracting the complexity of the underlying tables they reference
conn.execute("""
                CREATE OR REPLACE TABLE sales AS
                    SELECT
                        "order ID"::INTEGER AS order_id,
                        Product AS product,
                        "Quantity Ordered"::Integer AS quantity,
                        "Price Each"::DECIMAL AS price_each,
                        strptime("Order Date", '%m/%d/%Y %H:%M')::Date as order_date,
                        "Purchase Address" AS purchase_address
                    FROM df
                    WHERE
                        TRY_CAST("Order ID" AS INTEGER) NOTNULL
             """)
# %%
conn.execute("FROM sales").df()
# %%
# Exclude
conn.execute("""
            SELECT 
                * EXCLUDE (product, order_date, purchase_address)
            FROM sales
            """).df()
# %%
# columns expression (min value for order_id, quantity and price)
conn.execute("""
            SELECT 
                MIN(COLUMNS(* EXCLUDE (product, order_date, purchase_address)))
            FROM sales
            """).df()
# %%
# Since VIEWS are recreated each time a query reference them,
# if new data is added to the sales table,
# the VIEW gets updated as well
conn.execute("""
CREATE OR REPLACE VIEW aggregated_sales AS
    SELECT
        order_id,
        COUNT(1) as nb_orders,
        MONTH(order_date) as month,
        str_split(purchase_address, ',')[2] AS city,
        SUM(quantity * price_each) AS revenue
    FROM sales
    GROUP BY ALL
""")
# %%
conn.execute("FROM aggregated_sales").df()
# %%
conn.execute("""
                SELECT
                    city,
                    SUM(revenue) as total
                FROM aggregated_sales
                GROUP BY city
                ORDER BY total DESC
             """).df()
# %%
# parquet format files takes less space on disk and faster to process
conn.execute("COPY (FROM aggregated_sales) TO 'aggregated_sales.parquet' (FORMAT 'parquet')" )
# %%
conn.execute("FROM aggregated_sales.parquet").df()
# %%
