import duckdb

from lake import ACCESS_KEY, ENDPOINT, RAW_PREFIX, SECRET_KEY

con = duckdb.connect()   # in-memory, nothing to set up
con.execute("INSTALL httpfs")   # lets DuckDB read s3:// paths (downloaded once)
con.execute("LOAD httpfs")
con.execute(f"""
    CREATE SECRET minio (
        TYPE S3, KEY_ID '{ACCESS_KEY}', SECRET '{SECRET_KEY}',
        ENDPOINT '{ENDPOINT}', URL_STYLE 'path', USE_SSL false
    )
""")

# raw view: every Parquet object under every date= prefix, as one table
con.execute(f"""
    CREATE VIEW clicks AS
    SELECT * FROM read_parquet('s3://{RAW_PREFIX}/*/*.parquet', hive_partitioning = true)
""")

# deduped view: keep one row per event_id (delivery is at-least-once)
con.execute("""
    CREATE VIEW clicks_dedup AS
    SELECT * FROM clicks
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_time) = 1
""")


def show(title, sql):
    print(f"\n== {title}")
    con.sql(sql).show()


show("rows vs unique events (a gap means duplicates)", """
    SELECT count(*) AS total_rows, count(DISTINCT event_id) AS unique_events
    FROM clicks
""")

show("clicks per hour (in this machine's local timezone)", """
    SELECT date_trunc('hour', event_time) AS hour, count(*) AS clicks
    FROM clicks_dedup
    GROUP BY 1 ORDER BY 1
""")

show("button split", """
    SELECT button, count(*) AS clicks
    FROM clicks_dedup
    GROUP BY 1 ORDER BY 2 DESC
""")

show("sessions (a new session starts after 5 minutes of no clicks)", """
    WITH gaps AS (
        SELECT event_time - lag(event_time) OVER (ORDER BY event_time) AS gap
        FROM clicks_dedup
    )
    SELECT count(*) FILTER (WHERE gap IS NULL OR gap > INTERVAL 5 MINUTES) AS sessions
    FROM gaps
""")

show("top 5 hottest screen zones (screen split into an 8x4 grid)", """
    SELECT floor(x * 1.0 / screen_width * 8)::INT  AS col,
           floor(y * 1.0 / screen_height * 4)::INT AS row,
           count(*) AS clicks
    FROM clicks_dedup
    GROUP BY 1, 2 ORDER BY clicks DESC LIMIT 5
""")

show("data quality: clicks outside the reported screen size", """
    SELECT schema_version,
           count(*) AS out_of_bounds,
           max(x) AS max_x, any_value(screen_width) AS screen_w,
           max(y) AS max_y, any_value(screen_height) AS screen_h
    FROM clicks_dedup
    WHERE x >= screen_width OR y >= screen_height OR x < 0 OR y < 0
    GROUP BY schema_version
""")