df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("user", user)
    .option("password", password)

    # Step 1: Create temp table
    .option("prepareQuery", """
        SELECT *
        INTO #temp_hist
        FROM IOT.dbo.mis_table
    """)

    # Step 2: Read temp table
    .option("query", "SELECT * FROM #temp_hist")

    .load()
)
