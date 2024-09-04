# PySpark Functions

Here’s a breakdown of common functions in PySpark that are useful across different domains, including data processing, transformation, and analysis. These functions are organized by category for easier reference.

### **1. General Functions**
- **`col("column_name")`**: Accesses a column.
- **`lit(value)`**: Creates a literal column, useful for adding constant values.
- **`alias("new_name")`**: Renames a column.
- **`select("col1", "col2", ...)`**: Selects specific columns.
- **`distinct()`**: Returns distinct rows.
- **`dropDuplicates(["col1", "col2", ...])`**: Drops duplicate rows based on specific columns.
- **`withColumnRenamed("old_name", "new_name")`**: Renames a column.

### **2. DataFrame Operations**
- **`filter("condition")`** or **`where("condition")`**: Filters rows based on a condition.
- **`orderBy("col1", "col2", ...)`**: Orders rows by specific columns.
- **`groupBy("col1", "col2", ...)`**: Groups rows based on column(s).
- **`agg({"col1": "agg_func", ...})`**: Aggregates after grouping.
- **`join(other_df, "column", "type")`**: Joins with another DataFrame (inner, outer, left, right).
- **`union(other_df)`**: Union two DataFrames (adds rows).
- **`withColumn("new_col", expr)`**: Adds or modifies a column.

### **3. Window Functions**
- **`Window.partitionBy("col1").orderBy("col2")`**: Defines a window specification for advanced analytics.
- **`row_number().over(window_spec)`**: Assigns row numbers within a window.
- **`rank().over(window_spec)`**: Assigns ranks within a window.
- **`dense_rank().over(window_spec)`**: Assigns dense ranks within a window.
- **`lag("col", offset, default)`**: Accesses a value from a previous row within the window.
- **`lead("col", offset, default)`**: Accesses a value from a subsequent row within the window.
- **`cume_dist().over(window_spec)`**: Calculates cumulative distribution.

### **4. String Functions**
- **`substring("col", pos, len)`**: Extracts a substring.
- **`concat_ws(delimiter, *cols)`**: Concatenates multiple columns with a delimiter.
- **`lower("col")` / `upper("col")`**: Converts string to lower/upper case.
- **`trim("col")` / `ltrim("col")` / `rtrim("col")`**: Trims spaces from both/left/right sides.
- **`regexp_extract("col", pattern, group)`**: Extracts a substring using a regex pattern.
- **`regexp_replace("col", pattern, replacement)`**: Replaces occurrences of a regex pattern with a string.
- **`translate("col", src_chars, dst_chars)`**: Replaces characters in a string.

### **5. Date and Time Functions**
- **`current_date()`**: Returns the current date.
- **`current_timestamp()`**: Returns the current timestamp.
- **`date_add("start_date", days)`**: Adds days to a date.
- **`date_sub("start_date", days)`**: Subtracts days from a date.
- **`datediff("end_date", "start_date")`**: Returns the number of days between two dates.
- **`to_date("timestamp_col")`**: Converts a timestamp to a date.
- **`to_timestamp("string_col")`**: Converts a string to a timestamp.
- **`year("col")` / `month("col")` / `dayofmonth("col")`**: Extracts year, month, or day.
- **`hour("col")` / `minute("col")` / `second("col")`**: Extracts hour, minute, or second.
- **`unix_timestamp("col")`**: Converts to Unix timestamp.

### **6. Aggregate Functions**
- **`count("col")`**: Counts non-null values.
- **`sum("col")`**: Sums values.
- **`avg("col")`**: Computes the average.
- **`min("col")`**: Finds the minimum value.
- **`max("col")`**: Finds the maximum value.
- **`stddev("col")`**: Computes the standard deviation.
- **`variance("col")`**: Computes the variance.

### **7. Null Handling Functions**
- **`isnull("col")`**: Checks if a column value is null.
- **`isnan("col")`**: Checks if a column value is NaN.
- **`fillna(value, subset)`**: Replaces null values with a specified value.
- **`dropna(how, subset)`**: Drops rows with null values (how: 'any' or 'all').

### **8. JSON Functions**
- **`from_json("json_col", schema)`**: Parses a JSON string and infers schema.
- **`to_json("col")`**: Converts a column to a JSON string.
- **`get_json_object("json_col", "path")`**: Extracts a specific part of a JSON string.

### **9. Array Functions**
- **`array("col1", "col2", ...)`**: Creates an array from columns.
- **`explode("array_col")`**: Expands an array into multiple rows.
- **`array_contains("array_col", value)`**: Checks if an array contains a value.
- **`size("array_col")`**: Returns the size of an array.

### **10. Conditional Functions**
- **`when(condition, value).otherwise(other_value)`**: Creates a conditional column.
- **`coalesce("col1", "col2", ...)`**: Returns the first non-null value.
- **`ifnull("col1", "col2")`**: Returns `col1` if it's not null, otherwise returns `col2`.
- **`nvl("col1", "col2")`**: Same as `ifnull`.

### **11. User-Defined Functions (UDFs)**
- **`udf(lambda x: some_function(x), returnType)`**: Creates a user-defined function.
- **`pandas_udf(pandas_series_func, returnType)`**: Creates a Pandas UDF for vectorized operations.

### **12. DataFrame Methods**
- **`show(n=20, truncate=True)`**: Displays the top `n` rows of the DataFrame.
- **`count()`**: Returns the number of rows in the DataFrame.
- **`printSchema()`**: Prints the schema of the DataFrame.
- **`cache()`**: Caches the DataFrame in memory.
- **`persist(storageLevel)`**: Persists the DataFrame with a specific storage level (e.g., memory, disk).
- **`unpersist()`**: Removes the DataFrame from the cache.

### **13. Machine Learning (MLlib)**
- **`VectorAssembler()`**: Converts columns into a single vector column.
- **`StandardScaler()`**: Standardizes features by removing the mean and scaling to unit variance.
- **`StringIndexer()`**: Converts string labels into indices.
- **`OneHotEncoder()`**: Converts indices into one-hot encoded vectors.
- **`KMeans()`**: K-means clustering.
- **`LinearRegression()`**: Performs linear regression.
- **`RandomForestClassifier()`**: Random forest classifier.

### **14. SQL Functions**
- **`spark.sql("SQL QUERY")`**: Executes SQL queries on DataFrames registered as temporary views.
- **`createOrReplaceTempView("view_name")`**: Creates or replaces a temporary view for SQL queries.
- **`sqlContext.sql("SQL QUERY")`**: Executes SQL queries on a DataFrame (alternative to `spark.sql`).

### **15. I/O Functions**
- **`read.format("format").load("path")`**: Reads data from a specific format (e.g., CSV, JSON, Parquet).
- **`write.format("format").save("path")`**: Writes data to a specific format (e.g., CSV, JSON, Parquet).
- **`saveAsTable("table_name")`**: Saves the DataFrame as a Hive table.
- **`read.jdbc(url, table, properties)`**: Reads data from a JDBC source.
- **`write.jdbc(url, table, properties)`**: Writes data to a JDBC source.

These functions provide a comprehensive toolkit for working with PySpark, covering a wide range of data processing tasks, including ETL, data analysis, and machine learning.
