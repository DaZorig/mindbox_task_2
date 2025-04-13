from pyspark.sql import SparkSession


# Создадим сессию Product_Category
spark = SparkSession.builder.appName("Product_Category").getOrCreate()

# Создадим датафрейм products (продукты)
products = spark.createDataFrame([
    {"productId": 1, "productName": "Product A"},
    {"productId": 2, "productName": "Product B"},
    {"productId": 3, "productName": "Product C"},
    {"productId": 4, "productName": "Product D"},
])

# Создадим датафрейм categories (категории)
categories = spark.createDataFrame([
    {"categoryId": 1, "categoryName": "Category X"},
    {"categoryId": 2, "categoryName": "Category Y"},
    {"categoryId": 3, "categoryName": "Category Z"},
])

# Создадим датафрейм product_categories (связи между продуктами и категориями)
product_categories = spark.createDataFrame([
    {"productId": 1, "categoryId": 1},
    {"productId": 1, "categoryId": 2},
    {"productId": 2, "categoryId": 2},
    {"productId": 3, "categoryId": 3},
])

# Создадим и отобразим представление для каждого датафрейма
products.createOrReplaceTempView("products")
products.show()

# Products
# ---------+-----------+
# |productId|productName|
# +---------+-----------+
# |        1|  Product A|
# |        2|  Product B|
# |        3|  Product C|
# |        4|  Product D|
# +---------+-----------+

categories.createOrReplaceTempView("categories")
categories.show()

# Categories
# +----------+------------+
# |categoryId|categoryName|
# +----------+------------+
# |         1|  Category X|
# |         2|  Category Y|
# |         3|  Category Z|
# +----------+------------+

product_categories.createOrReplaceTempView("product_categories")
product_categories.show()

# Product_categories
# +----------+---------+
# |categoryId|productId|
# +----------+---------+
# |         1|        1|
# |         2|        1|
# |         2|        2|
# |         3|        3|
# +----------+---------+

# Напишем SQL-запрос для создания нового датафрейма final_df
final_df = spark.sql("""
    SELECT p.productName, c.categoryName
    FROM products p
    LEFT JOIN product_categories pc ON p.productId = pc.productId
    LEFT JOIN categories c ON pc.categoryId = c.categoryId
""")

# Отобразим конечный датафрейм
final_df.show()

# +-----------+------------+
# |productName|categoryName|
# +-----------+------------+
# |  Product D|        null|
# |  Product A|  Category X|
# |  Product C|  Category Z|
# |  Product A|  Category Y|
# |  Product B|  Category Y|
# +-----------+------------+