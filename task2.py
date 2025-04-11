from pyspark.sql import SparkSession


# Создадим сессию Product_Category
spark = SparkSession.builder.appName("Product_Category").getOrCreate()

# Создадим датафрейм products (продукты)
products = spark.createDataFrame([
    {"productId": 1, "productName": "Продукт 1"},
    {"productId": 2, "productName": "Продукт 2"},
    {"productId": 3, "productName": "Продукт 3"},
    {"productId": 4, "productName": "Продукт 4"},
])

# Создадим датафрейм categories (категории)
categories = spark.createDataFrame([
    {"categoryId": 1, "categoryName": "Категория 1"},
    {"categoryId": 2, "categoryName": "Категория 2"},
    {"categoryId": 3, "categoryName": "Категория 3"},
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
categories.createOrReplaceTempView("categories")
categories.show()
product_categories.createOrReplaceTempView("product_categories")
product_categories.show()

# Напишем SQL-запрос для создания нового датафрейма final_df
final_df = spark.sql("""
    SELECT p.productName, c.categoryName
    FROM products p
    LEFT JOIN product_categories pc ON p.productId = pc.productId
    LEFT JOIN categories c ON pc.categoryId = c.categoryId
""")

# Отобразим конечный датафрейм
final_df.show()
