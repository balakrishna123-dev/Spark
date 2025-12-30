import json
from spark_session import get_spark_session
from reader_factory import get_reader

# Load config
with open("config/config.json", "r") as f:
    config = json.load(f)

# Create Spark session
spark = get_spark_session(config["app_name"])

# Normalize Windows path
input_path = config["input_path"].replace("\\", "/")

# Get reader based on file type
reader = get_reader(config["file_type"])

# Read data
df = reader.read(
    spark,
    input_path,
    config.get("options", {})
)

# Basic validation
df.printSchema()
df.show(5)

spark.stop()
