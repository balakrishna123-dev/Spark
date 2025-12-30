class CSVReader:
    def read(self, spark, path, options):
        return spark.read.options(**options).csv(path)


class JSONReader:
    def read(self, spark, path, options):
        return spark.read.options(**options).json(path)


class ParquetReader:
    def read(self, spark, path, options):
        return spark.read.parquet(path)


