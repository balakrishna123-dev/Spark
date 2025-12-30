from file_readers import CSVReader, JSONReader, ParquetReader

def get_reader(file_type):

    readers = {
        "csv": CSVReader(),
        "json": JSONReader(),
        "parquet": ParquetReader()
    }

    if file_type not in readers:
        raise ValueError(f"Unsupported file type: {file_type}")

    return readers[file_type]
