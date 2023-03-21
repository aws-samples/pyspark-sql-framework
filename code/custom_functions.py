from pyspark.sql.types import StringType, MapType, ArrayType, IntegerType
from pyspark.sql import SparkSession
import json
from  custom_logger import CustomLogger


def convert_json_to_map(col):
  if col is None:
      return None
  return json.loads(col)


def convert_map_to_json(col):
    sorted_dict = {}
    if col is None:
        return None
    for i in sorted (col.keys()) :
        sorted_dict[i] = col[i]
    return json.dumps(sorted_dict)

def convert_array_as_string_array(col):
    import json
    if not col:
        return []
    elif col.startswith('['):
        return json.loads(col)
    else:
        return json.loads('[' + col + ']')
    

def register_functions():
    log = CustomLogger().adapter
    log.info("Registering Custom Python UDF's")
    spark = SparkSession.builder.getOrCreate()
    spark.udf.register("convert_json_to_map", convert_json_to_map, MapType(StringType(),StringType()))
    spark.udf.register("convert_map_to_json", convert_map_to_json, StringType())
    spark.udf.register("convert_array_as_string_array", convert_array_as_string_array, ArrayType(IntegerType()))
    log.info("Registering the functions completed!!")