# Databricks notebook source
# DBTITLE 1,nutter tests
from runtime.nutterfixture import NutterFixture, tag
class MultiTestFixture(NutterFixture):
    # Asserting to have more than 0 records
    def assertion_records_more_than_zero(self):       
        record_count = spark.sql("""SELECT COUNT(*) AS total FROM bronze_weather.current""").collect()[0].total
        assert (record_count >= 1)

# COMMAND ----------

result = MultiTestFixture().execute_tests()
print(result.to_string())      

# COMMAND ----------

result.exit(dbutils)