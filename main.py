from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
from IPython import embed
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def concat_files(input_dir, output_dir, filename):
    commands = []
    commands.append("head -n1 {0}/part-00000* > {1}/{2}.csv".format(input_dir, output_dir, filename))
    commands.append("for filename in $(ls {0}/part-*); do sed 1d $filename >> {1}/{2}.csv; done".format(input_dir, output_dir, filename))
    commands.append("rm -rf {0}".format(input_dir))
    for command in commands:
        os.system(command)

class Wizengamot:
    def __init__(self, old_file, new_file, output_directory):
        self.spark = SparkSession.builder.appName("Wizengamot").getOrCreate()
        self.old_file_df = self.spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").csv(old_file)
        self.new_file_df = self.spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").csv(new_file)
        self.output_directory = output_directory
        self.obtain_district_audits()

    def obtain_district_audits(self):
        logger.info("obtaining district audits.")
        audited_old_file = self.audit_districts(self.old_file_df, 'old').na.fill({'value': 'NULL'})
        audited_new_file = self.audit_districts(self.new_file_df, 'new').na.fill({'value': 'NULL'})
        join_conditions = ["registered_address__county", "district_type", "value"]
        joined_audit_report = audited_new_file.join(audited_old_file, join_conditions, 'outer').na.fill({'new_district_count': 0, 'old_district_count': 0})
        joined_audit_report.write.option("header", "true").csv("output/audit_report")
        concat_files("output/audit_report", self.output_directory, "audit_report")

    @staticmethod
    def audit_districts(df, new_file):
        districts = ["federal_district", "state_upper_district", "state_lower_district", "city_district",  "city_sub_district", "ward", "county_district", "party", "is_active_voter"]
        dfs = []
        for district in districts:
            current_frame = df.groupBy("registered_address__county", district).count().orderBy("registered_address__county")
            renamed_frame = current_frame.withColumnRenamed('count', '%s_district_count' % new_file).withColumnRenamed(district, 'value').withColumn("district_type", lit(district))
            dfs.append(renamed_frame)
        last_df = dfs.pop()
        for frame in dfs:
            last_df = last_df.union(frame)
        return last_df
