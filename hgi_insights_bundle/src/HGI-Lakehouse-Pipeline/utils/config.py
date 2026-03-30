import os

class DataLakeConfig:
    def __init__(self, source_system: str = None, customer_id: str = None, object_name: str = None, load_type: str = None):
        # Base bucket (Single source of truth for the environment)
        self.bucket_path = "s3://hgi-databricks-data-lakehouse-dev/"
        
        # Bronze Dynamic Paths
        if source_system and customer_id and object_name and load_type:
            self.landing_zone_path = f"{self.bucket_path}landing-zone/{source_system}/{customer_id}/{object_name}/{load_type}/"
            self.checkpoint_path = f"{self.bucket_path}layers/bronze/checkpoints/{load_type}/{source_system}/{customer_id}/{object_name}/"
            self.schema_path = f"{self.checkpoint_path}_schema/"
            
        # Silver Base Path
        self.silver_base_checkpoint_dir = f"{self.bucket_path}layers/silver/checkpoints/"

    # Universal helper for Landing Zone paths with YYYY/MM/DD/HH partitioning
    def get_landing_zone_timestamped_path(self, source_sys: str, cust_id: str, obj_name: str, load_type: str, ts_now):
        """
        Generates a path like: s3://bucket/landing-zone/source/cust/obj/incremental/2024/05/20/14
        Works for both 'historical' and 'incremental' load_type.
        """
        yyyy = ts_now.strftime("%Y")
        mm = ts_now.strftime("%m")
        dd = ts_now.strftime("%d")
        hh = ts_now.strftime("%H")
        return f"{self.bucket_path}landing-zone/{source_sys}/{cust_id}/{obj_name}/{load_type}/{yyyy}/{mm}/{dd}/{hh}"

    # Helper method for Silver notebook loops
    def get_silver_checkpoint_path(self, source_sys: str, cust_id: str, obj_name: str):
        return f"{self.silver_base_checkpoint_dir}{source_sys}/{cust_id}/{obj_name}/"