import argparse
import logging

from src.ingestion import IngestionHandler
from src.loader import DataLoader
from src.transformation import TransformationHandler
from src.validation import ValidationHandler

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

class AgriculturalMonitoringPipeline:
    def __init__(self):
        self.ingestion_handler = IngestionHandler()

    def run(self, action, file=None):
        if action == "ingest":

            logging.info("-----------------------------")
            df, processed_files = self.ingestion_handler.load_files()
            logging.info("-----------------------------\n\n")

            if not df.empty:
                logging.info("-----------------------------")
                transformer = TransformationHandler()
                transformed_df = transformer.transform(df)
                logging.info("-----------------------------\n\n")

                logging.info("-----------------------------")
                validator = ValidationHandler()
                validator.run_validations(transformed_df)
                logging.info("-----------------------------\n\n")

                logging.info("-----------------------------")
                loader = DataLoader()
                loader.save_parquet_partitioned(transformed_df, processed_files)
                logging.info("-----------------------------\n\n")

                logging.info(f"Ingestion pipeline completed.")
            else:
                logging.warning("Ingested DataFrame is empty.")
        elif action == "inspect":
            self.ingestion_handler.inspect_file_schema(file)
        else:
            logging.error("Invalid action specified.")

if __name__ == "__main__":
    """
    Start the ingestion pipeline
    """
    parser = argparse.ArgumentParser(description="Agricultural Monitoring Pipeline")
    parser.add_argument("--action", type=str, choices=["ingest", "inspect", "validate"], required=True)
    parser.add_argument("--file", type=str, help="Optional: specific parquet file")
    args = parser.parse_args()

    action = args.action
    file = args.file

    logging.info(f"Starting action: {action}")

    pipeline = AgriculturalMonitoringPipeline()
    pipeline.run(action, file)
