import polars as pl
from pathlib import Path
import logging
from typing import Dict, List
import hashlib
from datetime import datetime


class DataLoader:
    def __init__(
        self, raw_data_dir: str = "data/raw", processed_data_dir: str = "data/processed"
    ):
        self.raw_data_dir = Path(raw_data_dir)
        self.processed_data_dir = Path(processed_data_dir)
        self.setup_logging()
        self.ensure_directories()

        # Institution-specific configurations
        self.institution_configs = {
            "american_express": {
                "folder_name": "american_express",
                "file_patterns": ["*.xlsx", "*.xls"],
                "column_mappings": {
                    "Date": "date",
                    "Date Processed": "date",
                    "Description": "description",
                    "Cardmember": "cardmember",
                    "Amount": "amount",
                    "Foreign Spend Amount": "foreign_spend_amount",
                    "Commission": "commission",
                    "Exchange Rate": "exchange_rate",
                    "Merchant": "merchant",
                    "Merchant Address": "merchant_address",
                    "Additional Information": "additional_information",
                },
                "date_formats": [
                    "%d %b. %Y",  # "01 Jun. 2025" - with period
                    "%d %b %Y",  # "30 May 2025" - without period
                ],
                "amount_processing": "positive_expenses",  # AmEx shows expenses as negative
            },
            "wealthsimple": {
                "folder_name": "wealthsimple",
                "file_patterns": ["*.csv"],
                "column_mappings": {
                    "date": "date",
                    "transaction": "transaction",
                    "description": "description",
                    "amount": "amount",
                    "balance": "balance",
                },
                "date_formats": ["%Y-%m-%d"],
                "amount_processing": "negative_expenses",  # WealthSimple format
            },
        }

    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def ensure_directories(self):
        """Create necessary directories."""
        (self.processed_data_dir / "transactions").mkdir(parents=True, exist_ok=True)
        (self.processed_data_dir / "metadata").mkdir(parents=True, exist_ok=True)

    def load_institution_data(self, institution: str) -> None:
        """Load all data files for a specific institution."""
        if institution not in self.institution_configs:
            raise ValueError(f"Unsupported institution: {institution}")

        config = self.institution_configs[institution]
        institution_dir = self.raw_data_dir / config["folder_name"]

        if not institution_dir.exists():
            self.logger.warning(f"Directory not found: {institution_dir}")
            return

        # Find all files matching the patterns
        all_files = []
        for pattern in config["file_patterns"]:
            files = list(institution_dir.glob(pattern))
            all_files.extend(files)

        if not all_files:
            self.logger.warning(f"No data files found in {institution_dir}")
            return

        self.logger.info(f"Found {len(all_files)} files for {institution}")

        # Process each file
        for file_path in all_files:
            try:
                self.process_file(file_path, institution)
            except Exception as e:
                self.logger.error(f"Error processing {file_path}: {str(e)}")
                continue

    def process_file(self, file_path: Path, institution: str) -> None:
        """Process a single file for the given institution."""
        self.logger.info(f"Processing {file_path.name} for {institution}")

        # Check if file already processed
        file_hash = self._calculate_file_hash(str(file_path))
        if self._is_file_already_processed(file_hash):
            self.logger.info(f"File {file_path.name} already processed, skipping")
            return

        # Read the file
        df = self._read_file(file_path)

        if df.is_empty():
            self.logger.warning(f"Empty file: {file_path}")
            return

        # Apply institution-specific processing
        df = self._process_institution_data(df, institution)

        # Add metadata
        df = self._add_metadata(df, file_path, institution, file_hash)

        # Save as partitioned Parquet
        self._save_partitioned_parquet(df, institution)

        # Update load history
        self._update_load_history(str(file_path), institution, len(df), file_hash)

        self.logger.info(
            f"Successfully processed {len(df)} records from {file_path.name}"
        )

    def load_all_institutions(self) -> None:
        """Load data for all configured institutions."""
        for institution in self.institution_configs.keys():
            self.logger.info(f"Loading data for {institution}")
            self.load_institution_data(institution)

    def _read_file(self, file_path: Path) -> pl.DataFrame:
        """Read a file based on its extension."""
        try:
            if file_path.suffix.lower() == ".csv":
                return pl.read_csv(str(file_path))
            elif file_path.suffix.lower() in [".xlsx", ".xls"]:
                return pl.read_excel(str(file_path))
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            return pl.DataFrame()

    def _process_institution_data(
        self, df: pl.DataFrame, institution: str
    ) -> pl.DataFrame:
        """Apply institution-specific data processing."""
        config = self.institution_configs[institution]

        # Apply column mappings
        df = self._apply_column_mappings(df, config["column_mappings"])

        # Process dates
        df = self._process_dates(df, config["date_formats"])

        # Process amounts
        df = self._process_amounts(df, config["amount_processing"])

        # Add institution-specific derived fields
        df = self._add_derived_fields(df, institution)

        return df

    def _apply_column_mappings(
        self, df: pl.DataFrame, mappings: Dict[str, str]
    ) -> pl.DataFrame:
        """Apply column name mappings."""
        rename_dict = {}
        for old_name, new_name in mappings.items():
            if old_name in df.columns:
                rename_dict[old_name] = new_name

        if rename_dict:
            df = df.rename(rename_dict)

        return df

    def _process_dates(self, df: pl.DataFrame, date_formats: List[str]) -> pl.DataFrame:
        """Process date columns with multiple format attempts."""
        if "date" not in df.columns:
            raise ValueError("No date column found after mapping")

        # Try different date formats
        for date_format in date_formats:
            try:
                df = df.with_columns(
                    pl.col("date")
                    .str.strptime(pl.Date, date_format, strict=False)
                    .alias("date")
                )
                # If successful, break
                if df.filter(pl.col("date").is_not_null()).height > 0:
                    break
            except Exception:
                continue

        # Add derived date fields
        df = df.with_columns(
            [
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
                pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
            ]
        )

        return df

    def _process_amounts(
        self, df: pl.DataFrame, amount_processing: str
    ) -> pl.DataFrame:
        """Process amount columns based on institution-specific rules."""
        if "amount" not in df.columns:
            raise ValueError("No amount column found after mapping")

        # Check if amount column is already numeric or string
        amount_dtype = df["amount"].dtype

        if amount_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
            # Already numeric - just ensure it's Float64
            df = df.with_columns(
                [pl.col("amount").cast(pl.Float64, strict=False).alias("amount")]
            )
        else:
            # String type - clean and convert
            df = df.with_columns(
                [
                    pl.col("amount")
                    .str.replace_all(r"[,$]", "")  # Remove commas and dollar signs
                    .cast(pl.Float64, strict=False)
                    .alias("amount")
                ]
            )

        # Apply institution-specific amount processing
        if amount_processing == "positive_expenses":
            # American Express: expenses are positive, credits/refunds are negative
            df = df.with_columns(
                [
                    pl.when(pl.col("amount") > 0)
                    .then(pl.lit("Expense"))
                    .otherwise(pl.lit("Income"))  # Negative amounts are credits/refunds
                    .alias("flow_type"),
                    pl.col("amount").abs().alias("abs_amount"),
                ]
            )
        elif amount_processing == "positive_income":
            # WealthSimple: income is positive, expenses are negative
            df = df.with_columns(
                [
                    pl.when(pl.col("amount") > 0)
                    .then(pl.lit("Income"))
                    .otherwise(pl.lit("Expense"))
                    .alias("flow_type"),
                    pl.col("amount").abs().alias("abs_amount"),
                ]
            )
        elif amount_processing == "negative_expenses":
            # Other institutions: expenses are negative, income is positive
            df = df.with_columns(
                [
                    pl.when(pl.col("amount") < 0)
                    .then(pl.lit("Expense"))
                    .otherwise(pl.lit("Income"))
                    .alias("flow_type"),
                    pl.col("amount").abs().alias("abs_amount"),
                ]
            )

        return df

    def _add_derived_fields(self, df: pl.DataFrame, institution: str) -> pl.DataFrame:
        """Add institution-specific derived fields."""
        df = df.with_columns(
            [
                # Weekend indicator
                pl.col("date").dt.weekday().is_in([6, 7]).alias("is_weekend"),
                # Institution identifier
                pl.lit(institution.replace("_", " ").title()).alias("institution"),
                # Transaction hash for deduplication
                pl.concat_str(
                    [
                        pl.col("date").dt.strftime("%Y-%m-%d"),
                        pl.col("description").str.slice(0, 50),
                        pl.col("amount").cast(pl.String),
                    ],
                    separator="|",
                )
                .hash()
                .alias("transaction_hash"),
            ]
        )

        return df

    def _add_metadata(
        self, df: pl.DataFrame, file_path: Path, institution: str, file_hash: str
    ) -> pl.DataFrame:
        """Add metadata columns to the DataFrame."""
        return df.with_columns(
            [
                pl.lit(str(file_path)).alias("source_file"),
                pl.lit(file_hash).alias("file_hash"),
                pl.lit(datetime.now()).alias("loaded_at"),
                pl.lit(file_path.stat().st_mtime).alias("file_modified_at"),
            ]
        )

    def _save_partitioned_parquet(self, df: pl.DataFrame, institution: str) -> None:
        """Save DataFrame as partitioned Parquet files."""
        base_path = self.processed_data_dir / "transactions"

        # Group by year and month for partitioning
        for (year, month), group_df in df.group_by(["year", "month"]):
            # Create partition directory
            partition_dir = base_path / f"year={year}" / f"month={month:02d}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{institution}_{timestamp}.parquet"
            file_path = partition_dir / filename

            # Save to Parquet
            group_df.write_parquet(file_path)

            self.logger.info(f"Saved {len(group_df)} records to {file_path}")

    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _is_file_already_processed(self, file_hash: str) -> bool:
        """Check if file has already been processed."""
        try:
            transactions_path = self.processed_data_dir / "transactions"
            if not transactions_path.exists():
                return False

            # Check if any parquet file contains this file_hash
            df = pl.scan_parquet(str(transactions_path / "**" / "*.parquet"))
            existing_hashes = df.select("file_hash").unique().collect()
            return file_hash in existing_hashes.get_column("file_hash").to_list()
        except Exception:
            return False

    def _update_load_history(
        self, file_path: str, institution: str, record_count: int, file_hash: str
    ) -> None:
        """Update load history metadata."""
        history_path = self.processed_data_dir / "metadata" / "load_history.parquet"

        new_record = pl.DataFrame(
            {
                "file_path": [file_path],
                "institution": [institution],
                "record_count": [record_count],
                "file_hash": [file_hash],
                "loaded_at": [datetime.now()],
            }
        )

        if history_path.exists():
            try:
                existing_history = pl.read_parquet(history_path)
                combined_history = pl.concat([existing_history, new_record])
            except Exception:
                combined_history = new_record
        else:
            combined_history = new_record

        combined_history.write_parquet(history_path)


def run_data_loader():
    loader = DataLoader()
    loader.load_all_institutions()


if __name__ == "__main__":
    run_data_loader()
