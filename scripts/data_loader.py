import polars as pl
from pathlib import Path
import logging
from typing import List
import hashlib
from datetime import datetime


class DataLoader:
    def __init__(self):
        # Auto-detect project root directory
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        self.raw_data_dir = project_root / "data" / "raw"
        self.processed_data_dir = project_root / "data" / "processed"
        self.setup_logging()
        self.ensure_directories()

    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def ensure_directories(self):
        """Create necessary directories."""
        # Create institution-specific directories
        (self.processed_data_dir / "american_express" / "transactions").mkdir(
            parents=True, exist_ok=True
        )
        (self.processed_data_dir / "wealthsimple" / "transactions").mkdir(
            parents=True, exist_ok=True
        )

    def load_all_institutions(self) -> None:
        """Load data for all institutions."""
        self.logger.info("Loading all institution data...")

        # Load each institution
        self.load_american_express_data()
        self.load_wealthsimple_data()

        self.logger.info("Completed loading all institution data")

    def load_american_express_data(self) -> None:
        """Load American Express data from Excel files."""
        self.logger.info("Loading American Express data...")

        amex_dir = self.raw_data_dir / "american_express"
        if not amex_dir.exists():
            self.logger.warning(f"AmEx directory not found: {amex_dir}")
            return

        # Find Excel files
        excel_files = list(amex_dir.glob("*.xlsx")) + list(amex_dir.glob("*.xls"))

        if not excel_files:
            self.logger.warning(f"No Excel files found in {amex_dir}")
            return

        self.logger.info(f"Found {len(excel_files)} AmEx files")

        for file_path in excel_files:
            try:
                self._process_amex_file(file_path)
            except Exception as e:
                self.logger.error(f"Error processing AmEx file {file_path}: {str(e)}")
                continue

    def load_wealthsimple_data(self) -> None:
        """Load WealthSimple data from CSV files."""
        self.logger.info("Loading WealthSimple data...")

        ws_dir = self.raw_data_dir / "wealthsimple"
        if not ws_dir.exists():
            self.logger.warning(f"WealthSimple directory not found: {ws_dir}")
            return

        # Find CSV files
        csv_files = list(ws_dir.glob("*.csv"))

        if not csv_files:
            self.logger.warning(f"No CSV files found in {ws_dir}")
            return

        self.logger.info(f"Found {len(csv_files)} WealthSimple files")

        for file_path in csv_files:
            try:
                self._process_wealthsimple_file(file_path)
            except Exception as e:
                self.logger.error(
                    f"Error processing WealthSimple file {file_path}: {str(e)}"
                )
                continue

    def _process_amex_file(self, file_path: Path) -> None:
        """Process a single American Express Excel file."""
        self.logger.info(f"Processing AmEx file: {file_path.name}")

        # Check if file already processed
        file_hash = self._calculate_file_hash(str(file_path))
        if self._is_file_already_processed(file_hash):
            self.logger.info(f"AmEx file {file_path.name} already processed, skipping")
            return

        try:
            df = pl.read_excel(
                str(file_path),
                read_options={"header_row": 11},
            )
        except Exception as e:
            self.logger.error(f"Failed to read AmEx Excel file: {e}")
            return

        if df.is_empty():
            self.logger.warning(f"Empty AmEx file: {file_path}")
            return

        # AmEx-specific preprocessing - clean messy Excel file
        self.logger.info("Applying AmEx-specific preprocessing...")

        # Remove completely empty rows
        df = df.filter(~pl.all_horizontal(pl.col("*").is_null()))

        # Remove rows where all key columns are null
        key_columns = ["Date", "Description", "Amount"]
        existing_key_columns = [col for col in key_columns if col in df.columns]

        if existing_key_columns:
            df = df.filter(
                ~pl.all_horizontal(
                    [pl.col(col).is_null() for col in existing_key_columns]
                )
            )

        # Remove header rows that might be repeated
        if "Date" in df.columns:
            df = df.filter(
                (~pl.col("Date").str.contains("Date", literal=True))
                | pl.col("Date").is_null()
            )

        # Clean up string columns - remove extra whitespace
        string_columns = [col for col in df.columns if df[col].dtype == pl.String]
        for col in string_columns:
            df = df.with_columns(pl.col(col).str.strip_chars())

        # Remove completely empty columns
        non_empty_columns = []
        for col in df.columns:
            if not df[col].is_null().all():
                non_empty_columns.append(col)

        if non_empty_columns:
            df = df.select(non_empty_columns)

        # Handle AmEx specific amount cleaning
        if "Amount" in df.columns:
            # Remove any rows where Amount is clearly not a number
            df = df.filter(
                pl.col("Amount").str.replace_all(r"[^\d\.\-]", "").str.len_chars() > 0
            )

        self.logger.info(f"AmEx preprocessing: {len(df)} rows remaining after cleanup")

        if df.is_empty():
            self.logger.warning(
                f"No data remaining after AmEx preprocessing: {file_path}"
            )
            return

        # AmEx column mappings and processing
        column_mappings = {
            "Date": "date",
            "Date Processed": "date_processed",
            "Description": "description",
            "Cardmember": "cardmember",
            "Amount": "amount",
            "Foreign Spend Amount": "foreign_spend_amount",
            "Commission": "commission",
            "Exchange Rate": "exchange_rate",
            "Merchant": "merchant",
            "Merchant Address": "merchant_address",
            "Additional Information": "additional_information",
        }

        # Apply column mappings
        rename_dict = {}
        for old_name, new_name in column_mappings.items():
            if old_name in df.columns:
                rename_dict[old_name] = new_name

        if rename_dict:
            df = df.rename(rename_dict)

        # Validate required columns
        required_cols = ["date", "description", "amount"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required AmEx columns: {missing_cols}")

        # Process AmEx dates with multiple format attempts
        if "date" not in df.columns:
            raise ValueError("No date column found")

        # Clean up date strings first
        df = df.with_columns(
            pl.col("date").str.strip_chars().str.replace_all(r"\s+", " ")
        )

        # AmEx specific date formats
        date_formats = [
            "%d %b. %Y",  # "01 Jun. 2025" - with period
            "%d %b %Y",  # "30 May 2025" - without period
        ]

        # Try different date formats, using coalesce to combine them
        date_expr = pl.col("date")

        # Build a coalesce expression that tries each format
        for i, date_format in enumerate(date_formats):
            try:
                parsed_expr = pl.col("date").str.strptime(
                    pl.Date, date_format, strict=False
                )

                if i == 0:
                    # First format - use directly
                    date_expr = parsed_expr
                else:
                    # Subsequent formats - coalesce with previous attempts
                    date_expr = date_expr.fill_null(parsed_expr)

                # Test this format to log success
                temp_df = df.with_columns(parsed_expr.alias("test_date"))
                successful_conversions = temp_df.filter(
                    pl.col("test_date").is_not_null()
                ).height

                if successful_conversions > 0:
                    self.logger.info(
                        f"Format {date_format} parsed {successful_conversions} dates"
                    )

            except Exception as e:
                self.logger.debug(f"Date format {date_format} failed: {e}")
                continue

        # Apply the combined date parsing
        df = df.with_columns(date_expr.alias("date"))
        final_parsed = df.filter(pl.col("date").is_not_null()).height

        self.logger.info(f"Total dates successfully parsed: {final_parsed}")

        if final_parsed == 0:
            self.logger.error(
                "No date format worked! Adding default values to avoid partition issues"
            )
            # Add default year/month to avoid partition errors
            df = df.with_columns(
                [
                    pl.lit(None, dtype=pl.Date).alias("date"),
                    pl.lit(1900).alias("year"),
                    pl.lit(1).alias("month"),
                    pl.lit("1900-01").alias("year_month"),
                ]
            )
        else:
            # Add derived date fields
            df = df.with_columns(
                [
                    pl.col("date").dt.year().alias("year"),
                    pl.col("date").dt.month().alias("month"),
                    pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
                ]
            )

        # Process AmEx amounts: positive = expenses, negative = credits
        if "amount" not in df.columns:
            raise ValueError("No amount column found")

        # Clean and convert amount column (handle string formatting)
        try:
            df = df.with_columns(
                [pl.col("amount").cast(pl.Float64, strict=False).alias("amount")]
            )
        except:
            # If direct conversion fails, clean as string first
            df = df.with_columns(
                [
                    pl.col("amount")
                    .cast(pl.String)
                    .str.replace_all(r"[,$]", "")
                    .cast(pl.Float64, strict=False)
                    .alias("amount")
                ]
            )

        # AmEx: positive = expenses, negative = credits/refunds
        df = df.with_columns(
            [
                pl.when(pl.col("amount") > 0)
                .then(pl.lit("Expense"))
                .otherwise(pl.lit("Income"))
                .alias("flow_type"),
                pl.col("amount").abs().alias("abs_amount"),
            ]
        )

        # Add derived fields
        df = df.with_columns(
            [
                # Weekend indicator
                pl.col("date").dt.weekday().is_in([6, 7]).alias("is_weekend"),
                # Institution identifier
                pl.lit("American Express").alias("institution"),
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

        # Add metadata
        df = self._add_metadata(df, file_path, file_hash)

        # Save as partitioned Parquet
        self._save_partitioned_parquet(df, "american_express")

        self.logger.info(
            f"Successfully processed {len(df)} AmEx records from {file_path.name}"
        )

    def _process_wealthsimple_file(self, file_path: Path) -> None:
        """Process a single WealthSimple CSV file."""
        self.logger.info(f"Processing WealthSimple file: {file_path.name}")

        # Check if file already processed
        file_hash = self._calculate_file_hash(str(file_path))
        if self._is_file_already_processed(file_hash):
            self.logger.info(
                f"WealthSimple file {file_path.name} already processed, skipping"
            )
            return

        # Read CSV file
        try:
            df = pl.read_csv(str(file_path))
        except Exception as e:
            self.logger.error(f"Failed to read WealthSimple CSV file: {e}")
            return

        if df.is_empty():
            self.logger.warning(f"Empty WealthSimple file: {file_path}")
            return

        # WealthSimple-specific preprocessing - clean CSV files
        self.logger.info("Applying WealthSimple-specific preprocessing...")

        # Remove empty rows
        df = df.filter(~pl.all_horizontal(pl.col("*").is_null()))

        # Clean up string columns only
        for col in df.columns:
            if df[col].dtype == pl.String:
                df = df.with_columns(pl.col(col).str.strip_chars())

        self.logger.info(f"WealthSimple preprocessing: {len(df)} rows after cleanup")

        if df.is_empty():
            self.logger.warning(
                f"No data remaining after WealthSimple preprocessing: {file_path}"
            )
            return

        # WealthSimple column mappings and processing
        column_mappings = {
            "date": "date",
            "transaction": "transaction",
            "description": "description",
            "amount": "amount",
            "balance": "balance",
        }

        # Apply column mappings
        rename_dict = {}
        for old_name, new_name in column_mappings.items():
            if old_name in df.columns:
                rename_dict[old_name] = new_name

        if rename_dict:
            df = df.rename(rename_dict)

        # Map transaction to description if needed (WealthSimple uses 'transaction' not 'description')
        if "description" not in df.columns and "transaction" in df.columns:
            df = df.rename({"transaction": "description"})

        # Validate required columns
        required_cols = ["date", "amount", "description"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required WealthSimple columns: {missing_cols}")

        # Process WealthSimple dates
        if "date" not in df.columns:
            raise ValueError("No date column found")

        # Clean up date strings first
        df = df.with_columns(
            pl.col("date").str.strip_chars().str.replace_all(r"\s+", " ")
        )

        # WealthSimple specific date formats
        date_formats = ["%Y-%m-%d"]  # Standard ISO format from WealthSimple CSVs

        # Try different date formats, using coalesce to combine them
        date_expr = pl.col("date")

        # Build a coalesce expression that tries each format
        for i, date_format in enumerate(date_formats):
            try:
                parsed_expr = pl.col("date").str.strptime(
                    pl.Date, date_format, strict=False
                )

                if i == 0:
                    # First format - use directly
                    date_expr = parsed_expr
                else:
                    # Subsequent formats - coalesce with previous attempts
                    date_expr = date_expr.fill_null(parsed_expr)

                # Test this format to log success
                temp_df = df.with_columns(parsed_expr.alias("test_date"))
                successful_conversions = temp_df.filter(
                    pl.col("test_date").is_not_null()
                ).height

                if successful_conversions > 0:
                    self.logger.info(
                        f"Format {date_format} parsed {successful_conversions} dates"
                    )

            except Exception as e:
                self.logger.debug(f"Date format {date_format} failed: {e}")
                continue

        # Apply the combined date parsing
        df = df.with_columns(date_expr.alias("date"))
        final_parsed = df.filter(pl.col("date").is_not_null()).height

        self.logger.info(f"Total dates successfully parsed: {final_parsed}")

        if final_parsed == 0:
            self.logger.error(
                "No date format worked! Adding default values to avoid partition issues"
            )
            # Add default year/month to avoid partition errors
            df = df.with_columns(
                [
                    pl.lit(None, dtype=pl.Date).alias("date"),
                    pl.lit(1900).alias("year"),
                    pl.lit(1).alias("month"),
                    pl.lit("1900-01").alias("year_month"),
                ]
            )
        else:
            # Add derived date fields
            df = df.with_columns(
                [
                    pl.col("date").dt.year().alias("year"),
                    pl.col("date").dt.month().alias("month"),
                    pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
                ]
            )

        # Process WealthSimple amounts: positive = income, negative = expenses
        if "amount" not in df.columns:
            raise ValueError("No amount column found")

        # Handle numeric or string amounts
        amount_dtype = df["amount"].dtype

        if amount_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
            # Already numeric
            df = df.with_columns(
                [pl.col("amount").cast(pl.Float64, strict=False).alias("amount")]
            )
        else:
            # String - clean and convert
            df = df.with_columns(
                [
                    pl.col("amount")
                    .str.replace_all(r"[,$]", "")
                    .cast(pl.Float64, strict=False)
                    .alias("amount")
                ]
            )

        # WealthSimple: positive = income, negative = expenses
        df = df.with_columns(
            [
                pl.when(pl.col("amount") > 0)
                .then(pl.lit("Income"))
                .otherwise(pl.lit("Expense"))
                .alias("flow_type"),
                pl.col("amount").abs().alias("abs_amount"),
            ]
        )

        # Add derived fields
        df = df.with_columns(
            [
                # Weekend indicator
                pl.col("date").dt.weekday().is_in([6, 7]).alias("is_weekend"),
                # Institution identifier
                pl.lit("WealthSimple").alias("institution"),
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

        # Add metadata
        df = self._add_metadata(df, file_path, file_hash)

        # Save as partitioned Parquet
        self._save_partitioned_parquet(df, "wealthsimple")

        self.logger.info(
            f"Successfully processed {len(df)} WealthSimple records from {file_path.name}"
        )

    def _add_metadata(
        self, df: pl.DataFrame, file_path: Path, file_hash: str
    ) -> pl.DataFrame:
        """Add metadata columns to the DataFrame."""
        return df.with_columns(
            [
                pl.lit(str(file_path)).alias("source_file"),
                pl.lit(file_hash).alias("file_hash"),
                pl.lit(datetime.now()).alias("loaded_at"),
            ]
        )

    def _save_partitioned_parquet(self, df: pl.DataFrame, institution: str) -> None:
        """Save DataFrame using institution-specific Hive partitioning."""
        # Save to institution-specific path
        base_path = self.processed_data_dir / institution / "transactions"

        # Create directory if it doesn't exist
        base_path.mkdir(parents=True, exist_ok=True)

        # Use Polars built-in Hive partitioning
        df.write_parquet(
            base_path,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["year", "month"],
                "existing_data_behavior": "overwrite_or_ignore",
            },
        )

        self.logger.info(
            f"Saved {len(df)} {institution} records using Hive partitioning to {base_path}"
        )

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
            # Check in both institution directories
            for institution in ["american_express", "wealthsimple"]:
                transactions_path = (
                    self.processed_data_dir / institution / "transactions"
                )
                if transactions_path.exists():
                    # Check if any parquet file contains this file_hash
                    df = pl.scan_parquet(str(transactions_path / "**" / "*.parquet"))
                    existing_hashes = df.select("file_hash").unique().collect()
                    if file_hash in existing_hashes.get_column("file_hash").to_list():
                        return True
            return False
        except Exception:
            return False


def run_data_loader():
    loader = DataLoader()
    loader.load_all_institutions()


if __name__ == "__main__":
    run_data_loader()
