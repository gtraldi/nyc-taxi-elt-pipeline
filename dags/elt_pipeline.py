import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.operators.bash import BashOperator


@dag(
    dag_id="nyc_taxi_elt_pipeline",
    schedule="0 0 1,15 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "start_period": Param(
            default=None,
            type=["null", "string"],
            description="Start period in yyyy-mm format"
        ),
        "end_period": Param(
            default=None,
            type=["null", "string"],
            description="End period in yyyy-mm format"
        ),
        "force_reprocess": Param(
            default=False,
            type="boolean",
            description="Force reprocess of data in database"
        )
    }
)
def nyc_taxi_elt_pipeline():
    @task
    def generate_periods(**context) -> list:
        """
        Generates the time periods for the ELT pipeline
        
        Args:
            context: The Airflow context

        Returns:
            list: A list of tuples containing the year and month for each period
        """
        params = context.get("params", {})
        start_period = params.get("start_period")
        end_period = params.get("end_period")

        if start_period and end_period:
            try:
                start_date = datetime.strptime(start_period, "%Y-%m")
                end_date = datetime.strptime(end_period, "%Y-%m")
            except ValueError:
                raise ValueError("Invalid date format. Use YYYY-MM.")
        else:
            start_date = datetime.now(timezone.utc).replace(month=1)
            end_date = start_date.replace(month=12)
        
        periods = []
        current_date = start_date
        while current_date <= end_date:
            periods.append((current_date.year, current_date.month))
            current_date += relativedelta(months=1)

        logging.info(f"Generated {len(periods)} periods: {periods}")
        return periods

    @task
    def check_source_availability(periods: list) -> list:
        """
        Checks if the source file is available for download
        
        Args:
            periods (list): A list of tuples containing the year and month for each period

        Returns:
            list: A list of tuples containing the year and month for each period with available data
        """
        from scripts.extract import check_source_availability

        available_periods = []
        for period in periods:
            if check_source_availability(period[0], period[1]):
                available_periods.append(period)
            else:
                logging.warning(f"No data available for {period[0]}-{period[1]:02d}")

        if not available_periods:
            raise AirflowSkipException("No data available for the given period")

        logging.info(f"Available periods: {available_periods}")
        return available_periods

    @task
    def extract(period: tuple) -> str:
        """
        Extracts the data from the source
        
        Args:
            periods (list): A list of tuples containing the year and month for each period
        """
        from scripts.extract import download_parquet_files, check_files
        from scripts.load import check_data_in_db
        from utils.settings import LANDING_PATH
        
        context = get_current_context()
        params = context["params"]
        force_reprocess = params.get("force_reprocess")

        logging.info(f"Starting extraction check for period: {period[0]}-{period[1]:02d}")

        if force_reprocess:
            return download_parquet_files(period[0], period[1])

        if check_files(period[0], period[1]):
            if not check_data_in_db(period[0], period[1]):
                return f"{LANDING_PATH}/raw_{period[0]}_{period[1]}.parquet"
            
            raise AirflowSkipException(
                f"File {period[0]}-{period[1]:02d} already exists. Set force_reprocess=True if you want to overwrite and reload it."
            )
        
        return download_parquet_files(period[0], period[1])

    @task
    def load(file_path: str, **context) -> None:
        """
        Loads the preprocessed Parquet file into the PostgreSQL raw database schema.
        
        Args:
            file_path (str): Path to the Parquet file to load.
        """
        from scripts.load import load_data_to_db

        logging.info(f"Starting database load for file: {file_path}")
        load_data_to_db(file_path)

    task_dbt = BashOperator(
        task_id="dbt_run",
        bash_command="dbt seed; dbt run; dbt test",
        cwd="/opt/airflow/dbt"
    )

    periods = generate_periods()
    available_periods = check_source_availability(periods)

    extracted_data = extract.expand(period=available_periods)
    loaded_data = load.expand(file_path=extracted_data)

    loaded_data >> task_dbt

    

nyc_taxi_elt_pipeline()