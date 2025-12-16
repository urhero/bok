# Factor Analysis Pipeline

This repository contains a Python-based pipeline for factor analysis, including data download, processing, and report generation.

## Project Structure

- `main.py`: The main entry point for the application, handling command-line arguments for downloading data and generating reports.
- `config.py`: Stores configuration parameters such as database connection details, benchmark information, and field/factor keys.
- `insert_quantile_data.py`: Processes pickled factor data to extract and format quantile return data into a JSON format, suitable for database insertion.
- `read_list_abbv.py`: A utility script to read and print the contents of `list_abbv.pkl`, which likely contains factor abbreviations.
- `port/query_structure.py`: Provides a `GenerateQueryStructure` class to fetch raw factor data from an MS SQL Server database.
- `service/download/write_pkl.py`: Contains functions for downloading raw factor data, assigning factors, and persisting processed data into pickle files. It includes utilities for ranking and quantile labeling.
- `service/report/read_pkl.py`: Orchestrates the ETL (Extract, Transform, Load) process for generating various reports, including factor return matrices, negative correlation matrices, and optimized two-factor mixes. It also simulates factor exposures.
- `utils/util.py`: Contains helper functions used across the project, such as pickle dumping, numeric utilities for ranking and quantile labeling, and core factor assignment logic.
- `docs/FLOW_VISUALIZATION.md`: Visual guide and mermaid diagram explaining the data flow and execution logic of the model portfolio generation.

## Getting Started

### Prerequisites

- Python 3.x
- Required Python packages (install using `pip install -r requirements.txt` or `pipenv install` if using Pipenv)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/bok.git
   cd bok
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   # or if using Pipenv
   pipenv install
   ```

### Usage

The `main.py` script is the primary interface for running the pipeline.

#### Download Raw Factor Data

To download raw factor data for a specified date range:

```bash
python main.py download <start_date> <end_date>
```

Replace `<start_date>` and `<end_date>` with dates in `YYYY-MM-DD` format.

Example:
```bash
python main.py download 2023-01-01 2023-12-31
```

#### Generate Reports

To generate reports from the downloaded data:

```bash
python main.py report <start_date> <end_date>
```

Replace `<start_date>` and `<end_date>` with dates in `YYYY-MM-DD` format.

Example:
```bash
python main.py report 2023-01-01 2023-12-31
```

#### Generate Model Portfolio

To generate model portfolio from the downloaded data:

```bash
python main.py mp <start_date> <end_date>
```

Replace `<start_date>` and `<end_date>` with dates in `YYYY-MM-DD` format.

Example:
```bash
python main.py mp 2023-01-01 2023-12-31
```

## Data

The `data/` directory stores various CSV and pickle files generated or used by the pipeline, including:
- `aggregated_weights_YYYY-MM-DD.csv`: Aggregated weights of securities.
- `best_sub_factor.csv`: Information about the best sub-factors.
- `factor_info.csv`: Metadata about factors.
- `factor_rets.csv`: Monthly factor return matrix.
- `list_abbv.pkl`: Pickled list of factor abbreviations.
- `list_data.pkl`: Pickled list of processed factor data.
- `list_name.pkl`: Pickled list of factor names.
- `list_style.pkl`: Pickled list of factor styles.
- `mix_grid.csv`: Grid of weight mixes for factor optimization.
- `neg_corr.csv`: Negative correlation matrix.
- `style_neg_corr.csv`: Negative correlation between style portfolios.
- `style_portfolios.csv`: Best-mix return series for each style.

## Configuration

The `config.py` file contains important configuration details. You may need to modify this file to adjust database connection settings or other parameters relevant to your environment.

## Dependencies

Key libraries used in this project include:
- `pandas`: For data manipulation and analysis.
- `numpy`: For numerical operations.
- `sqlalchemy`: For database interaction.
- `pyodbc`: ODBC driver for SQL Server connection.
- `rich`: For rich terminal output and progress bars.
- `argparse`: For command-line argument parsing.
- `pickle`: For serializing and deserializing Python objects.
