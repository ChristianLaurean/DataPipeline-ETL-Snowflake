# DataPipeline-ETL-Snowflake

![Diagrama del Pipeline ETL](https://github.com/ChristianLaurean/my_obsidian/blob/main/01%20-%20Projects/ETL%20Basico/Dise%C3%B1o_ETL.excalidraw.png)

This repository contains an ETL (Extract, Transform, Load) Data Pipeline that extracts data from various sources, transforms it, and loads it into a Snowflake database as part of the ETL process.

## Project Structure

The project is structured as follows:

- **dockerfile**: Contains the Dockerfile for container configuration.
- **etl**: Main directory for the ETL pipeline source code.
  - **src**: Contains source data files.
  - **constants.py**: File defining constants used in the pipeline.
  - **pipeline_utils.py**: Module containing utility functions for the pipeline.
  - **etl_pipeline.py**: Main script orchestrating the ETL process.

## Requirements

Before running the pipeline, make sure you have Docker installed and have configured the necessary environment variables for connecting to the OLTP database and Snowflake.

## Usage

1. Clone this repository to your local machine.
2. Configure the necessary environment variables in a `.env` file in the project's root directory.
3. Run the following command to build the Docker image:

```
docker build -t image_name .
```

Once the Docker image is successfully built, run the following command to create and run a container based on the image:

```
docker run image_name
```
