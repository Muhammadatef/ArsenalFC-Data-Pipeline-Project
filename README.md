# ArsenalFC-Data-Pipeline-Project


# Arsenal FC Data Pipeline & Analysis 

This project encompasses a comprehensive data engineering pipeline designed to process and analyze data related to Arsenal Football Club's players, goalkeepers, and matches from 2017 to 2023. The processed data provides insights into player performances, match outcomes, and goalkeeper statistics.

![Architecture Diagram](/Architecture.jpg)

## Tech Stack & Tools

- **Infrastructure**: Docker
- **Data Warehouse**: PostgreSQL
- **Database**: PostgreSQL
- **Orchestration**: Apache Airflow
- **Data Processing**: Apache Spark
- **ETL Scripts**: Jupyter & Python
- **Serving Layer**: PowerBI

## Pipeline Overview

The pipeline starts by ingesting raw data from CSV files using Apache Spark for data processing. Following the ETL (Extract, Transform, Load) process, the data is stored in PostgreSQL, which acts as the central storage layer. The orchestration of the ETL workflow is managed by Apache Airflow, ensuring the tasks are scheduled and run systematically. Finally, the insights derived from the processed data are visualized using PowerBI, providing actionable intelligence through interactive reports and dashboards.

## Getting Started

This section will guide you through getting the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Docker
- Docker Compose

### Installation & Setup

1- Download the Repo
2- go to the folder of Docker_files
3- Run this command to build the infrastructure
<pre> <code> cd /<path of your docker folder>
        docker-compose up -d </code> </pre>
4- open Jupyter on <pre> <code> http://localhost:8085/ </code> </pre> 

5- Run the Jupyter Notebooks to extract, transform & Load the data from and to PostgreSQL, default schema for the raw data - DWH schema if for DWH Data under arsenalfc database.

6- Link Postgresql to PowerBI from Get Data Panel, choose Database then choose PostgreSQL Database, write the credentials:  
<pre> <code> server : localhost:5442 database: arsenalfc </codd> </pre>

### Usage

Instructions on executing ETL processes, scheduling via Airflow, and accessing PowerBI reports.

## Contributing

We welcome contributions! Please read our contributing guidelines to get started.

## License

This project is licensed under the [MIT License](LICENSE.md) - see the LICENSE file for details.
