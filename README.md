# ArsenalFC-Data-Pipeline-Project


# Arsenal FC Data Engineering Pipeline

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

Detailed instructions on how to set up the environment, configure services, and start the project using Docker.

### Usage

Instructions on executing ETL processes, scheduling via Airflow, and accessing PowerBI reports.

## Contributing

We welcome contributions! Please read our contributing guidelines to get started.

## License

This project is licensed under the [MIT License](LICENSE.md) - see the LICENSE file for details.
