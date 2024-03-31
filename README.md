# Arsenal FC Data Pipeline & Analysis Project

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
<pre> <code> cd / {path of your docker folder}
        docker-compose up -d </code> </pre>
4- open Jupyter on <pre> <code> http://localhost:8085/ </code> </pre> 

5- Run the Jupyter Notebooks to extract, transform & Load the data from and to PostgreSQL, default schema for the raw data - DWH schema if for DWH Data under arsenalfc database.

6- Link Postgresql to PowerBI from Get Data Panel, choose Database then choose PostgreSQL Database, and write the credentials:  
<pre> <code> server : localhost:5442 database: arsenalfc </codd> </pre>

## Data Extraction from PostgreSQL

The data extraction process involves reading the raw data stored in the PostgreSQL database. We have dedicated tables for Players, Goalkeepers, and Matches, each containing historical data from 2017 to 2023. This data forms the foundation for our analysis pipeline.

To extract the data, we connect to the PostgreSQL database using Apache Spark's JDBC connector, which allows us to handle large datasets efficiently. The extraction process is orchestrated by Apache Airflow, ensuring the data is consistently and reliably pulled into our processing environment for the next ETL steps.

## Transformation and Loading into the Data Warehouse Schema

Once the data is extracted, transformation routines are applied to clean, normalize, and enrich the datasets. These transformations include:

- Standardizing date formats
- Calculating derived metrics
- Deduplicating records
- Joining datasets to create comprehensive views

The transformed data is then loaded into a structured data warehouse schema within PostgreSQL. This schema is optimized for query efficiency and is designed to support the complex analytical queries required for our reporting and visualization needs.

## Galaxy Schema Overview

The Galaxy Schema, also known as a Fact Constellation Schema, is employed in our data warehouse to facilitate complex queries across different subject areas. It allows us to analyze performance from multiple dimensions while keeping our data model scalable and performant.

### Fact Tables:
- **FactPlayers**: Captures metrics related to players' performance in each match.
- **FactGoalKeepers**: Stores performance data specific to goalkeepers, allowing for a focused analysis on their unique contributions.

### Dimension Tables:
- **DimPlayers**: Includes attributes of players such as name, position, and unique identifiers.
- **DimGoalKeepers (DimGK)**: Contains goalkeepers' information, similar to DimPlayers but tailored to the specific role.
- **DimMatches**: Describes match details, including dates, teams, scores, and venues.
- **DimDate**: Provides a calendar dimension that facilitates time-series analysis and trend identification.

The relationships between these tables are designed to provide a comprehensive view of the club's performance, offering insights into individual and team progress over time. This schema is the backbone of our data-driven decision-making processes and is crucial for generating the analytical reports presented in PowerBI.

![Galaxy Schema](/GalaxySchema.png)

## Serving Layer Analysis with PowerBI and Jupyter Notebooks

The final steps in our data pipeline are the analysis and visualization of the transformed data. PowerBI serves as our primary serving layer, creating a suite of dashboards that provide deep insights into performance metrics. Additionally, we utilize Jupyter Notebooks for ad-hoc analysis and aggregations, leveraging PySpark for its powerful data processing capabilities. This combination ensures that stakeholders have access to both standardized reports and the flexibility for custom data exploration.

### PowerBI Dashboards

Our PowerBI dashboards harness the curated datasets to present interactive and insightful visualizations. These dashboards enable stakeholders to make informed decisions by encapsulating various aspects of the game, including player statistics, match outcomes, and seasonal performance reviews.

We have meticulously crafted a total of 8 dashboards, each designed to address specific analytical queries and offer unique data perspectives:

- Dashboard 1: Arsenal Matches and Goals through the seasons
- Dashboard 2: Arsenal vs Opponents Analysis
- Dashboard 3: Arsenal Players & Goalkeepers Analysis
- Dashboard 4: Analysis of Total Shots, Shots on Target for Attackers & Midfielders, and Total Blocks, Interceptions, Tackles for Defenders
- Dashboard 5: Midfielders Performance Analysis
- Dashboard 6: Attackers Performance Analysis
- Dashboard 7: Defenders Performance Analysis
- Dashboard 8: Goalkeepers Performance Analysis

### Further Insights and Ad-Hoc Analysis

For an in-depth walkthrough of the dashboard analysis, please view our detailed video overview:

[![Dashboard Analysis](http://img.youtube.com/vi/4onMnSjMhR8/0.jpg)](https://www.youtube.com/watch?v=4onMnSjMhR8 "Arsenal FC Dashboard Analysis 2017 till 2023")

The full details of our dashboard analysis can also be found on NovyPro:

[Arsenal FC Dashboard Analysis 2017 till 2023 on NovyPro](https://www.novypro.com/project/arsenal-fc-dashboard-analysis-2017-till-2023)

Beyond PowerBI, our Jupyter Notebooks provide an interactive environment for performing additional transformations and complex aggregations. These notebooks empower data analysts to work with large datasets in a more explorative and iterative manner.

### Sample Visualizations

The following visualizations highlight key aspects of our PowerBI dashboards:

![Dashboard Visualization 1](/ArsenalVsOpponent.png)

![Dashboard Visualization 2](/GoalKeepers.png)

## Orchestration with Apache Airflow

The ETL pipeline is orchestrated using Apache Airflow, which schedules and manages the workflow of our data processing tasks. Airflow's robust framework allows for scheduling, monitoring, and automating the pipeline's components, ensuring that data flows seamlessly from extraction through to visualization.

Airflow provides a user-friendly interface to visualize the pipeline's progress, monitor task execution, and manage workflows efficiently. Our ETL workflow is encapsulated in a DAG (`etl_arsenalfc`) which is triggered on a scheduled basis, ensuring that the most up-to-date data is always processed and available for analysis.

### Accessing the Airflow Web Interface

The Airflow web interface can be accessed through the following link, which provides a real-time view of the pipeline's status, DAGs, and allows for manual triggers and inspections of logs:

[Airflow ETL Pipeline - etl_arsenalfc](http://localhost:8090/dags/etl_arsenalfc/)

Please note that the provided link assumes that Airflow is running on `localhost` on port `8090`. If your Airflow instance is hosted elsewhere or running on a different port, please adjust the URL accordingly.


### Usage

Instructions on executing ETL processes, scheduling via Airflow, and accessing PowerBI reports.

## Contributing

We welcome contributions! Please read our contributing guidelines to get started.

## License

This project is licensed under the [MIT License](LICENSE.md) - see the LICENSE file for details.

# COYG!
