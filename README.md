# neo4j-road-fatality-analysis

# Graph Database Design and Query Project - Road Fatality Analysis

This repository contains the code, data processing scripts, graph model design, and final report for analyzing Australian road fatality data using a Neo4j graph database.

## Project Overview

The primary goal of this project is to design, implement, and query a graph database based on a provided dataset of Australian road fatalities (sourced from ARDD, December 2024). The project covers:

1.  **Graph Database Design:** Modeling the relationships between crashes, fatalities (persons), and locations using Arrows.app.
2.  **ETL Process:** Extracting, transforming, and loading the raw CSV data into a structure suitable for Neo4j import using Python (pandas).
3.  **Database Implementation:** Creating the graph database in Neo4j, including defining schema constraints and indexes, and loading data using Cypher scripts.
4.  **Cypher Querying:** Answering specific analytical questions provided in the project brief using Cypher queries.
5.  **Graph Data Science Application:** Discussing a potential application of Graph Data Science algorithms on the constructed graph.

## Repository Structure

```
.
├── arrows_model/                # Contains the graph model file(s)
│   ├── crash_graph_model.arrows
│   └── crash_graph_model.png    # Exported image of the model
├── data/                        # Holds the raw and processed data
│   ├── source/                  # Original provided dataset
│   │   └── Project2_Dataset.csv
│   ├── import_original/         # CSVs for Neo4j import (full dataset)
│   │   ├── crashes.csv
│   │   ├── persons.csv
│   │   ├── lgas.csv
│   │   ├── sa4s.csv
│   │   ├── states.csv
│   │   ├── rels_person_crash.csv
│   │   ├── rels_crash_lga.csv
│   │   ├── rels_crash_sa4.csv
│   │   └── rels_sa4_state.csv
│   └── import_filtered/         # CSVs for filtered dataset (if used)
├── scripts/                     # Contains ETL and Neo4j scripts
│   ├── ETL_Process.ipynb        # Python script for ETL process
│   ├── neo4j_db_load.txt        # Cypher script for loading data
│   └── cypher_query.txt         # Cypher script for running queries
├── report/                      # Contains the final project report
│   └── Report.pdf
└── README.md                    # This file
```

## Getting Started

### Prerequisites

- Python 3
- activate venv

```

python3 -m venv venv
source venv/bin/activate

```

- pandas library (`pip install pandas`)
- Neo4j Desktop or Neo4j Server (Version 5.x recommended)
- Access to Arrows.app (for viewing/editing the model)

### Setup and Execution

1. **Run ETL Script (jupyter notebook):**

- Place the original `Project2_Dataset.csv` in the `data/source/` directory.
- Execute the jupyter script to generate the import CSVs

- This will populate the `data/import_original/` (and optionally `data/import_filtered/`) directories.

2.  **Load Data into Neo4j:**
    - Ensure Neo4j instance is running.
    - Configure Neo4j to allow file imports from the `data/import_original/` directory (check Neo4j configuration settings for `dbms.security.allow_csv_import_from_file_urls` or place files in the designated Neo4j `import` folder).
    - Open the `scripts/load_and_query.cypher` file.
    - Execute the schema creation and data loading sections of the script within the Neo4j Browser or Cypher Shell. _(Remember to adjust file paths in `LOAD CSV` if necessary, e.g., `file:///crashes.csv`)_.
3.  **Run Queries:**
    - Execute the query sections within the `scripts/load_and_query.cypher` file in Neo4j Browser. _(Note: Query (f) might require loading from `import_filtered` if performance is an issue)._

## Report

The final analysis, design details, ETL process, implementation steps, query results, and GDS discussion are documented in the PDF report located in the `/report` directory.
