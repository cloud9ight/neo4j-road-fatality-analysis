# **Project 2: Graph Database Design and Cypher Query**

---

## **1. Graph Database Design using Arrows App**

For this project analysing the provided road fatality dataset, I designed a graph database model using Arrows.app. My main goal was to move away from the flat structure of the original CSV file, where crash details were repeated for each fatality record. Instead, I wanted a model that accurately represented the key entities – the crash events, the people involved, and the different levels of location – and clearly showed the relationships between them. This graph approach allows for more efficient querying of these connections.

The final model includes the following nodes and relationships:

- **Nodes:**
  - `Crash`: Represents **each unique crash event**. To ensure uniqueness within Neo4j and reliable links, I generated a sequential integer `internalCrashID` during the data preparation stage to act as the internal ID. This node holds properties specific to the crash itself, like `time`, `year`, `month`, `dayweek`, `crashType`, vehicle involvement flags (`busInvolvement`, etc.), `speedLimit`, `nationalRoadType`, `nationalRemotenessAreas`, and the `numberFatalities` (total for the event, determined during ETL). I also stored the original `crash_id` from the source data as a property (`crashID_orig`) for easier reference back to the source.
  - `Person`: Represents **each individual fatality**. recorded in the dataset. The unique identifier for this node is the `personID` property, which comes directly from the original unique `ID` column in the source CSV. It stores attributes specific to the person: `roadUser`, `gender`, `age`, and `ageGroup`.
  - `LGA`: Represents a **unique Local Government Area**. Identified by its `name` property (unique key).
  - `SA4`: Represents a **unique Statistical Area Level 4**. Identified by its `name` property (unique key).
  - `State`: Represents a **unique Australian State/Territory**. Identified by its `name` property (unique key).
- **Relationships:**
  - `(Person)-[:WAS_INVOLVED_IN]->(Crash)`: Connects each fatality (`Person`) to the single, unique `Crash` event they were part of. This is a **many-to-one** relationship from Person to Crash.
  - `(Crash)-[:OCCURRED_IN]->(LGA)`: Links each `Crash` event to the specific `LGA` where it took place. Assumed to be a **many-to-one** relationship (multiple crashes can occur in one LGA).
  - `(LGA)-[:PART_OF]->(SA4)`: Defines the **many-to-one** hierarchical link where an LGA belongs to an SA4 region.
  - `(SA4)-[:IN_STATE]->(State)`: Defines the **many-to-one** hierarchical link where an SA4 region belongs to a State.

Below is the graph model designed in Arrows.app:

<img src="/Users/haz/5504dw/neo4j-road-fatality-analysis/arrows_models/crash_graph_model.png" alt="crash_graph_model" style="zoom: 45%;" />

---

## **2. Discussions of Design Choices**

Creating the graph model involved making several key decisions about how to structure the data in Neo4j. These choices were influenced by the original data format, the types of questions the project required answering, and general graph modeling principles.

- **Choice 1: Modeling Crashes as Unique Nodes**
  - **Decision:** I decided early on that each unique crash event (`crash_id`) needed its own distinct `:Crash` node. The alternative, putting all crash info onto each `:Person` node (like the CSV), felt unnatural for a graph and would create massive redundancy. A crash is a single event that multiple people might be involved in, so modeling it as a central node connected via `[:WAS_INVOLVED_IN]` felt like the correct way to represent this relationship in a graph.
  - **Pros:** The main advantage is **reduced data duplication**. Storing time, location, vehicle details, and total fatality count once per crash makes the database more compact and consistent. Queries about crash characteristics (like finding crashes with specific vehicle types in Query a or g) target these properties directly on the `:Crash` node, which is conceptually simpler and likely performs better than scanning potentially thousands of `:Person` nodes with repeated info. Graph traversals like finding all people in a crash are also very intuitive with this model.
  - **Cons:** The downside was the **added ETL work**. I had to write specific pandas code (`drop_duplicates`) to identify the unique crashes, generate the `internalCrashID` needed for linking, and make sure properties like `numberFatalities` reflected the total for the event, not just the single row being processed initially.
- **Choice 2: Hierarchical Location Nodes (`LGA`, `SA4`, `State`)**
  - **Decision:** Rather than just storing location names as text properties on `:Crash` nodes, I created separate nodes for `:LGA`, `:SA4`, and `:State`, connecting them with `:OCCURRED_IN`, `:PART_OF`, and `:IN_STATE` relationships. This mirrors the hierarchical structure suggested by the column names (based on the Australian Statistical Geography Standard).
  - **Pros:** This structure is powerful for **hierarchical querying**. It allows easy aggregation or filtering at different geographic levels (e.g., grouping crashes by State for Query c, or by SA4 for Query e) by simply traversing the relationships. Critically, having distinct `:LGA` nodes was **non-negotiable for Query (f)**, which requires finding paths _between_ LGAs. This query would be impossible if LGAs were just string properties. It also improves data quality by storing each unique location name only once.
  - **Cons:** This adds more nodes and relationships to the graph compared to a flatter structure. The ETL needed careful handling to create these distinct location nodes and ensure the `:PART_OF` relationship correctly represented a unique mapping from LGA to SA4 (which required correction during troubleshooting – see Appendix A). I considered the simpler alternative (locations as properties) but rejected it as it wouldn't meet the specific pathfinding and hierarchical analysis needs of the project queries.
- **Choice 3: Time and Vehicle Involvement as Node Properties**
  - **Decision:** Attributes like date/time components (year, month, time string, etc.) and the vehicle involvement flags (bus, heavy rigid, articulated) were stored directly as properties on the `:Crash` node.
  - **Pros:** For the specific queries in this project, this approach offered **simplicity and efficiency**. Filtering crashes by year (Queries a, c), time ranges (Query e), or vehicle involvement (Queries a, g) using `WHERE` clauses on these properties is direct and can be optimized with indexes. I decided against creating more complex structures like a dedicated `:Date` hierarchy or individual `:Vehicle` nodes, as the overhead didn't seem justified for the questions asked; this kept the model focused.
  - **Cons:** This choice does limit **more advanced analysis**. Performing complex time-series analysis (like finding day-over-day trends) would be difficult. Similarly, without `:Vehicle` nodes, detailed analysis focused on vehicle specifics (beyond just involvement) isn't possible. I acknowledged these limitations but decided the property approach was the most pragmatic balance for completing the project requirements efficiently.

Overall, I believe these design choices provide a model that accurately represents the core entities and relationships, directly supports the required analytical queries, and balances modeling detail with practical implementation for this dataset.

---

## **3. ETL: Dataset Transformation Process**

To prepare the data for Neo4j, firstly need to transform the original flat CSV file (`Project2_Dataset_Corrected.csv`) into separate files for nodes and relationships, matching the graph model I designed earlier. This ETL process was conducted using Python with `pandas` library inside a Jupyter Notebook (`ETL_Process.ipynb`), as it allowed me to check each step. The output CSVs were saved to the `data/import_final/` directory.

The main stages involved:

### Step1 - **Loading and Cleaning:**

I started by loading the source CSV into a pandas DataFrame. To make working with the columns easier, especially avoiding issues with spaces or mixed casing in later code or Cypher queries, I applied a cleaning function. This converted names like 'Crash ID' or 'SA4 Name 2021' to a standard format like 'crash_id' or 'sa4_name_2021' (lowercase with underscores). I verified this mapping before proceeding.

### Step2 - **Node File Generation & Intermediate Data Extraction**

Next, I generated the CSV files for each node type and extracted necessary data for relationship linking later.

- **Locations (`State`, `SA4`, `LGA`):** I extracted the unique names from the cleaned `state`, `sa4_name_2021`, and `national_lga_name_2024` columns. For each type, I created a DataFrame containing only the unique names, formatted it with the Neo4j import headers (`name:ID(NodeType)`, `:LABEL`), and saved it (e.g., `states.csv`, `sa4s.csv`, `lgas.csv`). Alongside creating the node files, I also prepared intermediate DataFrames holding the unique _pairs_ needed for relationships: `sa4_link_data` (containing unique SA4-State pairs) and `lga_link_data` (containing unique LGA-SA4 pairs based on the source data). These were kept aside for use in the relationship generation step.
- **Crashes (`Crash`):** Representing **each crash event uniquely** was crucial. I achieved this by deduplicating the main DataFrame based on the cleaned `crash_id`, keeping only the first row encountered for each distinct crash ID using `df.drop_duplicates(subset=['crash_id'], keep='first')`. This resulted in the `crash_data` DataFrame. I then generated a sequential integer column, `internalCrashID:ID(Crash)`, to serve as the unique Neo4j identifier for these `Crash` nodes. I performed necessary data type conversions on `crash_data` (e.g., for year, month, fatalities, speed limit - using nullable integers for the latter) and standardized 'Yes'/'No' values to lowercase 'yes'/'no'. Finally, I selected the relevant crash attribute columns, renamed some for clarity (like storing the original `crash_id` as a property named `crashID_orig`), added the `:LABEL` ('Crash'), and saved this data as `crashes.csv`. I also created the `crash_id_map` at this stage, linking the original `crash_id` to the new `internalCrashID`, which was essential for connecting Person nodes later.

  ```python
  # Key steps for Crash node creation and mapping
  crash_data = df.drop_duplicates(subset=['crash_id'], keep='first').reset_index(drop=True).copy()
  crash_data['internalCrashID:ID(Crash)'] = crash_data.index
  # ... (Data type handling, column selection/renaming into crash_nodes DataFrame) ...
  crash_nodes.to_csv(os.path.join(OUTPUT_DIR, 'crashes.csv'), index=False, encoding='utf-8')
  crash_id_map = crash_data[['crash_id', 'internalCrashID:ID(Crash)']].copy().set_index('crash_id')
  ```

- **Persons (`Person`):** Since each row in the original dataset corresponds to one fatality, I extracted the `Person` data directly from the main cleaned DataFrame (`df`). I selected the required columns, using the original `id` as the unique identifier (`personID:ID(Person)`), performed necessary type conversions (like for `age`), added the `:LABEL` ('Person'), and saved the result as `persons.csv`.

### Step3 - **Relationship File Generation:**

With nodes prepared, the final ETL step was to create the CSV files defining the relationships between them.

- `:IN_STATE` (SA4 -> State): This file (`rels_sa4_state.csv`) was generated directly from the `sa4_link_data` DataFrame extracted in step 2, renaming columns to `:START_ID(SA4)` and `:END_ID(State)` and adding the `:TYPE`.
- `:PART_OF` (LGA -> SA4): Generating this relationship (`rels_lga_sa4.csv`) required addressing an issue identified during later validation (full troubleshooting details in **Appendix A**). The intermediate `lga_link_data` DataFrame (from step 2) initially contained instances where a single LGA was paired with multiple different SA4s from the source data. Directly using this would create incorrect multiple `:PART_OF` relationships for some LGAs. **To correct this**, before creating the relationship file, I processed `lga_link_data` to ensure each LGA appeared only once, keeping the first SA4 it was associated with: `lga_to_sa4_unique = lga_link_data.drop_duplicates(subset=['national_lga_name_2024'], keep='first')`. This `lga_to_sa4_unique` DataFrame, guaranteeing a one-to-one LGA-to-SA4 mapping, was then used to create `rels_lga_sa4.csv` with the appropriate `:START_ID(LGA)`, `:END_ID(SA4)`, and `:TYPE` columns.

  ```python
  # Corrected logic for ensuring unique LGA->SA4 mapping
  lga_to_sa4_unique = lga_link_data.drop_duplicates(subset=['national_lga_name_2024'], keep='first')
  lga_sa4_rels = lga_to_sa4_unique.copy()
  # ... (Rename columns, add type, dropna, save to rels_lga_sa4.csv) ...
  ```

- `:OCCURRED_IN` (Crash -> LGA): This linked the unique `Crash` nodes (from `crash_data`, using `internalCrashID`) to the corresponding `LGA` node (using `national_lga_name_2024`). Rows where the LGA name was missing in `crash_data` were dropped before saving `rels_crash_lga.csv` to avoid import errors.
- `:WAS_INVOLVED_IN` (Person -> Crash): This linked each `Person` node (`personID`) to the correct unique `Crash` node (`internalCrashID`). This required merging the main `df` with the `crash_id_map` (created in step 2) on the common `crash_id` key. From the merged result, I selected the person's `id` and the corresponding `internalCrashID`, formatted them with Neo4j headers (`:START_ID(Person)`, `:END_ID(Crash)`), added the `:TYPE`, and saved `rels_person_crash.csv`.
- All relationship files contained the standard `:START_ID(StartNodeType)`, `:END_ID(EndNodeType)`, and `:TYPE` columns for Neo4j import.

After running these steps, the complete set of node and relationship CSV files was ready in the `data/import_final/` directory for loading into Neo4j.

---

## **4. Graph Database Implementation**

### **4.1 Database Loading Process**

After generating the node and relationship CSV files using Python (`ETL_Reloaded.ipynb`), the next step was loading this data into Neo4j. I created a fresh database instance named `crash_analysis_db` in Neo4j Desktop(v1.6.1, Neo4j Server 5.24.0 on macOS) and copied the CSVs into its dedicated `import` folder.

To manage the loading effectively and monitor each stage, rather than attempting to run a single long script which had previously presented execution difficulties in the Browser environment. I adopted a **step-by-step loading approach** directly in the Neo4j Browser. I executed the core Cypher commands in logical blocks, waiting for each block to complete successfully before proceeding to the next. This involved:

1.  **Constraint Creation:** First, I ran the `CREATE CONSTRAINT` commands for all node types (`Person`, `Crash`, `LGA`, `SA4`, `State`) to enforce uniqueness on their primary identifiers (`personID`, `internalCrashID`, `name`) and to optimize subsequent `MERGE` operations.

    ```cypher
    CREATE CONSTRAINT unique_person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.personID IS UNIQUE;
    CREATE CONSTRAINT unique_crash_id IF NOT EXISTS FOR (c:Crash) REQUIRE c.internalCrashID IS UNIQUE;
    CREATE CONSTRAINT unique_lga_name IF NOT EXISTS FOR (l:LGA) REQUIRE l.name IS UNIQUE;
    CREATE CONSTRAINT unique_sa4_name IF NOT EXISTS FOR (s4:SA4) REQUIRE s4.name IS UNIQUE;
    CREATE CONSTRAINT unique_state_name IF NOT EXISTS FOR (st:State) REQUIRE st.name IS UNIQUE;
    ```

    These commands completed quickly, confirming the basic schema setup was successful.

2.  **Node Loading:** Next, I loaded the data for each node type individually using separate `LOAD CSV ... MERGE ...` commands. This involved running one command block for States, then one for SA4s, then LGAs, Persons, and finally Crashes. The `MERGE` command ensured that nodes were created based on their unique identifiers defined by the constraints. Data type conversions (like `toInteger()`) and handling for potential null values (e.g., using `CASE` for `speedLimit`) were included in these commands.

    ```cypher
    // Example: Loading State nodes (Block 2a)
    LOAD CSV WITH HEADERS FROM 'file:///states.csv' AS row FIELDTERMINATOR ','
    MERGE (st:State {name: row.`name:ID(State)`});

    // Example: Loading Crash nodes (Block 2e)
    LOAD CSV WITH HEADERS FROM 'file:///crashes.csv' AS row FIELDTERMINATOR ','
    MERGE (c:Crash {internalCrashID: toInteger(row.`internalCrashID:ID(Crash)`)})
    // ON CREATE SET / ON MATCH SET clauses followed, defining properties...
    // ... Full command as used ...
    ```

    Each node loading step was verified by the completion message in the Neo4j Browser (e.g., "Created X nodes, set Y properties...").

3.  **Relationship Loading:** Once all nodes were loaded, I proceeded to load the relationships, again executing the `LOAD CSV ... MATCH ... MERGE ...` command for each relationship type separately. This involved matching the start and end nodes (using the constraints created in step 1) and then creating the relationship between them. The `USING PERIODIC COMMIT` clause was included to help manage memory during these potentially larger operations.

    ```cypher
    // Example: Loading Person->Crash relationships (Block 3d)
    USING PERIODIC COMMIT 1000
    LOAD CSV WITH HEADERS FROM 'file:///rels_person_crash.csv' AS row FIELDTERMINATOR ','
    MATCH (p:Person {personID: toInteger(row.`:START_ID(Person)`)})
    MATCH (c:Crash {internalCrashID: toInteger(row.`:END_ID(Crash)`)})
    MERGE (p)-[r:WAS_INVOLVED_IN]->(c);
    // ... commands for IN_STATE, PART_OF, OCCURRED_IN executed similarly ...
    ```

    Each relationship loading step was also confirmed by the completion message (e.g., "Created X relationships...").

4.  **Additional Index Creation:** As the final step after all data was successfully loaded, I ran the `CREATE INDEX` commands together to create indexes on non-unique properties frequently used in the project's queries (like `Crash.year`, `Person.ageGroup`, etc.) to improve query performance.
    ```cypher
     // Example: Creating index on Crash year (Part of Block 4)
    CREATE INDEX idx_crash_year IF NOT EXISTS FOR (c:Crash) ON (c.year);
    // ... other CREATE INDEX commands executed ...
    ```

This block-by-block approach ensured that each stage of the database population completed successfully before moving to the next, leading to a fully loaded and indexed graph database ready for querying. The complete set of commands executed in this manner is provided in the submitted `load_and_query.txt - part1: Data Load / Schema Creation` script file.

### **4.2 Database Statistics & Schema**

After loading process, I verified the database contents using cypher query. The following statistics reflect the final state of the populated `crash_analysis_db` :

```cypher
MATCH (n) RETURN labels(n), count(*);
MATCH ()-[r]->() RETURN type(r), count(*);
CALL db.schema.visualization();
```

- **Node Counts:**
  - State: 8
  - SA4: 88
  - LGA: 509
  - Person: 10490
  - Crash: 9683
- **Relationship Counts:**
  - IN_STATE: 88
  - PART_OF: 509
  - WAS_INVOLVED_IN: 10490
  - OCCURRED_IN: 9683

<img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429184700530.png" alt="image-20250429184700530" style="zoom: 40%;" />

<img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429172308783.png" alt="image-20250429172308783" style="zoom:40%;" />

**The final database schema:**

<img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429144825936.png" alt="image-20250429144825936" style="zoom: 40%;" />

The final database schema, visualized using `CALL db.schema.visualization();`, confirms the nodes and relationships were created according to the design:

---

## **5. Answering Questions using Cypher**

With the database loaded, I used Cypher queries in the Neo4j Browser to answer the specific questions from the project brief.

_note: I ran these queries on the full dataset loaded previously, as performance was acceptable for questions a-g._

### **(a) WA Crashes (2020-2024), Articulated Trucks, Multiple Fatalities**

I matched persons involved in crashes (traverse from `Person` nodes to their associated `Crash`), filtered these crashes by location (WA), year (2020-2024), specific vehicle involvement (articulated truck), and severity (multiple fatalities), then returned the requested details for the involved individuals.

```cypher
MATCH (p:Person)-[:WAS_INVOLVED_IN]->(c:Crash)-[:OCCURRED_IN]->(l:LGA)-[:PART_OF]->(:SA4)-[:IN_STATE]->(st:State {name: 'WA'})
WHERE c.year >= 2020 AND c.year <= 2024
  AND c.articulatedTruckInvolvement = 'yes'
  AND c.numberFatalities > 1
RETURN DISTINCT
       c.crashID_orig AS CrashID,
       p.personID AS PersonID,
       p.roadUser AS RoadUser,
       p.age AS Age,
       p.gender AS Gender,
       l.name AS LGAName,
       c.month AS Month,
       c.year AS Year,
       c.numberFatalities AS TotalFatalitiesInCrash
ORDER BY CrashID, PersonID;
```

- **Results Screenshot:**
  <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429152103240.png" alt="image-20250429152103240" style="zoom:40%;" />

- **Explanation:** This query successfully identified the relevant crashes and individuals meeting all the criteria in WA. Only 4 records (representing 2 distinct crashes involving 2 people each) were returned, indicating such specific incidents were relatively rare in the dataset for this period.

- **Note:** To validate, I manually filtered the original source CSV using Excel based on the same criteria. The individuals identified matched those returned by this Cypher query, confirming the accuracy of the implementation for this scenario.

### **(b) Max/Min Age, Motorcycle Riders, Holiday Periods, Inner Regional**

I matched `Person` nodes identified as 'Motorcycle rider' and linked them to their `Crash` nodes, filtered these incidents based on the specified holiday periods and location type, and then calculated the maximum and minimum ages for the male and female riders found within this subset.

```cypher
MATCH (p:Person {roadUser: 'Motorcycle rider'})-[:WAS_INVOLVED_IN]->(c:Crash)
WHERE (c.christmasPeriod = 'yes' OR c.easterPeriod = 'yes')
  AND c.nationalRemotenessAreas = 'Inner Regional Australia'
  AND p.gender IN ['Male', 'Female']
  AND p.age IS NOT NULL
RETURN p.gender AS Gender,
       max(p.age) AS MaximumAge,
       min(p.age) AS MinimumAge;
```

- **Results Screenshot:**
  ![image-20250429153837030](/Users/haz/Library/Application Support/typora-user-images/image-20250429153837030.png)

- **Explanation:** The query aimed to find the age range for both male and female motorcycle riders under specific holiday/location conditions. As shown in the results, a row for 'Female' is absent, indicating that no female motorcycle riders matching all specified conditions were found in the dataset. For the male riders who did match, the observed age range was from 14 to 73 years old. The absence of results for one category is a meaningful finding itself, reflecting the data distribution.

### **(c) Young Drivers (17-25), Weekend/Weekday Counts per State (2024), Avg Age**

I focused on fatalities involving individuals identified as 'Driver' within the '17_to_25' age group during 2024, as emphasized in the project requirements. I linked these individuals to their crash location (State) and aggregated the counts based on whether the crash occurred on a 'Weekend' or 'Weekday' (`dayOfWeekType` property). The average age for this specific group within each state was also calculated using `avg(p.age)`.

```cypher
MATCH (p:Person {
           ageGroup: '17_to_25',
           roadUser: 'Driver'
       })
       -[:WAS_INVOLVED_IN]->(c:Crash)
       -[:OCCURRED_IN]->(:LGA)
       -[:PART_OF]->(:SA4)
       -[:IN_STATE]->(st:State)
WHERE c.year = 2024
RETURN st.name AS StateName,
       SUM(CASE c.dayOfWeekType WHEN 'Weekend' THEN 1 ELSE 0 END) AS WeekendFatalities,
       SUM(CASE c.dayOfWeekType WHEN 'Weekday' THEN 1 ELSE 0 END) AS WeekdayFatalities,
       avg(p.age) AS AverageAgeYoungDriver
```

- **Results Screenshot:**

  <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429182005394.png" alt="image-20250429182005394" style="zoom:60%;" />

- **Explanation:** This query aggregates fatal crashes involving young drivers (17-25) in 2024 by state, separating counts for weekends and weekdays, and includes the average age. The results show figures for states like NSW (13 weekend, 19 weekday, avg age ~20.9) and QLD (8 weekend, 14 weekday, avg age ~20.0). States such as WA, NT, or ACT are absent from the results, indicating no recorded fatal crashes involving drivers in this specific age group occurred in those jurisdictions during 2024 according to the dataset. Among the states listed, weekdays generally saw slightly higher counts than weekends for this demographic.

### **(d) WA, Friday (Weekend Category), Multi-Death, Male & Female Victims**

I first located crashes in WA meeting the specific time criteria ('Weekend' Friday) and severity (multiple deaths). Then, for these qualifying crashes, I specifically checked using `EXISTS {}` subqueries for the presence of _both_ a male and a female victim involved before returning distinct details about the crash location and road type.

```cypher
MATCH (c:Crash)-[:OCCURRED_IN]->(:LGA)-[:PART_OF]->(s4:SA4)-[:IN_STATE]->(st:State {name: 'WA'})
WHERE c.dayweek = 'Friday'
  AND c.dayOfWeekType = 'Weekend'
  AND c.numberFatalities > 1
WITH c, s4
WHERE EXISTS { (p_male:Person {gender: 'Male'})-[:WAS_INVOLVED_IN]->(c) }
  AND EXISTS { (p_female:Person {gender: 'Female'})-[:WAS_INVOLVED_IN]->(c) }
RETURN DISTINCT s4.name AS SA4_Name,
              c.nationalRemotenessAreas AS NationalRemotenessArea,
              c.nationalRoadType AS NationalRoadType
ORDER BY SA4_Name;
```

- **Results Screenshot:**
  ![image-20250429183735122](/Users/haz/Library/Application Support/typora-user-images/image-20250429183735122.png)

- **Explanation:** This query successfully identified two specific types of locations in WA where this complex combination of circumstances (Weekend Friday, multi-fatality, both genders involved) was recorded in the dataset. One occurred in the "Perth - South East" SA4 region (a 'Major Cities' area, on a 'Local Road'), and the other in the "Western Australia - Outback (North)" SA4 region (a 'Very Remote' area, on a 'National or State Highway'). This indicates such specific incidents occurred in diverse environments within WA.

### **(e) Top 5 SA4 Regions by Fatal Crashes during Peak Hours**

Step 1: Find all crashes (not fatalities) within either peak period, using the `time()` function, and link to SA4 -> Step 2: Group by SA4, calculate separate peak counts for those crashes -> Step 3: Order by the SUM of the calculated peaks and take top 5

```cypher
MATCH (c:Crash)-[:OCCURRED_IN]->(:LGA)-[:PART_OF]->(s4:SA4)
WITH c, s4, time(c.time) AS crashTime
WHERE (crashTime >= time('07:00') AND crashTime <= time('09:00'))
   OR (crashTime >= time('16:00') AND crashTime <= time('18:00'))
WITH s4, crashTime
RETURN s4.name AS SA4_Region,
       count(CASE WHEN crashTime >= time('07:00') AND crashTime <= time('09:00') THEN 1 END) AS MorningPeak,
       count(CASE WHEN crashTime >= time('16:00') AND crashTime <= time('18:00') THEN 1 END) AS AfternoonPeak

ORDER BY (MorningPeak + AfternoonPeak) DESC
LIMIT 5;
```

- **Results Screenshot:**
  ![image-20250429200618543](/Users/haz/Library/Application Support/typora-user-images/image-20250429200618543.png)
- **Explanation:** This query identifies the SA4 regions with the highest number of distinct fatal crash events during peak hours. The results show "Wide Bay" at the top, with 31 fatal crash events in the morning peak and 47 in the afternoon peak (78 total). "Melbourne - South East" follows (25 morning, 37 afternoon; 62 total). The remaining top 5 are "Capital Region" (60 total), "South Australia - South East" (58 total), and "New England and North West" (52 total). This correctly counts unique crash events per peak period, aligning with the question's requirement, and differs from initial validation attempts that mistakenly counted fatalities.

### **(f) Top 3 Paths of Length 3 between any two LGAs**

To find any path consisting of exactly three relationship steps connecting any two distinct LGA nodes within the graph. The pattern `MATCH path = (lga1:LGA)-[*]-(lga2:LGA)` allows traversal across any intermediate node type and any relationship type in either direction. The `WHERE` clause specified `length(path) = 3` and used `elementId(lga1) < elementId(lga2)` (the non-deprecated function) to avoid duplicates and self-loops. The query aimed to return the top 3 such paths (**Note: query was run on the full, unfiltered dataset**)

```cypher
MATCH path = (lga1:LGA)-[*]-(lga2:LGA)
WHERE elementId(lga1) < elementId(lga2)
  AND length(path) = 3
RETURN lga1.name AS StartLGA,
       lga2.name AS EndLGA
ORDER BY StartLGA ASC, EndLGA ASC
LIMIT 3;
```

- **Results Screenshot:**
  <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429204000652.png" alt="image-20250429204000652" style="zoom:50%;" />

**Explanation of "No Records" Result:**

This query returned no results, indicating that within the constructed graph based on the loaded dataset, **no pairs of distinct LGA nodes are connected by a path of exactly three relationships.** To understand why, I considered the specific structure of my graph model (as shown in the Arrows diagram, Section 1):

- **Direct LGA Connections:** LGAs are primarily connected _indirectly_.
- **Length 2 Paths:** The most direct connections between two distinct LGAs in the model typically have a length of 2:
  - Via a shared SA4: `(LGA1)-[:PART_OF]->(SA4)<-[:PART_OF]-(LGA2)`
  - Via a shared Crash (if a crash occurred exactly on a border or was linked to multiple LGAs, though less likely with clean data): `(LGA1)<-[:OCCURRED_IN]-(Crash)-[:OCCURRED_IN]->(LGA2)`
- **Length 4 Paths:** Longer paths are common, for example:
  - Via a shared State (through different SA4s): `(LGA1)-[:PART_OF]->(SA4_X)-[:IN_STATE]->(State)<-[:IN_STATE]-(SA4_Y)<-[:PART_OF]-(LGA2)`
  - Via Crashes involving the same Person (less direct LGA link): `(LGA1)<-[:OCCURRED_IN]-(Crash1)-[:WAS_INVOLVED_IN]->(Person)<-[:WAS_INVOLVED_IN]-(Crash2)-[:OCCURRED_IN]->(LGA2)` (This path has length 5 between Crashes, length 4 between LGAs if Person is intermediate).
- **Absence of Length 3 Paths:** Constructing a path of _exactly_ length 3 between two distinct `:LGA` nodes using the defined relationships (`:PART_OF`, `:IN_STATE`, `:OCCURRED_IN`, `:WAS_INVOLVED_IN`) and intermediate nodes (`:Crash`, `:Person`, `:SA4`, `:State`) proved difficult conceptually. For example, `(LGA1)-[:PART_OF]->(SA4)-[:IN_STATE]->(State)` only reaches a State (length 2). Linking back to another LGA requires at least two more steps (State->SA4->LGA2, total length 4). Similarly, paths involving Crashes and Persons tend to result in even path lengths (2 or 4+) between LGAs.
- _Another way of thinking this query:_ there is a map (my graph model) of roads connecting towns (LGAs) via cities (SA4s) and states (States):
  Towns in the same city are 2 steps apart (TownA -> CityX <- TownB).
  Towns in the same state but different cities are 4 steps apart (TownA -> CityX -> StateY <- CityY <- TownB).
  Finding towns exactly 3 steps apart might be impossible or very rare depending on how the road network (relationships after loading) is actually laid out.

Therefore, this "no records" result is consistent with my designed graph structure. While LGAs are connected through various paths, paths of precisely length 3 appear to be structurally absent or were not formed by the specific connections present in the loaded dataset. Tests confirmed paths of length 2 and 4 exist, further supporting that the query executed correctly but found no matches for the specific length-3 requirement.

- **Supporting Evidence Screenshots:**

  <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429203920636.png" alt="image-20250429203920636" style="zoom: 40%;" />

### **(g) Weekday Pedestrian Crashes, Bus/HeavyRigid Involved, Specific Speed Zones**

I identified weekday crashes involving pedestrian fatalities where specific heavy vehicles (Bus or Heavy Rigid Truck) were present and the speed limit was in the low (<40) or high (>=100) ranges. The query then grouped these specific crash events by the combination of time of day, pedestrian age group, involved vehicle type, and speed limit, counting the number of unique crashes in each group.

```cypher
MATCH (p:Person {roadUser: 'Pedestrian'})-[:WAS_INVOLVED_IN]->(c:Crash)
WHERE c.dayOfWeekType = 'Weekday'
  AND (c.busInvolvement = 'yes' OR c.heavyRigidTruckInvolvement = 'yes')
  AND (c.speedLimit < 40 OR c.speedLimit >= 100)
  AND c.speedLimit IS NOT NULL AND p.ageGroup IS NOT NULL AND c.timeOfDay IS NOT NULL
WITH p, c,
   CASE
     WHEN c.busInvolvement = 'yes' AND c.heavyRigidTruckInvolvement = 'yes' THEN 'Bus and Heavy Rigid'
     WHEN c.busInvolvement = 'yes' THEN 'Bus'
     WHEN c.heavyRigidTruckInvolvement = 'yes' THEN 'Heavy Rigid Truck'
     ELSE 'Other'
   END AS vehicleType
RETURN c.timeOfDay AS TimeOfDay,
       p.ageGroup AS AgeGroup,
       vehicleType AS VehicleType,
       c.speedLimit AS SpeedLimitation,
       count(DISTINCT c.internalCrashID) AS CrashCount
ORDER BY
  CASE c.timeOfDay WHEN 'Day' THEN 1 WHEN 'Night' THEN 2 ELSE 3 END ASC,
  p.ageGroup ASC,
  VehicleType ASC,
  SpeedLimitation ASC;
```

- **Results Screenshot:**

  ![image-20250429205024631](/Users/haz/Library/Application Support/typora-user-images/image-20250429205024631.png)

- **Explanation:** This query provides a detailed breakdown of fatal crashes under specific high-risk conditions involving pedestrians on weekdays. Each row represents a unique combination of factors, showing the number of distinct crash events (`CrashCount`) matching that exact scenario. For instance, the results show that there are 2 crashes occurred during Day time involving 26_to_39 pedestrians with a 'Heavy Rigid Truck' in a 100 km/h zone, also 40_to_64 pedestrians with the same heavy vehicle and time period in 110km/h. This granular view helps identify which specific combinations contribute most to these severe incidents.

---

## **6. My Queries**

### **h1. Compare High-Speed vs. Low/Medium-Speed Zones**

- **Objective:** To investigate if certain driver age groups are disproportionately involved in fatal crashes occurring in high-speed zones (>= 100 km/h) compared to their involvement in crashes at lower speed zones (< 100 km/h).

- **Approach:** I matched `Person` nodes where `roadUser = 'Driver'` and linked them to their `Crash` node, ensuring valid `ageGroup` and `speedLimit`. Using `WITH`, I passed these attributes forward and then used conditional aggregation (`count(CASE...)`) to count driver fatalities separately for high-speed (>= 100) and lower-speed (< 100) crashes, grouping the results by `DriverAgeGroup`.

- **Query:**
  ```cypher
  MATCH (p:Person {roadUser: 'Driver'})-[:WAS_INVOLVED_IN]->(c:Crash)
  WITH p.ageGroup AS DriverAgeGroup, c.speedLimit AS speedLimit
  RETURN DriverAgeGroup,
         count(CASE WHEN speedLimit >= 100 THEN 1 END) AS HighSpeedFatalities,
         count(CASE WHEN speedLimit < 100 THEN 1 END) AS LowerSpeedFatalities
  ORDER BY DriverAgeGroup;
  ```
- **Results Screenshot:**
  <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429211548123.png" alt="image-20250429211548123" style="zoom:50%;" />
- **Explanation:** This query compares driver fatalities across age groups based on speed limit. The results show counts for high-speed (>= 100 km/h) and lower-speed zones. The '40_to_64' group had the most high-speed fatalities (960 vs. 608 lower-speed). The '75_or_older' group was the only one with more fatalities in lower-speed zones (364 vs. 305 high-speed). Other groups ('17_to_25', '26_to_39', '65_to_74') showed more fatalities in high-speed zones, suggesting high-speed environments pose a greater fatal risk for most driver age groups in this dataset.

### **h2. Heavy Vehicle Crashes by Remoteness Area**

- **Objective:** Operating heavy vehicles (trucks, buses) might pose different risks depending on the environment. Remote areas might have long distances and fatigue factors, while urban areas have congestion and complex intersections. Are fatal crashes involving these vehicles more common in one type of remoteness area than another?

- **Approach:** I matched `Crash` nodes, filtering for those where at least one heavy vehicle involvement flag was 'yes' and `nationalRemotenessAreas` was present. The query then grouped these crashes by `nationalRemotenessAreas` and counted the distinct crash events (`count(c)`) in each category, ordering by the count.

- **Query:**

  ```cypher
  MATCH (c:Crash)
  WHERE (c.busInvolvement = 'yes'
     OR c.heavyRigidTruckInvolvement = 'yes'
     OR c.articulatedTruckInvolvement = 'yes')
    AND c.nationalRemotenessAreas IS NOT NULL
  RETURN c.nationalRemotenessAreas AS RemotenessArea,
         count(c) AS HeavyVehicleCrashCount
  ORDER BY HeavyVehicleCrashCount DESC;
  ```

- **Results Screenshot:**

  **<img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429213404261.png" alt="image-20250429213404261" style="zoom:60%;" />**

- **Explanation:** This query aggregates fatal crashes involving heavy vehicles by remoteness category. 'Inner Regional Australia' had the highest count (522), followed closely by 'Major Cities' (493) and 'Outer Regional' (405). Remote and Very Remote areas had significantly fewer such crashes (70 and 61 respectively). This suggests fatal crashes involving heavy vehicles are most frequent in regional and major city areas in this dataset.

### **h3. High-Risk Pattern Analysis**

- **Objective:** Explore if there's a pattern involving young drivers (often less experienced) involved in crashes on roads typically associated with longer journeys or higher speeds (National/State Highways) outside major urban centers. Find the count of fatal crashes involving 'Young Drivers' (17_to_25) who were specifically the Driver, occurring on National or State Highway road types, grouped by the State and National Remoteness Areas (excluding 'Major Cities').

- **Query:**

  ```cypher
  MATCH (p:Person {ageGroup: '17_to_25', roadUser: 'Driver'})
         -[:WAS_INVOLVED_IN]->(c:Crash {nationalRoadType: 'National or State Highway'}) // Filter Road Type here
         -[:OCCURRED_IN]->(:LGA)-[:PART_OF]->(:SA4)-[:IN_STATE]->(st:State)
  WHERE c.nationalRemotenessAreas <> 'Major Cities of Australia' // Exclude Major Cities
  WITH st.name AS State,
       c.nationalRemotenessAreas AS RemotenessArea,
       count(DISTINCT c.internalCrashID) AS HighwayCrashCount

  WHERE HighwayCrashCount > 10 // Keep only combinations with more than 10 crashes

  RETURN State,
         RemotenessArea,
         HighwayCrashCount
  ORDER BY State ASC, HighwayCrashCount DESC;
  ```

- **Results Screenshot:**
  <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429220231941.png" alt="image-20250429220231941" style="zoom:50%;" />

- **Explanation:** This query identifies State/Remoteness combinations (outside major cities) with over 10 fatal highway crashes involving young drivers. 'Inner Regional Australia' in NSW had the most (44), followed by 'Outer Regional Australia' in QLD (32). NSW, QLD, VIC, SA, and WA all showed counts above 10 primarily in inner or outer regional zones, suggesting these areas represent higher concentrations of this specific risk scenario for young drivers on highways.

### **h4. Analyzing Secondary Fatalities in Multi-Fatality Crashes**

- **Objective:** In crashes where multiple people die (numberFatalities > 1), are the characteristics (age group, road user type) of the _other_ victims different depending on the characteristics of _one_ specific victim type (e.g., if a young driver died, who else tends to die in that same crash)? -- For multi-fatality crashes involving at least one 'Young Driver' (17-25, Driver), identify the roadUser types and ageGroups of the _other_ people who died in the _same crash_.

- **Query:**

  ```cypher
  MATCH (p_driver:Person {ageGroup: '17_to_25', roadUser: 'Driver'})
         -[:WAS_INVOLVED_IN]->(c:Crash)
  WHERE c.numberFatalities > 1
  WITH c
  MATCH (p_other)-[:WAS_INVOLVED_IN]->(c) // Find anyone involved in that crash 'c'

  WHERE p_other.roadUser IS NOT NULL AND p_other.ageGroup IS NOT NULL

  RETURN p_other.roadUser AS OtherVictimRoadUser,
         p_other.ageGroup AS OtherVictimAgeGroup,
         count(p_other) AS CoFatalityCount // Count the other victims
  ORDER BY CoFatalityCount DESC
  LIMIT 20;
  ```

- **Results Screenshot:**
  <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429221308554.png" alt="image-20250429221308554" style="zoom:50%;" />

- **Explanation:** This query reveals the profiles of all victims in multi-fatality crashes involving a young driver. The most frequent profile found was 'Driver' aged '17_to_25' (152 occurrences), including the initial young drivers. 'Passenger' aged '17_to_25' was the next most common (78), indicating young passengers are frequently involved. Passengers aged '26_to_39' (20) and '0_to_16' (18) were also present, suggesting young drivers in these crashes may have older or younger passengers. Fatalities involving other drivers in these specific incidents were less common according to the results.

---

## **7. Potential Applications of Graph Data Science : LGA Crash Profile Clustering via Community Detection**

Having explored the data with specific Cypher queries, I considered how Graph Data Science (GDS) could offer further insights by analyzing the overall graph structure. I believe **Community Detection** represents a useful application for this dataset, specifically for **grouping Local Government Areas (LGAs) based on similarities in their fatal crash profiles**, potentially revealing shared risk factors that cross administrative boundaries.

- **Goal:** My objective was to move beyond administrative groupings (like SA4s used in Query e) and identify clusters of LGAs where the _types_ and _circumstances_ of fatal crashes are statistically similar. Standard groupings might obscure patterns if diverse LGAs are combined. Finding communities based on shared crash characteristics (e.g., several LGAs, regardless of location, experiencing high rates of nighttime single-vehicle crashes) could highlight common underlying issues (like specific road designs or traffic patterns) more effectively. While complex Cypher _could_ compare specific factors, GDS algorithms are better suited for this multi-faceted similarity analysis across many LGAs. Identifying such data-driven communities would allow for more targeted and potentially efficient safety interventions tailored to the specific risk profile shared by the LGAs within a community.

- **Algorithm Selection:** For this clustering task, **Community detection** algorithms seemed most appropriate. Based on research [1, 2] and common GDS practices, the **Louvain Modularity algorithm** [1] stands out. It's known for its efficiency on larger graphs and its method of optimizing network modularity – essentially finding groups where internal similarity (represented by edge weights in our case) is high relative to external similarity [1]. Comparative studies also often show modularity-based methods like Louvain perform well in finding meaningful structures in real-world networks [2].

- **Application Process:** Applying Louvain requires constructing a graph where connections represent similarity:

  1. **Graph Projection:** This is the critical setup step. A new graph would be projected containing only `:LGA` nodes.

  2. **Defining Relationships & Weights (Similarity):** An edge would be created between two LGAs (LGA1, LGA2) only if they meet a similarity threshold. The **edge weight** must quantify their crash profile similarity. I considered different ways to calculate this:

     - _Using Feature Vectors:_ One method involves creating a feature vector for each LGA summarizing its crash data (e.g., normalized counts of `crashType`s, `roadUser` victims). Techniques exist for representing nodes via attribute statistics [3]. The similarity (and thus edge weight) between two LGAs could then be calculated using a metric like cosine similarity on these vectors [3].

     - _Using Shared Events:_ Alternatively, similarity could be based on counts of specific shared crash characteristics (e.g., number of crashes involving pedestrians at night occurring in _both_ LGAs).

     - _Conceptual Query:_ The GDS projection query would need to implement the chosen similarity calculation to create the weighted edges:

       ```cypher
       // Conceptual GDS Projection - Defines LGA nodes & similarity-weighted edges
       CALL gds.graph.project.cypher(
         'lgaSimilarityGraph',
         'MATCH (l:LGA) RETURN elementId(l) AS id', // Nodes
         // Relationship query calculates similarity weight between LGA pairs
         'MATCH (l1:LGA), (l2:LGA) WHERE elementId(l1) < elementId(l2)
          WITH l1, l2, custom.calculate_crash_similarity(l1, l2) AS similarityScore // Placeholder [3]
          WHERE similarityScore > 0.1 // Example threshold
          RETURN elementId(l1) AS source, elementId(l2) AS target, similarityScore AS weight'
       ) // ... YIELD ...
       ```

  3. **Louvain Execution:** The Louvain algorithm is then run on this projected graph, using the similarity score as the weight:

     ```cypher
     // Conceptual GDS Louvain Execution
     CALL gds.louvain.stream('lgaSimilarityGraph', { relationshipWeightProperty: 'weight' })
     YIELD nodeId, communityId
     RETURN gds.util.asNode(nodeId).name AS LGAName, communityId ORDER BY communityId;

     ```

  4. **Interpretation:** The results group LGAs by `communityId`. The key analysis is then to examine the dominant crash characteristics _within_ each community to understand its defining risk profile.

- **Why Not Other Methods?** While techniques like node embeddings (Node2Vec, FastRP) followed by K-Means clustering could also group LGAs, I felt the community detection approach with an explicitly defined similarity metric (even if complex to formulate perfectly) offered a more directly interpretable link between the graph structure (shared crash characteristics defining similarity edges) and the resulting clusters for this specific application goal.
- **Reference:**
  [1] V. D. Blondel, J.-L. Guillaume, R. Lambiotte, and E. Lefebvre, "Fast unfolding of communities in large networks," Journal of Statistical Mechanics: Theory and Experiment, vol. 2008, no. 10, p. P10008, Oct. 2008. doi: 10.1088/1742-5468/2008/10/p10008.
  [2] J. Leskovec, K. J. Lang, and M. Mahoney, "Empirical comparison of algorithms for network community detection," in Proceedings of the 19th International Conference on World Wide Web (WWW '10), New York, NY, USA: Association for Computing Machinery, 2010, pp. 631–640. doi: 10.1145/1772690.1772755.
  [3] J. Gibert, E. Valveny, and H. Bunke, "Graph embedding in vector spaces by node attribute statistics," Pattern Recognition, vol. 45, no. 9, pp. 3072-3083, Sep. 2012. doi: 10.1016/j.patcog.2012.01.009.

---

## **8. Conclusion**

This project involved designing, building, and querying a Neo4j graph database to analyze the provided Australian road fatality data. I developed a graph model centered around unique `Crash` events connected to involved `Person` fatalities and a hierarchical `LGA-SA4-State` location structure, which proved effective in handling the original dataset's redundancy and supporting relationship-based queries.

The ETL phase, using Python and pandas, was a critical step in transforming the source CSV. This involved standardizing column names, generating unique node IDs (especially the `internalCrashID` for deduplicated crashes), handling data types, and creating separate node and relationship import files. During validation, I identified and corrected an issue in the initial logic for the LGA-to-SA4 (`:PART_OF`) relationship to ensure accurate mapping (detailed in Appendix A). Loading the data into Neo4j also required adapting my approach; due to execution stalls with large scripts in Neo4j Browser, I successfully used a step-by-step method, running commands in logical blocks.

With the graph database populated and indexed, I executed the required Cypher queries (a-g) along with several self-designed ones (h1-h4). These queries allowed exploration of specific patterns, for example, confirming the relatively low number of multi-fatality crashes involving articulated trucks in WA (Query a), identifying the age range of male motorcyclists in specific holiday/regional crashes (Query b), highlighting the concentration of heavy vehicle related fatalities in regional/urban areas (Query h2), and revealing the common co-occurrence of young passengers in fatal crashes involving young drivers (Query h4).

Beyond these queries, I considered how Graph Data Science, specifically using Community Detection algorithms like Louvain [1] on a derived LGA similarity graph, could offer deeper insights by identifying clusters of LGAs with shared crash risk profiles, potentially guiding more targeted safety strategies.

While the final graph model effectively addressed the project requirements, potential future work could involve more detailed time or vehicle modeling. The analysis is also inherently limited by the scope of the provided fatality data. Overall, this project provided valuable experience in applying graph database principles to a real-world dataset, including navigating practical ETL and data loading challenges to enable meaningful graph-based analysis.

---

## Appendix

### Appendix A: Data Loading Troubleshooting Summary

Several issues were encountered during the Neo4j data loading phase:

1. **Script Execution Stall:** Initial attempts to run the complete data loading Cypher script in the Neo4j Browser resulted in the process hanging after `CREATE CONSTRAINT` commands, displaying "waiting for it's turn...". Standard troubleshooting (restarts, permission checks, config changes like `allow_csv_import_from_file_urls=true`, macOS Full Disk Access) did not resolve this specific stall.

   - **Resolution:** Adopted a manual, block-by-block execution strategy within Neo4j Browser, running constraints, then each `LOAD CSV` for nodes, then each `LOAD CSV` for relationships, and finally `CREATE INDEX` commands separately. This sequential approach bypassed the execution stall and allowed successful data population.

2. **ETL Correction for `:PART_OF` Relationship:** When checking query results with raw data regarding Question c, found an anomaly. Therefore I used another query to count specific persons (`count(p) = 81`) yielded a different, higher count (`count(p) = 112`) when the query pattern required traversing the full location hierarchy (`Person -> Crash -> LGA -> SA4 -> State`).

   ```cypher
   // Diagnostic Query: Count people matching core criteria WITH a complete location path
   MATCH (p:Person {ageGroup: '17_to_25', roadUser: 'Driver'})
          -[:WAS_INVOLVED_IN]->(c:Crash)
          -[:OCCURRED_IN]->(:LGA)
          -[:PART_OF]->(:SA4)
          -[:IN_STATE]->(:State)
   WHERE c.year = 2024
   RETURN count(p) AS CountWithFullPath;
   ```

- **Diagnosis:** Using `MATCH (l:LGA)-[:PART_OF]->(s4:SA4) WITH l, count(s4) as cnt WHERE cnt > 1 RETURN l.name, cnt` confirmed that the initial ETL process had incorrectly generated multiple `:PART_OF` relationships originating from single `LGA` nodes, linking them to several different `SA4` nodes. This "fan-out" caused the inaccurate counting in path-dependent queries. The relationship count for `:PART_OF` was initially 582, higher than the number of unique LGAs (509).

- <img src="/Users/haz/Library/Application Support/typora-user-images/image-20250429144532650.png" alt="image-20250429144532650" style="zoom: 50%;" />

- **Resolution:** The ETL script ( Cell 6, Relationship _2. LGA -> SA4_) was corrected to enforce a unique LGA-to-SA4 mapping by using `lgas_data.drop_duplicates(subset=['national_lga_name_2024'], keep='first')` before creating the `rels_lga_sa4.csv` file. This ensures that each LGA name is associated with only the first corresponding SA4 name encountered, thus enforcing a unique mapping. -> re-executed and overwrite the import csv -> update in Neo4j Import folder

- Then use `MATCH (n) DETACH DELETE n;` to clean Neo4j database (all nodes and relationships)

- then reloaded relationships step-by-step using the corrected CSV file, and indexes were recreated. Post-correction validation confirmed that each LGA had only one outgoing `:PART_OF` relationship (total count became 509), and subsequent queries yielded consistent results aligned with expectations.

![image-20250429180912853](/Users/haz/Library/Application Support/typora-user-images/image-20250429180912853.png)

"no records" result shows that `:PART_OF` relationships are now clean and represent the intended unique mapping.

### Appendix B: GenAI Declaration

In preparing this project report and developing the associated code, generative AI (specifically ChatGPT 4) was utilized as an assistive tool. Its use included:

- **Code Explanation and Debugging:** Helping to understand Cypher query logic, explaining error messages encountered during Neo4j loading, and suggesting potential Cypher or Python code corrections based on error descriptions.
- **Generating Code Snippets:** Providing example code snippets for Python/pandas ETL operations and Cypher queries based on specified logic (e.g., the corrected LGA->SA4 mapping logic, diagnostic queries).
- **Brainstorming and Refining Ideas:** Generating initial suggestions for self-designed queries (Section 6) and potential Graph Data Science applications (Section 7).

---
