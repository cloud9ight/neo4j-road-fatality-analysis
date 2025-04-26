// Schema and Load Script
// ==========================================
// Purpose: Creates constraints, loads nodes and relationships from CSV files,
//          and creates indexes for the road fatality graph database.
// Usage: Copy and paste this entire script into the Neo4j Browser
//        (ensure multi-statement execution is enabled in Browser settings)
//        and run it once. Assumes CSV files are in the Neo4j 'import' directory.
// ==========================================

// 0. Cleanup (Optional: Run this ONLY if you need to completely reset the database before loading)
// WARNING: This deletes ALL data in the current database. Uncomment with extreme caution.
// MATCH (n) DETACH DELETE n;
// :echo "Database cleanup complete (if uncommented)."

// 1. Create Constraints
// Ensures uniqueness and speeds up MERGE/MATCH operations on these identifiers.
// Run these first before loading any data.
:echo "Creating constraints..."
CREATE CONSTRAINT unique_person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.personID IS UNIQUE;
CREATE CONSTRAINT unique_crash_id IF NOT EXISTS FOR (c:Crash) REQUIRE c.internalCrashID IS UNIQUE;
CREATE CONSTRAINT unique_lga_name IF NOT EXISTS FOR (l:LGA) REQUIRE l.name IS UNIQUE;
CREATE CONSTRAINT unique_sa4_name IF NOT EXISTS FOR (s4:SA4) REQUIRE s4.name IS UNIQUE;
CREATE CONSTRAINT unique_state_name IF NOT EXISTS FOR (st:State) REQUIRE st.name IS UNIQUE;
:echo "Waiting for constraints indexes to come online..."
CALL db.awaitIndexes(300); // Wait up to 300 seconds (5 minutes) for indexes backing constraints
:echo "Constraints created and indexes online."

// 2. Load Nodes
// Load data for each node type using MERGE to avoid duplicates based on the unique constraint.
// 'file:///filename.csv' assumes files are directly in the database's import folder.

// Load States
:echo "Loading States from states.csv..."
LOAD CSV WITH HEADERS FROM 'file:///states.csv' AS row FIELDTERMINATOR ','
MERGE (st:State {name: row.`name:ID(State)`});

// Load SA4s
:echo "Loading SA4s from sa4s.csv..."
LOAD CSV WITH HEADERS FROM 'file:///sa4s.csv' AS row FIELDTERMINATOR ','
MERGE (s4:SA4 {name: row.`name:ID(SA4)`});

// Load LGAs
:echo "Loading LGAs from lgas.csv..."
LOAD CSV WITH HEADERS FROM 'file:///lgas.csv' AS row FIELDTERMINATOR ','
MERGE (l:LGA {name: row.`name:ID(LGA)`});

// Load Persons
:echo "Loading Persons from persons.csv..."
LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS row FIELDTERMINATOR ','
MERGE (p:Person {personID: toInteger(row.`personID:ID(Person)`)})
ON CREATE SET // Set properties only when node is created
    p.roadUser = row.roadUser,
    p.gender = row.gender,
    p.age = toInteger(row.age), // Convert string 'age' from CSV to integer
    p.ageGroup = row.ageGroup
ON MATCH SET // Optionally update properties if node already exists (usually not needed with MERGE ON ID)
    p.roadUser = row.roadUser,
    p.gender = row.gender,
    p.age = toInteger(row.age),
    p.ageGroup = row.ageGroup;


// Load Crashes
:echo "Loading Crashes from crashes.csv..."
LOAD CSV WITH HEADERS FROM 'file:///crashes.csv' AS row FIELDTERMINATOR ','
MERGE (c:Crash {internalCrashID: toInteger(row.`internalCrashID:ID(Crash)`)})
ON CREATE SET // Set properties only when node is created
    c.crashID = row.crashID, // Original Crash ID as property
    c.year = toInteger(row.year),
    c.month = toInteger(row.month),
    c.dayweek = row.dayweek,
    c.time = row.time,
    c.crashType = row.crashType,
    c.numberFatalities = toInteger(row.numberFatalities),
    c.busInvolvement = row.busInvolvement,
    c.heavyRigidTruckInvolvement = row.heavyRigidTruckInvolvement,
    c.articulatedTruckInvolvement = row.articulatedTruckInvolvement,
    c.speedLimit = CASE WHEN row.speedLimit IS NULL OR row.speedLimit = '' OR row.speedLimit = '-1' THEN null ELSE toInteger(row.speedLimit) END, // Handle potential non-numeric/placeholder values
    c.nationalRoadType = row.nationalRoadType,
    c.christmasPeriod = row.christmasPeriod,
    c.easterPeriod = row.easterPeriod,
    c.nationalRemotenessAreas = row.nationalRemotenessAreas,
    c.dayOfWeekType = row.dayOfWeekType,
    c.timeOfDay = row.timeOfDay
ON MATCH SET // Optionally update properties if node already exists
    c.crashID = row.crashID,
    c.year = toInteger(row.year),
    c.month = toInteger(row.month),
    c.dayweek = row.dayweek,
    c.time = row.time,
    c.crashType = row.crashType,
    c.numberFatalities = toInteger(row.numberFatalities),
    c.busInvolvement = row.busInvolvement,
    c.heavyRigidTruckInvolvement = row.heavyRigidTruckInvolvement,
    c.articulatedTruckInvolvement = row.articulatedTruckInvolvement,
    c.speedLimit = CASE WHEN row.speedLimit IS NULL OR row.speedLimit = '' OR row.speedLimit = '-1' THEN null ELSE toInteger(row.speedLimit) END,
    c.nationalRoadType = row.nationalRoadType,
    c.christmasPeriod = row.christmasPeriod,
    c.easterPeriod = row.easterPeriod,
    c.nationalRemotenessAreas = row.nationalRemotenessAreas,
    c.dayOfWeekType = row.dayOfWeekType,
    c.timeOfDay = row.timeOfDay;
:echo "Node loading complete."


// 3. Load Relationships
// Use PERIODIC COMMIT to handle potentially large relationship files without running out of memory.
// MATCH existing nodes using their unique identifiers before creating relationships.

// Load SA4 -> State relationships
:echo "Loading SA4 -> State relationships from rels_sa4_state.csv..."
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rels_sa4_state.csv' AS row FIELDTERMINATOR ','
MATCH (s4:SA4 {name: row.`:START_ID(SA4)`})
MATCH (st:State {name: row.`:END_ID(State)`})
MERGE (s4)-[r:IN_STATE]->(st);

// Load LGA -> SA4 relationships
:echo "Loading LGA -> SA4 relationships from rels_lga_sa4.csv..."
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rels_lga_sa4.csv' AS row FIELDTERMINATOR ','
MATCH (l:LGA {name: row.`:START_ID(LGA)`})
MATCH (s4:SA4 {name: row.`:END_ID(SA4)`})
MERGE (l)-[r:PART_OF]->(s4);

// Load Crash -> LGA relationships
:echo "Loading Crash -> LGA relationships from rels_crash_lga.csv..."
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rels_crash_lga.csv' AS row FIELDTERMINATOR ','
MATCH (c:Crash {internalCrashID: toInteger(row.`:START_ID(Crash)`)})
MATCH (l:LGA {name: row.`:END_ID(LGA)`})
MERGE (c)-[r:OCCURRED_IN]->(l);

// Load Person -> Crash relationships
:echo "Loading Person -> Crash relationships from rels_person_crash.csv..."
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rels_person_crash.csv' AS row FIELDTERMINATOR ','
MATCH (p:Person {personID: toInteger(row.`:START_ID(Person)`)})
MATCH (c:Crash {internalCrashID: toInteger(row.`:END_ID(Crash)`)})
MERGE (p)-[r:WAS_INVOLVED_IN]->(c);
:echo "Relationship loading complete."


// 4. Create Indexes
// Create indexes on properties frequently used in WHERE clauses for better query performance.
// These are different from constraints; they index non-unique properties.
:echo "Creating additional indexes..."
CREATE INDEX idx_crash_year IF NOT EXISTS FOR (c:Crash) ON (c.year);
// Example composite index - adjust properties based on frequent combined filtering
CREATE INDEX idx_crash_trucks IF NOT EXISTS FOR (c:Crash) ON (c.articulatedTruckInvolvement, c.busInvolvement, c.heavyRigidTruckInvolvement);
CREATE INDEX idx_crash_timeofday IF NOT EXISTS FOR (c:Crash) ON (c.timeOfDay);
CREATE INDEX idx_crash_dayweektype IF NOT EXISTS FOR (c:Crash) ON (c.dayOfWeekType);
CREATE INDEX idx_crash_holiday IF NOT EXISTS FOR (c:Crash) ON (c.christmasPeriod, c.easterPeriod);
CREATE INDEX idx_crash_remoteness IF NOT EXISTS FOR (c:Crash) ON (c.nationalRemotenessAreas);
CREATE INDEX idx_crash_speed IF NOT EXISTS FOR (c:Crash) ON (c.speedLimit);
CREATE INDEX idx_crash_time IF NOT EXISTS FOR (c:Crash) ON (c.time); // Index on time string if used in WHERE
CREATE INDEX idx_person_agegroup IF NOT EXISTS FOR (p:Person) ON (p.ageGroup);
CREATE INDEX idx_person_roaduser IF NOT EXISTS FOR (p:Person) ON (p.roadUser);
CREATE INDEX idx_person_gender IF NOT EXISTS FOR (p:Person) ON (p.gender);
:echo "Waiting for indexes to come online..."
CALL db.awaitIndexes(300); // Wait up to 5 minutes again
:echo "Index creation complete."


:echo "------------------------------------------"
:echo " Database Loading Script Finished "
:echo "------------------------------------------"

// ==========================================
// Query Section Starts Below (Add later)
// ==========================================