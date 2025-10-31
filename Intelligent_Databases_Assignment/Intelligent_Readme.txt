#  Intelligent Databases – PostgreSQL Implementation

This repository contains PostgreSQL solutions for the **Intelligent Databases Assignment, which explores multiple advanced database concepts — including declarative constraints, triggers, recursion, ontologies, and spatial reasoning** using PostGIS.


## Assignment Overview

| Task | Topic | Description |
|------|--------|--------------|
| 1 | Rules (Declarative Constraints) | Enforcing data integrity for safe medical prescriptions using table constraints. |
| 2 | Active Databases (E–C–A Triggers) | Maintaining correct bill totals automatically via statement-level triggers. |
| 3 | Deductive Databases (Recursive WITH) | Using recursion to find the top supervisor and the chain length for employees. |
| 4 | Knowledge Bases (Triples & Ontology) | Computing ontology-driven inferences for infectious diseases. |
| 5 | Spatial Databases (PostGIS) | Using spatial queries to find nearby and nearest clinics within a given radius. |



## 1. Safe Prescriptions (Declarative Constraints)

File: `1_safe_prescriptions.sql`

Defines the `PATIENT_MED` table under the `HEALTHNET` schema with strong integrity constraints:
- Ensures non-negative dosage
- Enforces referential integrity to `PATIENT`
- Validates logical date ranges

Test: Includes two valid inserts and two that should fail (negative dose and inverted dates).



##  2. Active Databases (E–C–A Trigger)

File: `2_bill_totals_trigger.sql`

Implements a statement-level trigger that:
- Automatically updates each bill’s total after any item change (INSERT/UPDATE/DELETE).
- Inserts an audit record in `BILL_AUDIT` for traceability.
- Avoids mutating table errors and redundant recalculations.

Test: A mixed DML script that validates totals and audit records.



##  3. Deductive Databases (Recursive WITH)

File: `3_supervision_chain.sql`

Uses a recursive CTE to compute:
- Each employee’s top supervisor.
- The number of hops (levels) to reach them.
- A cycle guard to avoid infinite recursion.

Example Output:
| emp | top_supervisor | hops |
|------|----------------|------|
| Alice | Diana | 3 |
| Frank | Bob | 2 |



##  4. Knowledge Bases (Triples & Ontology)

File: `4_infectious_disease_rollup.sql`

Models facts as RDF-style triples (`subject`, `predicate`, `object`) and computes:
- The transitive closure of `isA` relationships.
- All patients whose diagnoses ultimately classify under `InfectiousDisease`.

Example Triples:



Output:  
All patients whose diagnosis belongs (directly or indirectly) to the `InfectiousDisease` category.



## 5. Spatial Databases (PostGIS)

File: `5_spatial_queries.sql`

Uses PostGIS to:
- Find all clinics within 1 km of an ambulance location.
- List the 3 nearest clinics with distances in kilometers.

Example Queries:
```sql
 Clinics within 1km
SELECT name FROM clinic
WHERE ST_DWithin(geom, ST_GeogFromText('SRID=4326;POINT(30.0600 -1.9570)'), 1000);

 Nearest 3
SELECT name, ROUND(ST_Distance(geom, amb_point)/1000,2) AS km
FROM clinic, (SELECT ST_GeogFromText('SRID=4326;POINT(30.0600 -1.9570)') AS amb_point) a
ORDER BY km
LIMIT 3;
