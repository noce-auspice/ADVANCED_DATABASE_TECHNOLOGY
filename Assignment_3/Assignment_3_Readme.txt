
README FILE


PROJECT TITLE:
Agricultural Farm Production and Supply Management System

COURSE:
Advanced Database Management Systems (ADMS)
Topic: Parallel and Distributed Databases

STUDENT:
IYIZIRE Noce Auspice / 224020025
University of Rwanda, College of Business and Economics
African Center of Excellence in Data Science (ACE-DS)

INSTRUCTOR:
Rukundo Prince


1. PROJECT OVERVIEW

This project implements a distributed and parallel database system 
for an Agricultural Farm Production and Supply Management System. 
The goal is to demonstrate how database fragmentation, distribution, 
and parallelization can improve scalability, performance, and 
availability across multiple farm branches.

The system manages key agricultural data such as:
- Crop production
- Field management
- Supply distribution
- Farmer records
- Harvest tracking
- Transactions between branches

The project is implemented using Oracle Database 19c, focusing on 
distributed schema design, database links, parallel query execution, 
and transaction management.


2. OBJECTIVES

• To design and implement a distributed agricultural database 
  across two branches (BranchDB_A and BranchDB_B).
• To use database links for remote queries and distributed joins.
• To evaluate parallel query performance and efficiency.
• To simulate two-phase commit (2PC) and recovery in distributed 
  transactions.
• To analyze concurrency control and locking mechanisms.
• To model a three-tier client–server architecture for the system.


3. SYSTEM COMPONENTS

Main tables include:
• CROP (FieldID, CropName, PlantingDate, Status, HarvestDate)
• FARMER (FarmerID, Name, Contact, Address)
• SUPPLY (SupplyID, CropID, Quantity, Destination, Date)
• TRANSACTION (TransID, CropID, Buyer, Quantity, Price, Date)

Each branch database stores part of the data using horizontal 
fragmentation (e.g., crops by region or farmer group).


4. IMPLEMENTED TASKS (Based on Lab Assessment)

Task 1: Distributed Schema Design and Fragmentation  
Task 2: Create and Use Database Links  
Task 3: Parallel Query Execution  
Task 4: Two-Phase Commit Simulation  
Task 5: Distributed Rollback and Recovery  
Task 6: Distributed Concurrency Control  
Task 7: Parallel Data Loading / ETL Simulation  
Task 8: Three-Tier Architecture Design  
Task 9: Distributed Query Optimization  
Task 10: Performance Benchmark and Report

Each task is implemented with SQL/PLSQL scripts, screenshots, 
and brief analysis as required.


5. FILE STRUCTURE

|-- README.txt
|-- scripts/
|    |-- farm_db_schema.sql
|    |-- branchA_schema.sql
|    |-- branchB_schema.sql
|    |-- dblink_setup.sql
|    |-- parallel_query.sql
|    |-- distributed_transaction.sql
|-- report/
|    |-- Agricultural_Farm_DB_Report.pdf
|    |-- ER_Diagram.png
|    |-- Three_Tier_Architecture.png


6. EXECUTION INSTRUCTIONS

1. Create two Database schemas: BranchDB_A and BranchDB_B.
2. Run the schema scripts in each branch database.
3. Establish a database link between the two branches.
4. Execute SQL queries in order as defined in the .sql files.
5. Observe output and performance metrics using EXPLAIN PLAN, 
   AUTOTRACE, and DBMS_XPLAN.DISPLAY.
6. Document your results in the lab report (PDF).


7. TECHNOLOGIES USED

• posgres Database   
• postgres SQL Developer  
• SQL  
• Parallel DML / Query Hinting  
• dblink (Database Link)  
• Two-Phase Commit (2PC)


8. EXPECTED OUTCOMES

By completing this project, the Agricultural Farm Production and 
Supply Management System should:
• Demonstrate distributed database design using fragmentation.
• Successfully execute remote queries through database links.
• Show improved performance through parallel execution.
• Handle distributed transactions with atomicity and recovery.
• Present an efficient three-tier client–server design.


9. NOTE

This project highlights the importance of database distribution 
and parallelism in real-world agricultural systems, ensuring 
efficient resource allocation, timely supply management, and 
data-driven decision-making for sustainable farming.


10. CONTACT INFORMATION

Name: IYIZIRE Noce Auspice
Email: noceauspice@gmail.com
Institution: University of Rwanda, ACE-DS


END OF README

