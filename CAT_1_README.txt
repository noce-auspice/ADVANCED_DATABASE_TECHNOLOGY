# Agricultural Farm Production and Supply Management System

## Project Overview

A comprehensive farm management database system designed to track agricultural production from planting to sales. This system helps farmers monitor production efficiency, input costs, and profitability across different crops and fields.

## Database Schema

### Table Structure

#### 1. Field Table
- Purpose: Stores field location and characteristics
- Key Fields: FieldID (PK), SizeHectares, Location, SoilType
- Constraints: SizeHectares > 0, all fields NOT NULL

#### 2. Crop Table  
- Purpose: Tracks crop planting and growth cycles
- Key Fields: CropID (PK), FieldID (FK), PlantingDate, HarvestDate, Status
- Constraints: HarvestDate ≥ PlantingDate, Status in predefined values

#### 3. Fertilizer Table
- Purpose: Records fertilizer applications and costs
- Key Fields: FertilizerID (PK), CropID (FK), QuantityUsed, Cost, DateApplied
- Constraints: QuantityUsed > 0, Cost ≥ 0

#### 4. Worker Table
- Purpose: Manages farm labor information
- Key Fields: WorkerID (PK), Role, DailyWage, Contact
- Constraints: DailyWage ≥ 0

#### 5. Harvest Table
- Purpose: Records harvest yields and quality
- Key Fields: HarvestID (PK), CropID (FK), WorkerID (FK), QuantityKG, Grade
- Constraints: QuantityKG > 0, Grade in (A,B,C,D)

#### 6. Sale Table
- Purpose: Tracks sales transactions
- Key Fields: SaleID (PK), HarvestID (FK), QuantitySold, PricePerKG, SaleDate
- Constraints: QuantitySold > 0, PricePerKG > 0

##  Database Relationships

Field (1) → (N) Crop (1) → (N) Fertilizer
↓
(1) → (N) Harvest (1) → (N) Sale
↑
Worker (1) → (N) Harvest
