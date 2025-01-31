# Incremental ETL Process for AdventureWorks

## Project Overview
This repository contains an Incremental ETL process using SQL scripts for a Data Warehouse built from the AdventureWorks_Basics database. The ETL process includes logging, dimension table synchronization, and fact table population.

## Change Log
- **2021-01-17** - RRoot - Created initial ETL process.
- **2025-01-27** - GObse - Completed ETL implementation.

## Database and ETL Process
### Data Sources
- **AdventureWorks_Basics Database**: The source OLTP database.
- **DWAdventureWorks_BasicsWithSCD**: The target Data Warehouse containing Slowly Changing Dimensions (SCD).

### ETL Steps
1. **Setup**: Restore `AdventureWorks_Basics` and create `DWAdventureWorks_BasicsWithSCD`.
2. **Create ETL Logging Tables**: Logs ETL actions and errors.
3. **Incremental Data Processing**:
   - **DimDates**: Inserts date-based records.
   - **DimProducts**: Extracts and updates product data with SCD handling.
   - **DimCustomers**: Maintains customer information with incremental updates.
   - **FactOrders**: (Upcoming) Populates fact table with sales data.

### Key Stored Procedures and Views
- `pETLFillDimDates` – Populates `DimDates`.
- `pETLSyncDimProducts` – Syncs `DimProducts` with SCD logic.
- `pETLSyncDimCustomers` – Syncs `DimCustomers` with SCD logic.
- `vETLDimProducts`, `vETLDimCustomers`, `vETLFactOrders` – Views for ETL transformations.
- `pInsETLLog` – Logs ETL events.

## Usage
1. **Restore AdventureWorks_Basics** (if not already available).
2. **Run the SQL scripts in sequence**:
   - Create the Data Warehouse (`DWAdventureWorks_BasicsWithSCD`).
   - Run ETL scripts to populate dimensions and fact tables.
3. **Validate Results**:
   - Check logs using `SELECT * FROM vETLLog;`
   - Verify table contents using `SELECT * FROM DimCustomers;`, etc.

## Future Enhancements
- Complete `FactOrders` ETL.
- Automate scheduling via SQL Agent.
- Optimize performance with indexing strategies.

## License
This project is open-source. Feel free to contribute or modify as needed.

---

Happy Coding! 🚀
