--*************************************************************************--
-- Title: Assignment02
-- Author: Girum.Obse
-- Desc: This file tests you knowlege on how to create a Incremental ETL process with SQL code
-- Change Log: When,Who,What
-- 2021-01-17,RRoot,Created File
-- 2025-01-27,GObse,Completed File

-- Instructions: 
-- (STEP 1) Restore the AdventureWorks_Basics database by running the provided code.
-- (STEP 2) Create a new Data Warehouse called DWAdventureWorks_BasicsWithSCD based on the AdventureWorks_Basics DB.
--          The DW should have three dimension tables (for Customers, Products, and Dates) and one fact table.
-- (STEP 3) Fill the DW by creating an Incremental ETL Script
--**************************************************************************--
USE [DWAdventureWorks_BasicsWithSCD];
go
SET NoCount ON;

--  Setup Logging Objects ----------------------------------------------------

If NOT Exists(Select * From Sys.tables where Name = 'ETLLog')
  Create -- Drop
  Table ETLLog
  (ETLLogID int identity Primary Key
  ,ETLDateAndTime datetime Default GetDate()
  ,ETLAction varchar(100)
  ,ETLLogMessage varchar(2000)
  );
go

Create or Alter View vETLLog
As
  Select
   ETLLogID
  ,ETLDate = Format(ETLDateAndTime, 'D', 'en-us')
  ,ETLTime = Format(Cast(ETLDateAndTime as datetime2), 'HH:mm', 'en-us')
  ,ETLAction
  ,ETLLogMessage
  From ETLLog;
go


Create or Alter Proc pInsETLLog
 (@ETLAction varchar(100), @ETLLogMessage varchar(2000))
--*************************************************************************--
-- Desc:This Sproc creates an admin table for logging ETL metadata. 
-- Change Log: When,Who,What
-- 2020-01-01,RRoot,Created Sproc
--*************************************************************************--
As
Begin
  Declare @ReturnCode int = 0;
  Begin Try
    Begin Tran;
      Insert Into ETLLog
       (ETLAction,ETLLogMessage)
      Values
       (@ETLAction,@ETLLogMessage)
    Commit Tran;
    Set @ReturnCode = 1;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @ReturnCode = -1;
  End Catch
  Return @ReturnCode;
End
Go

--********************************************************************--
-- A) Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
 -- NOT NEEDED FOR INCREMENTAL LOADING: 
--********************************************************************--


--********************************************************************--
-- B) Synchronize the Tables
--********************************************************************--

/****** [dbo].[DimDates] ******/
Create or Alter Procedure pETLFillDimDates
/* Author: RRoot
** Desc: Inserts data Into DimDates
** Change Log: When,Who,What
** 20200117,RRoot,Created Sproc.
*/
AS
 Begin
  Declare @ReturnCode int = 0;
  Begin Try

    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/2000'
      Declare @EndDate datetime = '12/31/2010' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row Into the date dimension table for this date
       Begin Tran;
       Insert Into DimDates 
       ( [DateKey], [FullDate],[FullDateName],[MonthID],[MonthName],[YearID],[YearName])
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        ,@DateInProcess -- [FullDate]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthID]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Year(@DateInProcess) -- [YearID] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       Commit Tran;
       End
    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimDates'
	       ,@ETLLogMessage = 'DimDates filled';
    Set @ReturnCode = +1
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsETLLog 
	     @ETLAction = 'pETLFillDimDates'
	    ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1;
  End Catch
  Return @ReturnCode;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
 Select * From vETLLog;
*/
go


/****** [dbo].[DimProducts] ******/
go 
Create or Alter View vETLDimProducts
/* Author: GObse
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 2025-01-27,GObse,Created Sproc.
*/
As
  Select
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.Name as nVarchar(50))
   ,[StandardListPrice] = Cast(p.ListPrice as decimal(18,4))
   ,[ProductSubCategoryID] = IsNull(ps.ProductSubcategoryID, -1)
   ,[ProductSubCategoryName] = CAST(ps.Name as nVarchar(50))
   ,[ProductCategoryID] = IsNull(pc.ProductCategoryID, -1)
   ,[ProductCategoryName] = CAST(pc.Name as nVarchar(50))
  From [AdventureWorks_Basics].dbo.ProductCategory as pc
  Inner Join [AdventureWorks_Basics].dbo.ProductSubcategory as ps
   On pc.ProductCategoryID = ps.ProductCategoryID
  Inner Join [AdventureWorks_Basics].dbo.Products as p
  ON ps.ProductSubcategoryID = p.ProductSubcategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create or Alter Procedure pETLSyncDimProducts
/* Author: <YourNameHere>
** Desc: Updates data in DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 2021-01-17,<YourNameHere>,Created Sproc.
*/
AS
 Begin
  Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    ---SELECT '<Your Code Here>' as TODO
    Begin Tran;
      -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows
      -- NOTE: Performing the Update before an Insert makes the coding eaiser since there is only one current version of the data      
      With ChangedProducts 
      As(
         Select ProductID, ProductName,StandardListPrice,ProductSubCategoryID,ProductSubCategoryName, ProductCategoryID, ProductCategoryName From vETLDimProducts
         Except
         Select ProductID, ProductName, StandardListPrice, ProductSubCategoryID,ProductSubCategoryName, ProductCategoryID, ProductCategoryName From DimProducts
           Where IsCurrent = 1 -- Needed if the value is changed back to previous value
        ) UPDATE [DWAdventureWorks_BasicsWithSCD].dbo.DimProducts 
           SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
              ,IsCurrent = 0
           WHERE ProductID IN (Select ProductID From ChangedProducts)
           ;

      -- 2)For INSERT or UPDATES: Add new rows to the table
      With AddedORChangedProducts 
        As(
            Select ProductID, ProductName,StandardListPrice,ProductSubCategoryID,ProductSubCategoryName, ProductCategoryID, ProductCategoryName From vETLDimProducts
         Except
         Select ProductID, ProductName, StandardListPrice, ProductSubCategoryID,ProductSubCategoryName, ProductCategoryID, ProductCategoryName From DimProducts
              Where IsCurrent = 1 -- Needed if the value is changed back to previous value
          ) INSERT INTO [DWAdventureWorks_BasicsWithSCD].dbo.DimProducts
            ([ProductID],[ProductName],[StandardListPrice],[ProductSubCategoryID],[ProductSubCategoryName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
            SELECT
              [ProductID]
             ,[ProductName]
			 ,[StandardListPrice]
			 ,[ProductSubCategoryID]
			 ,[ProductSubCategoryName]
             ,[ProductCategoryID]
             ,[ProductCategoryName]
             ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
             ,[EndDate] = Null
             ,[IsCurrent] = 1
            FROM vETLDimProducts
            WHERE ProductID IN (Select ProductID From AddedORChangedProducts)
            ;

      -- 3) For Delete: Change the IsCurrent status to zero
      With DeletedProducts 
          As(
              Select ProductID, ProductName,StandardListPrice,ProductSubCategoryID, ProductCategoryID, ProductCategoryName From DimProducts
                Where IsCurrent = 1 -- We do not care about row already marked zero!
              Except            			
              Select ProductID, ProductName,StandardListPrice,ProductSubCategoryID, ProductCategoryID, ProductCategoryName  From vETLDimProducts
            ) UPDATE [DWAdventureWorks_BasicsWithSCD].dbo.DimProducts 
                SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
                   ,IsCurrent = 0
                WHERE ProductID IN (Select ProductID From DeletedProducts)
                ;
     Commit Tran;


    -- ETL Logging Code --
    Exec pInsETLLog
	        @ETLAction = 'pETLSyncDimProducts'
	       ,@ETLLogMessage = 'DimProducts synced';
    Set @ReturnCode = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLSyncDimProducts'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimProducts;
 Print @Status;
 Select * From DimProducts Order By ProductID
*/


/****** [dbo].[DimCustomers] ******/
go 
Create or Alter View vETLDimCustomers
/* Author: Girum.Obse
** Desc: Extracts and transforms data for DimCustomers
** Change Log: When,Who,What
** 2025-01-27,GObse,Created Sproc.
*/

As
  select 
  [CustomerId] = [CustomerID]
, [CustomerFullName] = cast([FirstName]+ ' '+[LastName] As nvarchar (100) )
, [CustomerCityName] = cast([City] as nvarchar(50))
, [CustomerStateProvinceName] =cast([StateProvinceName] As nvarchar(50))
, [CustomerCountryRegionCode] = cast([CountryRegionCode] As nvarchar(50))
, [CustomerCountryRegionName] = cast([CountryRegionName] As nvarchar(50))
from [AdventureWorks_Basics].[dbo].[Customer]
go




/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create or Alter Procedure pETLSyncDimCustomers
/* Author: Girum.Obse
** Desc: Inserts data into DimCustomers
** Change Log: When,Who,What
** 2025-01-27,GObse,Created Sproc.
*/

AS
 Begin
  Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
      -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows  
      With ChangedCustomers
      As(
         Select CustomerId, CustomerFullName, CustomerCityName, CustomerStateProvinceName, CustomerCountryRegionCode, CustomerCountryRegionName From vETLDimCustomers
         Except
         Select CustomerId, CustomerFullName,CustomerCityName,CustomerStateProvinceName,CustomerCountryRegionCode, CustomerCountryRegionName From DimCustomers
           Where IsCurrent = 1 -- Needed if the value is changed back to previous value
        ) UPDATE [DWAdventureWorks_BasicsWithSCD].dbo.DimCustomers 
           SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
              ,IsCurrent = 0
           WHERE CustomerId IN (Select CustomerId From ChangedCustomers)
           ;

      -- 2)For INSERT or UPDATES: Add new rows to the table
      With AddedORChangedCustomers 
        As(
             Select CustomerId, CustomerFullName, CustomerCityName, CustomerStateProvinceName, CustomerCountryRegionCode, CustomerCountryRegionName From vETLDimCustomers
         Except
         Select CustomerId, CustomerFullName,CustomerCityName,CustomerStateProvinceName,CustomerCountryRegionCode, CustomerCountryRegionName From DimCustomers
              Where IsCurrent = 1 -- Needed if the value is changed back to previous value
          ) INSERT INTO [DWAdventureWorks_BasicsWithSCD].dbo.DimCustomers
            ([CustomerId],[CustomerFullName],[CustomerCityName],[CustomerStateProvinceName],[CustomerCountryRegionCode],[CustomerCountryRegionName],[StartDate],[EndDate],[IsCurrent])
            SELECT
               [CustomerId]
			  ,[CustomerFullName]
			  ,[CustomerCityName]
			  ,[CustomerStateProvinceName]
			  ,[CustomerCountryRegionCode]
			  ,[CustomerCountryRegionName]
              ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
              ,[EndDate] = Null
              ,[IsCurrent] = 1
            FROM vETLDimCustomers
            WHERE CustomerId IN (Select CustomerId From AddedORChangedCustomers)
            ;

      -- 3) For Delete: Change the IsCurrent status to zero
      With DeletedCustomers
          As(
               Select CustomerId, CustomerFullName, CustomerCityName, CustomerStateProvinceName, CustomerCountryRegionCode, CustomerCountryRegionName From vETLDimCustomers
         Except
         Select CustomerId, CustomerFullName,CustomerCityName,CustomerStateProvinceName,CustomerCountryRegionCode, CustomerCountryRegionName From DimCustomers
            ) UPDATE [DWAdventureWorks_BasicsWithSCD].dbo.DimCustomers 
                SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as date)
                   ,IsCurrent = 0
                WHERE CustomerId IN (Select CustomerId From DeletedCustomers)
                ;
     Commit Tran;


    -- ETL Logging Code --
    Exec pInsETLLog
	        @ETLAction = 'pETLSyncDimCustomers'
	       ,@ETLLogMessage = 'DimCustomers synced';
    Set @ReturnCode = +1
  End Try
  Begin Catch
  -- Error Handling Code --
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLSyncDimCustomers'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go


/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimCustomers;
 Print @Status;
*/

--Select * From DimCustomers Order By CustomerId
go


/****** [dbo].[FactOrders] ******/


Create or Alter View vETLFactOrders
/* Author: GObse
** Desc: Extracts and transforms data for FactOrders
** Change Log: When,Who,What
** 2025-01-27,GObse,Created Sproc.
*/
As
  SELECT [SalesOrderID] = SOH.[SalesOrderID]
        ,[SalesOrderDetailID] = SOD.SalesOrderDetailID
        ,[OrderDate] = Cast(SOH.OrderDate as Date)
        ,[OrderDateKey] = dD.DateKey
        ,[CustomerID] = SOH.CustomerID
        ,[CustomerKey] = dC.CustomerKey
        ,[ProductID] = SOD.ProductID
        ,[ProductKey] = dP.ProductKey
        ,[OrderQty] = Cast(SOD.OrderQty as Int)
        ,[ActualUnitPrice] = Cast(SOD.UnitPrice as decimal(18,4))
        FROM [AdventureWorks_Basics].[dbo].[SalesOrderHeader] as SOH
    JOIN [AdventureWorks_Basics].[dbo].[SalesOrderDetail] as SOD
     ON SOH.SalesOrderID = SOD.SalesOrderID
    JOIN [DWAdventureWorks_BasicsWithSCD].[dbo].[DimCustomers] as dC
     ON SOH.CustomerID = dC.CustomerId
    JOIN [DWAdventureWorks_BasicsWithSCD].[dbo].[DimDates] as dD
     ON SOH.OrderDate = dD.FullDate
    JOIN [DWAdventureWorks_BasicsWithSCD].[dbo].[DimProducts] as dP
     ON SOD.ProductID = dP.ProductID
go
/* Testing Code:
 Select * From  vETLFactOrders;
*/

go
Create or Alter Procedure pETLSyncFactOrders
/* Author: GObse
** Desc: Inserts data Into FactOrders
** Change Log: When,Who,What
** 2025-01-27,Gobse,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
    Insert Into [dbo].[FactSalesOrders]
    ([SalesOrderID], [SalesOrderDetailID], [OrderDateKey], [CustomerKey], [ProductKey], [OrderQty], [ActualUnitPrice])
    Select 
    SalesOrderID, SalesOrderDetailID, OrderDateKey, CustomerKey, ProductKey, OrderQty, ActualUnitPrice
    From vETLFactOrders

    Exec pInsETLLog
	        @ETLAction = 'pETLSyncFactOrders'
	       ,@ETLLogMessage = 'FactSales Synced';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLSyncFactOrders'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go


/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncFactOrders;
 Print @Status;
*/
--Testing Code
--select * from [dbo].[FactSalesOrders]
go

--********************************************************************--
-- C)  NOT NEEDED FOR INCREMENTAL LOADING: Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--


--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int = 0;
Exec @Status = pETLSyncDimProducts;
Select [Object] = 'pETLSyncDimProducts', [Status] = @Status;

Exec @Status = pETLSyncDimCustomers;
Select [Object] = 'pETLSyncDimCustomers', [Status] = @Status;

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = @Status;

Exec @Status = pETLSyncFactOrders;
Select [Object] = 'pETLFillFactOrders', [Status] = @Status;

go
Select * from [dbo].[DimProducts];
Select * from [dbo].[DimCustomers];
Select * from [dbo].[DimDates];
Select * from [dbo].[FactSalesOrders];