/****************** [ETL TEST Script] *********************
Title: Testing your ETL process
Desc: This file will test Incremental ETL changes in [DWAdventureWorks_Basics]
Instructions:
  1) Create an ETL script for the assignment.
  2) Run the ETL script to fill the DW with initial data.
  3) Run this script to add new data.
  4) Run your ETL script to test that the new data is processed into the DW.
Change Log: (When,Who,What)
2022-12-29,RRoot,Created File
****************** Instructors Version ***************************/

USE [AdventureWorks_Basics]
GO

INSERT INTO [dbo].[Customer]
           ([CustomerID]
           ,[FirstName]
           ,[LastName]
           ,[AddressID]
           ,[AddressLine1]
           ,[City]
           ,[StateProvinceName]
           ,[CountryRegionCode]
           ,[CountryRegionName]
           ,[PostalCode]
           ,[BusinessEntityID])
     VALUES
           (000001
           ,'Test Ins Customer'
           ,'ETL'
           ,000001
           ,'123 Main'
           ,'Seattle'
           ,'Washington'  
           ,'WA'
           ,'USA'
           ,'98001'
           ,000001);
Go
INSERT INTO [dbo].[Products]
           ([Name]
           ,[ListPrice]
           ,[ProductSubcategoryID]
           ,[SafetyStockLevel]
           ,[ProductLine])
     VALUES
           ('Test Ins Product'
           ,539.99
           ,2
           ,100
           ,'R'
           )
Go

INSERT INTO [dbo].[SalesOrderHeader]
           ([SalesOrderID]
           ,[OrderDate]
           ,[CustomerID]
           ,[CreditCardID]
           ,[CreditCardApprovalCode]
           ,[TotalDue]
           ,[Comment])
     VALUES
           (11111
           ,GetDate()
           ,000001
           ,0101
           ,'1aaaaaaaaa86'
           ,3953.99
           ,Null
           )
GO

Declare @ProductID int;
Select @ProductID = ProductID From Products Where Name = 'Test Ins Product';
INSERT INTO [dbo].[SalesOrderDetail]
           ([SalesOrderID]
           ,[SalesOrderDetailID]
           ,[ProductID]
           ,[OrderQty]
           ,[UnitPrice]
           ,[UnitPriceDiscount])
     VALUES
           (11111
           ,123
           ,@ProductID
           ,1
           ,3953.9884
           ,0.00)
Go
Select * From Customer Where FirstName = 'Test Ins Customer';
Select * From Products Where Name = 'Test Ins Product';
Select * From SalesOrderHeader Where SalesOrderID = 11111;
Select * From SalesOrderDetail Where SalesOrderID = 11111;
Go

-- Test Updating Data --
/*
Update Customer Set FirstName = 'Test Upd Customer' Where FirstName = 'Test Ins Customer';
Update Products Set Name = 'Test Upd Product' Where Name = 'Test Ins Product';
Update SalesOrderHeader Set OrderDate = '20100101' Where SalesOrderID = 11111;
Update SalesOrderDetail Set UnitPrice = 1000 Where SalesOrderID = 11111;

Select * From Customer Where FirstName = 'Test Upd Customer';
Select * From Products Where Name = 'Test Upd Product';
Select * From SalesOrderHeader Where SalesOrderID = 11111;
Select * From SalesOrderDetail Where SalesOrderID = 11111;
*/


-- Test Deleting Data --
/*
Delete From SalesOrderDetail Where SalesOrderID = 11111;
Delete From SalesOrderHeader Where SalesOrderID = 11111;
Delete From Customer Where FirstName = 'Test Upd Customer';
Delete From Products Where Name = 'Test Upd Product';

Select * From Customer Where FirstName = 'Test Upd Customer';
Select * From Products Where Name = 'Test Upd Product';
Select * From SalesOrderHeader Where SalesOrderID = 11111;
Select * From SalesOrderDetail Where SalesOrderID = 11111;
*/