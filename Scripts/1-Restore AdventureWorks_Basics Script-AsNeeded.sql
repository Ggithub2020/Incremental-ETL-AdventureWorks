/********************************************************************
-- Title: Restore the AdventureWorks_Basics database
Instructions:
    1) Create a folder on your on you C:\ drive called _BISolutions
       NOTE: We will use this folder for the rest of the class so please 
       do not use a different drive or folder name, or it may cause you trouble.
     2)Copy and Paste the AdventureWorks_Basics.bak into the C:\_BISolutions folder
     3) Run the following scrip to restore the backup.
        NOTE: You may need to open SQL Management Studio as an Administrator to do this!
        https://www.youtube.com/watch?v=nNVdaJXYCbA  - Windows 7 to 8.1
        https://youtu.be/KbynSwsd8wo  - Windows 10
     4) Right-Click on the Database icon in Object Explorer and choose Refersh for the 
    database to show in the tree-view.
********************************************************************/

USE [master]
If Exists(Select Name from Sys.Databases Where Name = 'AdventureWorks_Basics')
	Begin
		Use Master
		ALTER DATABASE [AdventureWorks_Basics] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
		DROP DATABASE [AdventureWorks_Basics]
	End;
Go

BEGIN TRY 
	RESTORE DATABASE [AdventureWorks_Basics] 
	FROM  DISK = N'C:\_BISolutions\AdventureWorks_Basics.bak' 
	WITH 
		MOVE N'AdventureWorks_Basics' 
		  TO N'C:\_BISolutions\AdventureWorks_Basics.mdf',  
		MOVE N'AdventureWorks_Basics_log' 
		  TO N'C:\_BISolutions\AdventureWorks_Basics_log.ldf',  
		REPLACE;
	SELECT 'The [AdventureWorks_Basics] database was restored';
END TRY
BEGIN CATCH
	Raiserror('There was an error! 
			   1) Please check that you have created the folder C:\_BISolutions
			   2) Please check that the .bak file is in that folder
			   3) Please check that you are running SQL Server Managment Studio as an Administrator 
				  (it will say "(Administrator)" about the menu. See the links at the top of this script!
			   ', 15, 1);
END CATCH
Go
