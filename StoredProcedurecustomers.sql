IF NOT EXISTS (
SELECT 1 FROM sys.tables WHERE name = 'error_log' AND type = 'U'
)
BEGIN
CREATE TABLE dbo.error_log (
log_id INT IDENTITY(1,1) PRIMARY KEY,
error_message NVARCHAR(4000),
error_time DATETIME DEFAULT GETDATE(),
procedure_name SYSNAME,
batch_info NVARCHAR(200),
error_number INT,
error_line INT
);
END
GO

CREATE OR ALTER PROCEDURE dbo.sp_Clean_Customers
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;

-- Assumption: primary key is customer_id INT. Replace if different.
DECLARE @BatchSize INT = 500;
DECLARE @RowCount INT = 1;
DECLARE @ErrorMessage NVARCHAR(4000);
DECLARE @ErrorNumber INT;
DECLARE @ErrorLine INT;

BEGIN TRY
------------------------------------------------------------
-- 1. Batch clean text columns: only update rows that will change
------------------------------------------------------------
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH to_clean AS (
SELECT TOP (@BatchSize) customer_id,
first_name, last_name, email, phone, city, state
FROM dbo.customers
WHERE first_name LIKE '%[^a-zA-Z ]%'
OR last_name LIKE '%[^a-zA-Z ]%'
OR email LIKE '%[^a-zA-Z0-9@._-]%'
OR phone LIKE '%[^0-9+()- ]%'
OR city LIKE '%[^a-zA-Z ]%'
OR state LIKE '%[^a-zA-Z ]%'
ORDER BY customer_id
)
-- compute cleaned values once per row
, computed AS (
SELECT t.customer_id,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(t.first_name, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_first_name,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(t.last_name, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_last_name,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(t.email, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_email,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(t.phone, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_phone,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(t.city, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_city,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(t.state, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_state
FROM to_clean t
)
UPDATE c
SET first_name = comp.new_first_name,
last_name = comp.new_last_name,
email = comp.new_email,
phone = comp.new_phone,
city = comp.new_city,
state = comp.new_state
FROM dbo.customers c
JOIN computed comp ON c.customer_id = comp.customer_id
WHERE
ISNULL(c.first_name,'') <> ISNULL(comp.new_first_name,'')
OR ISNULL(c.last_name,'') <> ISNULL(comp.new_last_name,'')
OR ISNULL(c.email,'') <> ISNULL(comp.new_email,'')
OR ISNULL(c.phone,'') <> ISNULL(comp.new_phone,'')
OR ISNULL(c.city,'') <> ISNULL(comp.new_city,'')
OR ISNULL(c.state,'') <> ISNULL(comp.new_state,'');

SET @RowCount = @@ROWCOUNT;

COMMIT TRAN;
END

------------------------------------------------------------
-- 2. Replace NULLs with defaults using targeted updates
------------------------------------------------------------
-- Do one small update per column to minimize logging and locking
UPDATE dbo.customers
SET first_name = 'Unknown'
WHERE first_name IS NULL;

UPDATE dbo.customers
SET last_name = 'Unknown'
WHERE last_name IS NULL;

UPDATE dbo.customers
SET email = 'noemail@example.com'
WHERE email IS NULL;

UPDATE dbo.customers
SET phone = 'N/A'
WHERE phone IS NULL;

UPDATE dbo.customers
SET address = 'N/A'
WHERE address IS NULL;

UPDATE dbo.customers
SET city = 'Unknown'
WHERE city IS NULL;

UPDATE dbo.customers
SET state = 'Unknown'
WHERE state IS NULL;

UPDATE dbo.customers
SET zip = '00000'
WHERE zip IS NULL;

UPDATE dbo.customers
SET created_at = GETDATE()
WHERE created_at IS NULL;

------------------------------------------------------------
-- 3. Remove duplicates in batches keeping earliest created_at
------------------------------------------------------------
-- This deletes duplicates in small batches to avoid long transactions
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH dup AS (
SELECT TOP (@BatchSize) customer_id
FROM (
SELECT customer_id,
ROW_NUMBER() OVER (
PARTITION BY ISNULL(first_name,'') , ISNULL(last_name,''), ISNULL(email,'')
ORDER BY ISNULL(created_at, '19000101')
) AS rn
FROM dbo.customers
) x
WHERE x.rn > 1
ORDER BY customer_id
)
DELETE c
FROM dbo.customers c
JOIN dup d ON c.customer_id = d.customer_id;

SET @RowCount = @@ROWCOUNT;

COMMIT TRAN;
END

END TRY
BEGIN CATCH
SET @ErrorMessage = ERROR_MESSAGE();
SET @ErrorNumber = ERROR_NUMBER();
SET @ErrorLine = ERROR_LINE();

INSERT INTO dbo.error_log (error_message, procedure_name, batch_info, error_number, error_line)
VALUES (@ErrorMessage, 'sp_Clean_Customers', CONCAT('Batch size: ', @BatchSize), @ErrorNumber, @ErrorLine);

THROW; -- rethrow so caller is aware
END CATCH
END
GO