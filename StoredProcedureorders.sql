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

-- Example: ensure orders table exists (remove if your table already exists)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'orders' AND type = 'U')
BEGIN
CREATE TABLE dbo.orders (
order_id INT IDENTITY(1,1) PRIMARY KEY,
customer_id INT,
order_date DATETIME,
status NVARCHAR(100),
order_total FLOAT,
currency NVARCHAR(10),
shipping_address NVARCHAR(400),
shipping_city NVARCHAR(200),
shipping_state NVARCHAR(100),
shipping_zip NVARCHAR(50)
);
END
GO

CREATE OR ALTER PROCEDURE dbo.sp_Clean_Orders
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @BatchSize INT = 500;
DECLARE @RowCount INT = 1;
DECLARE @ErrorMessage NVARCHAR(4000);
DECLARE @ErrorNumber INT;
DECLARE @ErrorLine INT;

BEGIN TRY
------------------------------------------------------------
-- 1. Batch clean text columns: status, currency, shipping_*.
-- - remove control chars (tabs/newlines/carriage returns)
-- - trim
-- - update only when value changes
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH to_clean AS (
SELECT TOP (@BatchSize) order_id,
status, currency, shipping_address, shipping_city, shipping_state, shipping_zip
FROM dbo.orders
WHERE status LIKE '%[^ -~]%'
OR currency LIKE '%[^ -~]%'
OR shipping_address LIKE '%[^ -~]%'
OR shipping_city LIKE '%[^ -~]%'
OR shipping_state LIKE '%[^ -~]%'
OR shipping_zip LIKE '%[^ -~]%'
ORDER BY order_id
),
computed AS (
SELECT t.order_id,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.status,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_status,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.currency,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_currency,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.shipping_address,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_shipping_address,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.shipping_city,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_shipping_city,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.shipping_state,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_shipping_state,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.shipping_zip,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_shipping_zip
FROM to_clean t
)
UPDATE o
SET status = comp.new_status,
currency = comp.new_currency,
shipping_address = comp.new_shipping_address,
shipping_city = comp.new_shipping_city,
shipping_state = comp.new_shipping_state,
shipping_zip = comp.new_shipping_zip
FROM dbo.orders o
JOIN computed comp ON o.order_id = comp.order_id
WHERE
ISNULL(o.status,'') <> ISNULL(comp.new_status,'')
OR ISNULL(o.currency,'') <> ISNULL(comp.new_currency,'')
OR ISNULL(o.shipping_address,'') <> ISNULL(comp.new_shipping_address,'')
OR ISNULL(o.shipping_city,'') <> ISNULL(comp.new_shipping_city,'')
OR ISNULL(o.shipping_state,'') <> ISNULL(comp.new_shipping_state,'')
OR ISNULL(o.shipping_zip,'') <> ISNULL(comp.new_shipping_zip,'');

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

------------------------------------------------------------
-- 2. Normalize order_total to FLOAT using TRY_CONVERT
-- - convert textual numeric values where possible
-- - set default 0.0 for NULL or unconvertible values
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH to_fix AS (
SELECT TOP (@BatchSize) order_id, order_total
FROM dbo.orders
WHERE order_total IS NULL
OR (order_total IS NOT NULL AND TRY_CONVERT(FLOAT, order_total) IS NULL)
ORDER BY order_id
),
computed AS (
SELECT t.order_id,
TRY_CONVERT(FLOAT, t.order_total) AS new_order_total
FROM to_fix t
)
UPDATE o
SET order_total = ISNULL(comp.new_order_total, 0.0)
FROM dbo.orders o
JOIN computed comp ON o.order_id = comp.order_id
WHERE
(o.order_total IS NULL AND comp.new_order_total IS NOT NULL)
OR (o.order_total IS NULL AND comp.new_order_total IS NULL)
OR (TRY_CONVERT(FLOAT, o.order_total) IS NOT NULL AND TRY_CONVERT(FLOAT, o.order_total) <> ISNULL(comp.new_order_total, 0.0));

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

------------------------------------------------------------
-- 3. Replace remaining NULLs with sensible defaults (targeted updates)
------------------------------------------------------------
UPDATE dbo.orders SET status = 'Unknown' WHERE status IS NULL;
UPDATE dbo.orders SET currency = 'USD' WHERE currency IS NULL;
UPDATE dbo.orders SET shipping_address = 'N/A' WHERE shipping_address IS NULL;
UPDATE dbo.orders SET shipping_city = 'Unknown' WHERE shipping_city IS NULL;
UPDATE dbo.orders SET shipping_state = 'Unknown' WHERE shipping_state IS NULL;
UPDATE dbo.orders SET shipping_zip = '00000' WHERE shipping_zip IS NULL;
UPDATE dbo.orders SET order_total = 0.0 WHERE order_total IS NULL;
UPDATE dbo.orders SET order_date = GETDATE() WHERE order_date IS NULL;

------------------------------------------------------------
-- 4. Remove duplicates in batches (keep earliest order_id)
-- - partition by customer_id + order_date + order_total (adjust if your uniqueness rule differs)
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH dup AS (
SELECT TOP (@BatchSize) order_id
FROM (
SELECT order_id,
ROW_NUMBER() OVER (
PARTITION BY ISNULL(CAST(customer_id AS NVARCHAR(50)),''),
ISNULL(CONVERT(VARCHAR(30), order_date, 121),''),
ISNULL(CONVERT(VARCHAR(50), order_total), '')
ORDER BY order_id
) AS rn
FROM dbo.orders
) x
WHERE x.rn > 1
ORDER BY order_id
)
DELETE o
FROM dbo.orders o
JOIN dup d ON o.order_id = d.order_id;

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

END TRY
BEGIN CATCH
SET @ErrorMessage = ERROR_MESSAGE();
SET @ErrorNumber = ERROR_NUMBER();
SET @ErrorLine = ERROR_LINE();

INSERT INTO dbo.error_log (error_message, procedure_name, batch_info, error_number, error_line)
VALUES (@ErrorMessage, 'sp_Clean_Orders', CONCAT('Batch size: ', @BatchSize), @ErrorNumber, @ErrorLine);

THROW;
END CATCH
END
GO