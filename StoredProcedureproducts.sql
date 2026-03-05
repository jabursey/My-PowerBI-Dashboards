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

CREATE OR ALTER PROCEDURE dbo.sp_Clean_Products
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
-- 1. Batch clean text columns: sku, product_name, category
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH to_clean AS (
SELECT TOP (@BatchSize) product_id,
sku, product_name, category
FROM dbo.products
WHERE sku LIKE '%[^ -~]%'
OR product_name LIKE '%[^ -~]%'
OR category LIKE '%[^ -~]%'
ORDER BY product_id
),
computed AS (
SELECT t.product_id,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.sku,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_sku,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.product_name,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_product_name,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.category,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_category
FROM to_clean t
)
UPDATE p
SET sku = comp.new_sku,
product_name = comp.new_product_name,
category = comp.new_category
FROM dbo.products p
JOIN computed comp ON p.product_id = comp.product_id
WHERE
ISNULL(p.sku,'') <> ISNULL(comp.new_sku,'')
OR ISNULL(p.product_name,'') <> ISNULL(comp.new_product_name,'')
OR ISNULL(p.category,'') <> ISNULL(comp.new_category,'');

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

------------------------------------------------------------
-- 2. Normalize price and cost to FLOAT using TRY_CONVERT
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH to_fix AS (
SELECT TOP (@BatchSize) product_id, price, cost
FROM dbo.products
WHERE price IS NULL
OR cost IS NULL
OR (price IS NOT NULL AND TRY_CONVERT(FLOAT, price) IS NULL)
OR (cost IS NOT NULL AND TRY_CONVERT(FLOAT, cost) IS NULL)
ORDER BY product_id
),
computed AS (
SELECT t.product_id,
TRY_CONVERT(FLOAT, t.price) AS new_price,
TRY_CONVERT(FLOAT, t.cost) AS new_cost
FROM to_fix t
)
UPDATE p
SET price = ISNULL(comp.new_price, 0.0),
cost = ISNULL(comp.new_cost, 0.0)
FROM dbo.products p
JOIN computed comp ON p.product_id = comp.product_id
WHERE
(p.price IS NULL AND comp.new_price IS NOT NULL)
OR (p.cost IS NULL AND comp.new_cost IS NOT NULL)
OR (p.price IS NULL AND comp.new_price IS NULL)
OR (p.cost IS NULL AND comp.new_cost IS NULL)
OR (TRY_CONVERT(FLOAT, p.price) IS NOT NULL AND TRY_CONVERT(FLOAT, p.price) <> ISNULL(comp.new_price, 0.0))
OR (TRY_CONVERT(FLOAT, p.cost) IS NOT NULL AND TRY_CONVERT(FLOAT, p.cost) <> ISNULL(comp.new_cost, 0.0));

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

------------------------------------------------------------
-- 3. Replace remaining NULLs with defaults (targeted updates)
------------------------------------------------------------
UPDATE dbo.products SET sku = 'UNKNOWN' WHERE sku IS NULL;
UPDATE dbo.products SET product_name = 'Unnamed Product' WHERE product_name IS NULL;
UPDATE dbo.products SET category = 'Uncategorized' WHERE category IS NULL;
UPDATE dbo.products SET price = 0.0 WHERE price IS NULL;
UPDATE dbo.products SET cost = 0.0 WHERE cost IS NULL;
UPDATE dbo.products SET is_active = 1 WHERE is_active IS NULL;
UPDATE dbo.products SET created_at = GETDATE() WHERE created_at IS NULL;

------------------------------------------------------------
-- 4. Remove duplicates in batches (keep earliest created_at)
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH dup AS (
SELECT TOP (@BatchSize) product_id
FROM (
SELECT product_id,
ROW_NUMBER() OVER (
PARTITION BY ISNULL(sku,'')
ORDER BY ISNULL(created_at, '19000101')
) AS rn
FROM dbo.products
) x
WHERE x.rn > 1
ORDER BY product_id
)
DELETE p
FROM dbo.products p
JOIN dup d ON p.product_id = d.product_id;

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

END TRY
BEGIN CATCH
SET @ErrorMessage = ERROR_MESSAGE();
SET @ErrorNumber = ERROR_NUMBER();
SET @ErrorLine = ERROR_LINE();

INSERT INTO dbo.error_log (error_message, procedure_name, batch_info, error_number, error_line)
VALUES (@ErrorMessage, 'sp_Clean_Products', CONCAT('Batch size: ', @BatchSize), @ErrorNumber, @ErrorLine);

THROW;
END CATCH
END
GO