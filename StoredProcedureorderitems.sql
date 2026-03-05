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

-- Optional: create ordersitems table if not present (remove if table exists)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'orderitems' AND type = 'U')
BEGIN
CREATE TABLE dbo.orderitems (
order_item_id INT IDENTITY(1,1) PRIMARY KEY,
order_id INT,
product_id INT,
sku NVARCHAR(100),
quantity INT,
unit_price FLOAT,
line_total FLOAT
);
END
GO

CREATE OR ALTER PROCEDURE dbo.sp_Clean_OrdersItems
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
-- 1. Batch clean text column: sku
-- - remove control characters, trim, update only when changed
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH to_clean AS (
SELECT TOP (@BatchSize) order_item_id, sku
FROM dbo.orderitems
WHERE sku LIKE '%[^ -~]%'
OR sku LIKE '%'+CHAR(9)+'%'
OR sku LIKE '%'+CHAR(10)+'%'
OR sku LIKE '%'+CHAR(13)+'%'
ORDER BY order_item_id
),
computed AS (
SELECT t.order_item_id,
LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(ISNULL(t.sku,''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))) AS new_sku
FROM to_clean t
)
UPDATE oi
SET sku = comp.new_sku
FROM dbo.orderitems oi
JOIN computed comp ON oi.order_item_id = comp.order_item_id
WHERE ISNULL(oi.sku,'') <> ISNULL(comp.new_sku,'');

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

------------------------------------------------------------
-- 2. Normalize numeric columns: quantity -> INT, unit_price -> FLOAT, line_total -> FLOAT
-- - use TRY_CONVERT to avoid errors
-- - update only rows that need conversion or defaults
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH to_fix AS (
SELECT TOP (@BatchSize) order_item_id, quantity, unit_price, line_total
FROM dbo.orderitems
WHERE quantity IS NULL
OR unit_price IS NULL
OR line_total IS NULL
OR (quantity IS NOT NULL AND TRY_CONVERT(INT, quantity) IS NULL)
OR (unit_price IS NOT NULL AND TRY_CONVERT(FLOAT, unit_price) IS NULL)
OR (line_total IS NOT NULL AND TRY_CONVERT(FLOAT, line_total) IS NULL)
ORDER BY order_item_id
),
computed AS (
SELECT t.order_item_id,
TRY_CONVERT(INT, t.quantity) AS new_quantity,
TRY_CONVERT(FLOAT, t.unit_price) AS new_unit_price,
TRY_CONVERT(FLOAT, t.line_total) AS new_line_total
FROM to_fix t
)
UPDATE oi
SET quantity = ISNULL(comp.new_quantity, 0),
unit_price = ISNULL(comp.new_unit_price, 0.0),
line_total = ISNULL(comp.new_line_total, 0.0)
FROM dbo.orderitems oi
JOIN computed comp ON oi.order_item_id = comp.order_item_id
WHERE
(oi.quantity IS NULL AND comp.new_quantity IS NOT NULL)
OR (oi.quantity IS NULL AND comp.new_quantity IS NULL)
OR (oi.unit_price IS NULL AND comp.new_unit_price IS NOT NULL)
OR (oi.unit_price IS NULL AND comp.new_unit_price IS NULL)
OR (oi.line_total IS NULL AND comp.new_line_total IS NOT NULL)
OR (oi.line_total IS NULL AND comp.new_line_total IS NULL)
OR (TRY_CONVERT(INT, oi.quantity) IS NOT NULL AND TRY_CONVERT(INT, oi.quantity) <> ISNULL(comp.new_quantity, 0))
OR (TRY_CONVERT(FLOAT, oi.unit_price) IS NOT NULL AND TRY_CONVERT(FLOAT, oi.unit_price) <> ISNULL(comp.new_unit_price, 0.0))
OR (TRY_CONVERT(FLOAT, oi.line_total) IS NOT NULL AND TRY_CONVERT(FLOAT, oi.line_total) <> ISNULL(comp.new_line_total, 0.0));

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

------------------------------------------------------------
-- 3. Targeted NULL/default replacements
------------------------------------------------------------
UPDATE dbo.orderitems SET sku = 'UNKNOWN' WHERE sku IS NULL;
UPDATE dbo.orderitems SET quantity = 0 WHERE quantity IS NULL;
UPDATE dbo.orderitems SET unit_price = 0.0 WHERE unit_price IS NULL;
UPDATE dbo.orderitems SET line_total = 0.0 WHERE line_total IS NULL;

------------------------------------------------------------
-- 4. Recalculate line_total where inconsistent: quantity * unit_price
-- - update only when computed value differs to avoid unnecessary writes
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH recalc AS (
SELECT TOP (@BatchSize) order_item_id, quantity, unit_price,
(TRY_CONVERT(FLOAT, ISNULL(quantity,0)) * TRY_CONVERT(FLOAT, ISNULL(unit_price,0.0))) AS calc_line_total
FROM dbo.orderitems
ORDER BY order_item_id
)
UPDATE oi
SET line_total = r.calc_line_total
FROM dbo.orderitems oi
JOIN recalc r ON oi.order_item_id = r.order_item_id
WHERE ISNULL(TRY_CONVERT(FLOAT, oi.line_total), 0.0) <> ISNULL(r.calc_line_total, 0.0);

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

------------------------------------------------------------
-- 5. Remove duplicates in batches (keep lowest order_item_id)
-- - partition by order_id + product_id + ISNULL(sku,'')
------------------------------------------------------------
SET @RowCount = 1;
WHILE @RowCount > 0
BEGIN
BEGIN TRAN;

;WITH dup AS (
SELECT TOP (@BatchSize) order_item_id
FROM (
SELECT order_item_id,
ROW_NUMBER() OVER (
PARTITION BY ISNULL(CAST(order_id AS NVARCHAR(50)),''),
ISNULL(CAST(product_id AS NVARCHAR(50)),''),
ISNULL(sku,'')
ORDER BY order_item_id
) AS rn
FROM dbo.orderitems
) x
WHERE x.rn > 1
ORDER BY order_item_id
)
DELETE oi
FROM dbo.orderitems oi
JOIN dup d ON oi.order_item_id = d.order_item_id;

SET @RowCount = @@ROWCOUNT;
COMMIT TRAN;
END

END TRY
BEGIN CATCH
SET @ErrorMessage = ERROR_MESSAGE();
SET @ErrorNumber = ERROR_NUMBER();
SET @ErrorLine = ERROR_LINE();

INSERT INTO dbo.error_log (error_message, procedure_name, batch_info, error_number, error_line)
VALUES (@ErrorMessage, 'sp_Clean_OrdersItems', CONCAT('Batch size: ', @BatchSize), @ErrorNumber, @ErrorLine);

THROW;
END CATCH
END
GO