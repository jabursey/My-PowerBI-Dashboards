-- Which business categories are best suited to last over the course of centuries?
WITH cte AS (
	SELECT
		b.country_code,
		b.category_code,
        c.continent,
		MIN(b.year_founded) AS year_founded
	FROM countries c
	INNER JOIN businesses b
		ON c.country_code = b.country_code
	GROUP BY b.country_code, b.category_code, c.continent
)
SELECT
	cte.continent,
	ca.category,
	cte.year_founded
FROM categories ca
INNER JOIN cte cte 
	ON ca.category_code = cte.category_code
ORDER BY cte.continent, ca.category ASC;