-- How many countries per continent lack data on the oldest businesses
-- Does including the `new_businesses` data change anything?;
SELECT 
	continent, 
	COUNT (country) AS countries_without_businesses
FROM countries c
LEFT JOIN (
    SELECT * FROM businesses
    UNION ALL
    SELECT * FROM new_businesses
) b
ON c.country_code = b.country_code
WHERE business IS NULL
GROUP BY continent;