-- What is the oldest business on each continent?
WITH continents_with_oldest_businesses AS (
    SELECT 
        continent,
        MIN(year_founded) AS year_founded
    FROM businesses b
    INNER JOIN countries c
        ON b.country_code = c.country_code
    GROUP BY continent
)
SELECT 
    c.continent,
    c.country,
    b.business,
    cwob.year_founded
FROM businesses b
INNER JOIN countries c 
	ON b.country_code = c.country_code
INNER JOIN continents_with_oldest_businesses cwob
    	ON c.continent = cwob.continent
    	AND b.year_founded = cwob.year_founded
ORDER BY c.continent, c.country, b.business;