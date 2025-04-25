
-- STEP A: CREATING A COPY OF THE RAW DATA
SELECT*
FROM layoffs;

CREATE TABLE layoffs_stagging
LIKE layoffs;

INSERT INTO layoffs_stagging
SELECT *
FROM layoffs;

SELECT*
FROM layoffs_stagging;





-- STEP B: DATA CLEANING
-- Removing duplicates.

-- 1: Create a new column to show duplicates.
SELECT *,
ROW_NUMBER() OVER(PARTITION  BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
AS row_numb
FROM layoffs_staging;

-- 2: Creating a new table using the filter above. This will put all the data above into a new table.
CREATE TABLE layoffs_staging2
LIKE layoffs_staging;

ALTER TABLE layoffs_staging2
ADD row_numb INT;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION  BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
AS row_numb
FROM layoffs_staging;

-- 3: After creating this new table, then I moved on to delete the duplicates.
SELECT *
FROM layoffs_staging2
WHERE row_numb > 1;

DELETE
FROM layoffs_staging2
WHERE row_numb > 1;







-- STEP C: STANDADARZING
--  1: Triming the company column.
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- 2: Ensuring the names in the industry column are unique.
SELECT industry
FROM layoffs_staging2
WHERE industry LIKE 'crypto%'
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 3: Ensuring the names in the country column are unique.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 4: I converted the date column which was originally text into date format.
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') as New_date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date;

-- 5: Remove the row_numb column added earlier.
SELECT row_numb
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_numb;

-- 6: Removing nulls from the industry column.
	-- The goal here is to identify missing industry values in some rows, using other rows with the same company to fill them in.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- To convert blanks to nulls.
UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT s1.company, s1.industry, s2.industry
FROM layoffs_staging2 s1
JOIN layoffs_staging2 s2
	ON s1.company = s2.company
WHERE s1.industry IS NOT NULL AND s2.industry IS NULL;

UPDATE layoffs_staging2 S2
JOIN layoffs_staging2 s1
	ON s1.company = s2.company
SET s2.industry = s1.industry
WHERE s1.industry IS NOT NULL AND s2.industry IS NULL;

-- To test if it works, kindly put the industries with null earlier in the company space below. These industries are: juul, airbnb, and carvana.
SELECT *
FROM layoffs_staging2
WHERE company = 'airbnb';

-- 7: Checking for nulls in the total_laid_off and percentage_laid_off.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Because these companies have null values for both total laid off and percentage laid off, it means they did not lay off anyone.
-- Because of this, I will remove all the nulls. 
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;




-- EXPLORATORY DATA ANALYSIS (EDA) 
--  Total number of layoffs in each company
SELECT company, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
GROUP BY company
ORDER BY Total_layoffs DESC;

--  Total number of layoffs in each country
SELECT country, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

--  Total number of layoffs in each country and location within each country 
SELECT country, location, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
GROUP BY country, location
ORDER BY 1 ;

--  Total number of layoffs per industry
SELECT industry, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
GROUP BY industry
ORDER BY Total_layoffs DESC;

--  Total number of layoffs per month
SELECT DATE_FORMAT(`date`, '%Y-%m') AS `Date`, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
WHERE `DATE` IS NOT NULL
GROUP BY DATE_FORMAT(`date`, '%Y-%m')
ORDER BY `Date`;

--  Companies with the highest lay offs based on ranking (first 5 picked in each year)
WITH Total_layoffs_ranking AS 
(
SELECT company, DATE_FORMAT(`date`, '%Y') AS `Date`, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
WHERE `DATE` IS NOT NULL
GROUP BY company, DATE_FORMAT(`date`, '%Y')
), 
total_off AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY `Date` ORDER BY total_layoffs DESC) ranking
FROM Total_layoffs_ranking
)
SELECT *
FROM total_off
WHERE ranking <= 5
;

--  Total number of layoffs per year
SELECT DATE_FORMAT(`date`, '%Y') AS `Date`, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
WHERE `DATE` IS NOT NULL
GROUP BY DATE_FORMAT(`date`, '%Y')
ORDER BY 1 DESC;

-- Companies and the funds they raised in millions
SELECT DISTINCT company, funds_raised_millions, YEAR(`Date`) AS `Date`
FROM layoffs_staging2
ORDER BY funds_raised_millions DESC, `Date`;

