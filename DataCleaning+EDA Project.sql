-- Layoffs DataBase Project

-- -------------------
-- Data Cleaning
-- -------------------

SELECT * FROM layoffs_table ;

CREATE TABLE layoffs_staging LIKE layoffs_table ; -- Making the staging table

INSERT layoffs_staging         -- Adding the data to the staging table
SELECT * FROM layoffs_table ;

SELECT * FROM layoffs_staging ; -- Previw the data

DESCRIBE layoffs_staging ; -- Check column names and data types

SELECT * FROM layoffs_staging ;

-- ----------------------------------------------------------------------------------

SELECT *, COUNT(*) AS duplicate_count    -- Check fully duplicated rows
FROM layoffs_staging
GROUP BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
HAVING COUNT(*) > 1 ;

SELECT *     -- Another way to check the full duplicated rows
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM layoffs_staging
) duplicates
WHERE 
	row_num > 1 ;
    

WITH duplicates AS (             -- Deleting the fully duplicated rows
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions 
           ) AS row_num
    FROM layoffs_staging
)
DELETE FROM layoffs_staging 
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions 
    FROM duplicates WHERE row_num > 1
);

SELECT *, COUNT(*) AS row_num    -- Show the table with row_num column (we dont have row_num greater than 1) 
FROM layoffs_staging
GROUP BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
HAVING row_num > 1 ;

-- -------------------------------------------------------------------------------------

SELECT DISTINCT company 
FROM layoffs_staging 
ORDER BY company ;  -- Check for inconsistencies

UPDATE layoffs_staging SET company = TRIM(LOWER(company)) ;        -- Removes extra spaces and makes lowercase

UPDATE layoffs_staging               -- Correct the format of (United States) instead of (United States.)           
SET country = TRIM(TRAILING '.' FROM country) ;  -- Removing the . from the end of the word

SELECT DISTINCT industry     -- Check industy
FROM layoffs_staging
ORDER BY industry;

SELECT DISTINCT industry
FROM layoffs_staging
WHERE industry IS NULL or industry = ''
ORDER BY industry;

UPDATE layoffs_staging        -- Replace blank values with NULLs
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging            -- Standardize the data in table (industy crpto,..)
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency') ;

SELECT DISTINCT industry     -- Check industy
FROM layoffs_staging
ORDER BY industry;

UPDATE layoffs_staging t1   -- Fill some NULLs
JOIN layoffs_staging t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL ;

UPDATE layoffs_staging           -- Replace NULL in industry with Unknown' industy
SET industry = 'Unknown' WHERE industry IS NULL ;

-- -----------------------------------------------------------------

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') AS new_date     -- Correcting the date format 
FROM layoffs_staging ;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ;

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE ;

SELECT percentage_laid_off   -- Correcting the percentage format 
FROM layoffs_staging ;

ALTER TABLE layoffs_staging
MODIFY COLUMN percentage_laid_off FLOAT ;

-- -------------------------------------------------------

-- It is normal to have NULLs in total_laid_off, percentage_laid_off and funds_raised_millions

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL ;

-- But it will not be helpful if we have total_laid_off and percentage_laid_off bot NULL

DELETE FROM layoffs_staging    -- Deleting records that we will not use
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL ; 

SELECT * 
FROM layoffs_staging ;

SELECT * 
FROM layoffs_staging
ORDER BY `date` ;

-- -------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------

-- -----------------------------
-- Exploratory Data Analysis
-- -----------------------------

-- 1-Total Layoffs by Industry
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY industry
ORDER BY total_layoffs DESC;

-- 2-Top 5 Companies with the Most Layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY company
ORDER BY total_layoffs DESC
LIMIT 5;

-- 3-Total Layoffs by Country
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY country
ORDER BY total_layoffs DESC;

-- 4-Total Layoff Over Time
SELECT DATE_FORMAT(`date`, '%Y-%m') AS `month`, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL AND `date` IS NOT NULL
GROUP BY month
ORDER BY month;

-- 5-Companies with the Highest Percentage of Layoffs
SELECT company, percentage_laid_off
FROM layoffs_staging
WHERE percentage_laid_off IS NOT NULL
ORDER BY percentage_laid_off DESC
LIMIT 10;

-- 6-Companies with the Lowest Percentage of Layoffs
SELECT DISTINCT company, percentage_laid_off
FROM layoffs_staging
WHERE percentage_laid_off IS NOT NULL
ORDER BY percentage_laid_off ASC
LIMIT 10;

-- 7-Total Layoffs by Funding Stage
SELECT stage, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL AND stage IS NOT NULL
GROUP BY stage
ORDER BY total_layoffs DESC;

-- 8-Correlation between Funds Raised in Millions and Layoffs
SELECT funds_raised_millions, AVG(percentage_laid_off) AS avg_percentage_laid_off
FROM layoffs_staging
WHERE percentage_laid_off IS NOT NULL AND funds_raised_millions IS NOT NULL
GROUP BY funds_raised_millions
ORDER BY funds_raised_millions DESC;

-- 9-Locations Facing the Most Layoffs
SELECT location, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY location
ORDER BY total_laid_off DESC;

-- 10-Layoffs Happening per Quarter or Year
SELECT YEAR(`date`) AS `year`,
       QUARTER(`date`) AS `quarter`,
       SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE `date` IS NOT NULL
GROUP BY `year`, `quarter`
ORDER BY `year`, `quarter`;


-- 11-Months or Seasons with Higher Layoffs
SELECT MONTH(`date`) AS `month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE `date` IS NOT NULL AND total_laid_off IS NOT NULL
GROUP BY `month`
ORDER BY total_laid_off DESC;

-- 12-Funds Raised in Companys Lays off More
SELECT funds_raised_millions, AVG(percentage_laid_off) AS avg_percentage_laid_off
FROM layoffs_staging
WHERE percentage_laid_off > 0.1  -- threshold (10%)
GROUP BY funds_raised_millions
ORDER BY funds_raised_millions DESC;

-- 13-Layoffs Over Time in Specific Idustry
SELECT industry, YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE `date` IS NOT NULL AND total_laid_off IS NOT NULL
GROUP BY industry, `year`
ORDER BY `year`, total_laid_off DESC;

-- 14-Average Percentage of Layoffs for Companies by Country
SELECT country, AVG(percentage_laid_off) AS avg_percentage_laid_off
FROM layoffs_staging
WHERE percentage_laid_off IS NOT NULL
GROUP BY country
ORDER BY avg_percentage_laid_off DESC;


