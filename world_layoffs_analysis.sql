--
--
-- DATA CLEANING
--
--

USE world_layoffs;
SELECT * FROM layoffs;

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- REMOVE DUPLICATES

# CHECK FOR DUPLICATES

SELECT *
FROM world_layoffs.layoffs_staging
;


SELECT *
FROM (SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
	ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,
						percentage_laid_off,`date`, stage, country, funds_raised_millions
						) AS row_num
	FROM world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

SELECT *
FROM layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
;

-- DELETE ROWS WERE ROW_NUM IS GREATER THAN 2

SELECT * FROM layoffs_staging2
WHERE row_num >= 2;


DELETE FROM layoffs_staging2
WHERE row_num >= 2;


-- STANDARDIZE DATA

SELECT * 
FROM world_layoffs.layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- THE CRYPTO IN INDUSTRY HAS MULTIPLE DIFFERENT VARIATIONS. WE NEED TO STANDARDIZE THAT - LET'S SAY ALL TO CRYPTO
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- NOW THAT'S TAKEN CARE OF:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- UNITED STATES HAS '.' IN ONE ROW
SELECT DISTINCT country
from layoffs_staging2
order by 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- FIX THE DATE COLUMNS
SELECT *
FROM world_layoffs.layoffs_staging2;

-- USE STR_TO_DATE TO UPDATE THIS FIELD -> STILL TEXT
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- NOW WE CAN CONVERT THE DATA TYPE PROPERLY TO DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2;



-- LOOK AT NULL VALUES

-- MAY NEED TO REMOVE THESE THAT BOTH ARE NULL MAYBE USELESS BUT WE'LL SEE LATER
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- INDUSTRY HAS SOME NULL AND EMPTY ROWS
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = ''
ORDER BY industry;

-- SET THE BLANKS TO NULLS
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- CHECK
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL OR industry = ''
ORDER BY industry;

-- CHECK AIRBNB COMPANY IF EXISTS ANYWHERE ELSE SO MAYBE CHANGE TO TRAVEL
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb';

-- SELF JOIN SAME COMPANY AND LOCATION WHERE ONE HAS NULL INDUSTRY AND ONE NOT NULL
SELECT t1.company, t1.location, t1.industry,t2.company, t2.location, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry is NULL AND t2.industry IS NOT NULL;

-- POPULATE THOSE NULLS IF POSSIBLE
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- BALLY'S WAS THE ONLY ONE WITHOUT A POPULATED ROW TO POPULATE THIS NULL VALUES
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- --------------------------------------
-- --------------------------------------


-- REMOVE COLUMNS AND ROWS 

SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- DELETE USELESS DATA WE CAN'T REALLY USE
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- -----------
SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

--
--
--
--EXPLORATORY DATA ANALYSIS
--
--
--

SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- WHICH COMPANIES HAD 100% OF THEY COMPANY LAID OFF
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY total_laid_off DESC;

-- ORDER BY FUNCS_RAISED_MILLIONS TO SEE HOW BIG THESE COMPANIES WERE
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY funds_raised_millions DESC;

-- COMPANIES WITH THE MOST TOTAL LAYOFFS
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- INDUSTRIES WITH THE MOST TOTAL LAYOFFS
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- COUNTRIES WITH THE MOST TOTAL LAYOFFS
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- BY LOCATION
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- BY STAGE
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 DESC;

-- ROLLING TOTAL OF LAYOFFS PER MONTH
-- this gives us by month but takes january from all years
SELECT SUBSTRING(date,6,2) AS `Month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY `Month`
ORDER BY `Month` ASC;

-- take year and month together
SELECT SUBSTRING(date,1,7) AS `Month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY `Month` ASC;

-- now use it in a CTE so we can query off of it
WITH Rolling_Total AS 
(
SELECT SUBSTRING(date,1,7) AS `Month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY `Month`
)
SELECT `Month`, total_off, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;
 
  
-- COMPANIES WITH THE MOST LAYOFFS PER YEAR

WITH Company_Year AS 
(
-- total laid off by company, by year
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, 
		DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


