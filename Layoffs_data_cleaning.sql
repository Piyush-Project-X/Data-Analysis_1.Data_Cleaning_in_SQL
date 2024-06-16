-- Select all records from the 'layoffs' table
SELECT * FROM layoffs;

-- Process of Data cleaning
-- 1. Remove Duplicates
-- 2. Standardize the data (Like spelling mistakes)
-- 3. Null values or blank values
-- 4. Remove unnecessary columns and rows

-- Create a new table 'layoffs_staging' with the same structure as 'layoffs'
create table layoffs_staging like layoffs;

-- Insert all data from 'layoffs' into 'layoffs_staging'
insert layoffs_staging
SELECT * FROM layoffs;

-- Identify duplicate entries using row_number() function
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country) as row_num
from layoffs_staging;

-- Common Table Expression (CTE) to find duplicates based on specified columns
with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country) as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num>1;

-- Select records from 'layoffs_staging' where company is 'Casper'
select * from layoffs_staging
where company = 'Casper';

-- Create a new table 'layoffs_staging2' with modified columns and added 'row_num' column
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Select records from 'layoffs_staging2' where 'row_num' is greater than 1
select * from layoffs_staging2
where row_num > 1;

-- Insert data into 'layoffs_staging2' with row_number() partitioned by specified columns
insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country) as row_num
from layoffs_staging;

-- Delete duplicates from 'layoffs_staging2' where 'row_num' is greater than 1
delete from layoffs_staging2
where row_num > 1;

-- Standardize data by trimming leading and trailing spaces from 'company' column
select  company, TRIM(company) from layoffs_staging2;

-- Update 'company' column in 'layoffs_staging2' to trim leading and trailing spaces
update layoffs_staging2 
set company = TRIM(company);

-- Update 'industry' column to standardize as 'Crypto' for values starting with 'Crypto_'
update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto_%';

-- Display distinct values from 'industry' column in 'layoffs_staging2' ordered alphabetically
Select distinct industry from layoffs_staging2
order by 1;

-- Display distinct values from 'country' column in 'layoffs_staging2'
select distinct country from layoffs_staging2;

-- Update 'country' column to remove trailing '.' characters from values starting with 'United States'
update layoffs_staging2
set country = TRIM(TRAILING '.' from country)
where country like 'United States%';

-- Convert 'date' column to DATE format using str_to_date() function
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

-- Update 'date' column in 'layoffs_staging2' to DATE format using str_to_date() function
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- Modify 'date' column in 'layoffs_staging2' to DATE datatype
alter table layoffs_staging2
modify column `date` DATE;

-- Select records from 'layoffs_staging2' where 'total_laid_off' and 'percentage_laid_off' are NULL
select * from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off is NULL;

-- Delete records from 'layoffs_staging2' where 'total_laid_off' and 'percentage_laid_off' are NULL
Delete from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off is NULL;

-- Drop 'row_num' column from 'layoffs_staging2'
alter table layoffs_staging2
drop column row_num;

-- Select all records from 'layoffs_staging2'
select * from layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Companies with the biggest single Layoff

SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- by location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- by location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
where dates is not null
ORDER BY dates ASC;


-- Count the total number of records in 'layoffs_staging2 after cleaning data to check if we removed duplicates and unnecessary rows'
Select count(*) from layoffs_staging2;
