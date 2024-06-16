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

