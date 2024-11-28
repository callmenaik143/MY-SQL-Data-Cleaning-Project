select * from layoffs;


-- 1. Remove duplicate
-- 2. standardize the data
-- 3. Null value and blank values
-- 4. Remove unnecessary columns


CREATE TABLE layoffs_staging
LIKE Layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;

SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, industry , total_laid_off, percentage_laid_off , 'data') as row_num
FROM layoffs_staging;

with duplicate_cte as (
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location , total_laid_off, percentage_laid_off , 'data', stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
select * from
duplicate_cte
where row_num > 1;



SELECT * FROM layoffs_staging
WHERE company  = 'Casper';


SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, industry , total_laid_off, percentage_laid_off , 'data') as row_num
FROM layoffs_staging;

with duplicate_cte as (
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location , total_laid_off, percentage_laid_off , 'data', stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
DELETE FROM
duplicate_cte
where row_num > 1;

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location , total_laid_off, percentage_laid_off , 'data', stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- 2 standardizing Data
-- we removee the whiespace at the begaining of column with trim function

SELECT  company ,TRIM(company)
from layoffs_staging2;

-- we just update the company and reomve the whitespcae
UPDATE layoffs_staging2
SET company =  TRIM(company);

SELECT  company ,TRIM(company)
from layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT * FROM layoffs_staging2
WHERE
industry like 'crypto%';

update layoffs_staging2
SET industry = 'crypto'
where industry like 'crypto%';


-- in this we remove the period from in the last of country name 
SELECT distinct country , TRIM(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set country = TRIM(TRAILING '.' FROM country)
where country like 'United States';

SELECT * FROM layoffs_staging2;

select `date` from layoffs_staging2;

-- we can format the date through str_to_date format
SELECT `date`,
 str_to_date(`date`, '%m/%d/%Y')
 from layoffs_staging2;
 
 UPDATE layoffs_staging2
 SET `date` =  str_to_date(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- HERE WE CAN UPDATED THE BLANK COLUMN TO NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- CHECK WEATHER INDUSRTY COLUMN IS NULL OR BLANK
SELECT * FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
on t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%';


SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;










