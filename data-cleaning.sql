-- Data Cleaning

SELECT *
FROM layoffs;

/*
Steps taken for Data cleaning
1. Remove Duplicates
2. Standardize the Data
3. Null values or blank values
4. Remove Any columns and rows that are irrelevant 
*/

/*
- Create a duplicate table of world_layoffs
- duplicate table for staging work so as to keep original table unaffected by changes
*/

CREATE TABLE layoffs_staging
LIKE layoffs; 

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

/*
1. Remove Duplicates
-- create a row_num, over by all column to identify duplicate rows
-- this is because no unique column exist for this table
*/

SELECT *, ROW_NUMBER() OVER(PARTITION BY company, industry, 
		total_laid_off, percentage_laid_off, `date`, stage, 
        country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
		total_laid_off, percentage_laid_off, `date`, stage, 
        country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

/*
- remove duplicates
- since ctes cannot be updated, create a new table
- then remove duplicate rows

*/

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
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
		total_laid_off, percentage_laid_off, `date`, stage, 
        country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- duplicate rows removed
SELECT *
FROM layoffs_staging2;

/*
2. Standardizing data
- Finding issues in data and fixing it
*/

-- 2.1 remove whitespaces on the company column

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

-- 2.2 Rename industry name that are the same but writing in a slightly different way
-- identify industry refering to same industry but written in different formats

SELECT distinct(industry)
FROM layoffs_staging2;

SELECT distinct(industry)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2.3 identify country name where there is variation in spellings for same country
SELECT distinct(country)
FROM layoffs_staging2;

-- remove . at the end of United State
SELECT TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT(country)
FROM layoffs_staging2
WHERE country like 'United States%';

-- 2.4 change date column from text to date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


