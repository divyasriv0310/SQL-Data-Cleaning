-- Data Cleaning
select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns
create table layoffs_staging like layoffs;
Select * from layoffs_staging;

Insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;

with duplicate_cte as (
select *, row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging) 
select * from duplicate_cte where row_num > 1;

select * from layoffs_staging where company = 'Casper';

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

select * from layoffs_staging2 where row_int>1;

insert into layoffs_staging2
select *, row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs_staging;

delete from layoffs_staging2 where row_int > 1;

SET SQL_SAFE_UPDATES = 0;

-- Standardizing data
select company, TRIM(company) from layoffs_staging2;

update layoffs_staging2 
set company = TRIM(company);

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country, TRIM(TRAILING '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = TRIM(TRAILING '.' from country)
where industry like 'United States%';

select `date` from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y')
WHERE `date` LIKE '%/%/%';

alter table layoffs_staging2
modify column `date` DATE;

update layoffs_staging2
set industry = NULL
where industry = '';

select * from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company 
and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company 
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_int;