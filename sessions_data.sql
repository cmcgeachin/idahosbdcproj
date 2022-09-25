-- creates table and sets session_id as primary key
CREATE TABLE session_data (
	customer_id bigint NOT NULL,
	session_date date NOT NULL,
	contact_hours numeric(5,2),
	prep_hours numeric(10,2),
	capital_formation numeric(20,2),
	sales_increase numeric(20,2),
	jobs_created integer, -- The table was created with numeric(5,2) changed after column rearranging.
	naics_code bigint,
	verified text,
	area_of_counseling int,
	session_id bigint CONSTRAINT session_key PRIMARY KEY,
	sub_program text,
	program text,
	staff_id text NOT NULL,
	session_type text NOT NULL
);

-- copies data from crm export, repeated from 2012 - 2021
COPY session_data
FROM '/Users/christophermcgeachin/Desktop/idaho_sbdc_session_analysis/session_data_2012.csv'
WITH (FORMAT CSV, HEADER);

-- after 2012 import returns 7730 rows. 2013: 8091, 2014: 8083, 2015: 8999, 2016: 9403, 2017: 10230, 2018: 10481, 2019: 11970, 2020: 18059, 2021: 16794
SELECT * FROM session_data;

--import of 2013 revealed data issue. .5 of a job should be counted as a whole job. Will need to inspect whole row and correct this.
ERROR:  invalid input syntax for type integer: "1.5"
CONTEXT:  COPY session_data, line 51, column jobs_created: "1.5"
SQL state: 22P02

-- I edited the data type temporarily for the import.
ALTER TABLE session_data ALTER COLUMN jobs_created SET DATA TYPE numeric(5,2);

-- After the import I used a temporary column to fix the data type issue returining to int.
ALTER TABLE session_data ADD column jobs_created_temp int;

-- Setting data for new column
UPDATE session_data
SET jobs_created_temp = round(jobs_created,0);


-- Checking that rounding did not alter results. Returns 0 records
SELECT session_date, 
	jobs_created,
	jobs_created_temp,
	jobs_created_temp - jobs_created AS diff
FROM session_data
WHERE jobs_created_temp - jobs_created > .5
ORDER BY jobs_created DESC;

--DROP and Rename columns
ALTER TABLE session_data DROP COLUMN jobs_created;

ALTER TABLE session_data RENAME COLUMN jobs_created_temp TO jobs_created;

-- Total rows 109840
SELECT * 
FROM session_data;

-- Data Analysis Can Begin:

-- SUM of Sales Increase, capital raised, jobs created, contact hours, clients, and sessions by year and center.
WITH centers AS
	(SELECT center, staff_id
	FROM staff_and_centers)
	
SELECT 
	date_part('year', session_date) AS year,
	center,
	sum(sales_increase) AS sales_increase,
	sum(capital_formation) AS capital_raised,
	sum(jobs_created) AS jobs_created,
	sum(contact_hours) AS contact_hours,
	count(DISTINCT customer_id) AS clients,
	count(session_id) AS sessions
FROM session_data JOIN centers
ON session_data.staff_id = centers.staff_id
WHERE center != '99 Lead Office'
GROUP BY year, center
ORDER BY year, center;

-- CORR sales increase to jobs_created: r= .428 moderate relationship on 1993 records r2=.184, Y= .00001(x) + 2.7
WITH
	aggregates (sales, jobs, customer_id) AS
	(SELECT sum(sales_increase) AS sales, sum(jobs_created) AS jobs, customer_id
	FROM session_data
	GROUP BY customer_id)
	
SELECT 
	round(
		corr(jobs, sales)::numeric, 3) AS r,
	round(
		regr_r2(jobs, sales)::numeric, 3) AS r_squared,
	round(
		regr_slope(jobs, sales)::numeric, 5) AS slope,
	round(regr_intercept(jobs, sales)::numeric, 2) AS y_intercept
FROM aggregates
WHERE sales IS NOT NULL;

-- CORR Session count to Sales Increase: weak relationship WITH
	aggregates (sales, jobs, customer_id) AS
	(SELECT sum(sales_increase) AS sales, sum(jobs_created) AS jobs, customer_id
	FROM session_data
	GROUP BY customer_id)
	
SELECT 
	round(
		corr(jobs, sales)::numeric, 3) AS r,
	round(
		regr_r2(jobs, sales)::numeric, 3) AS r_squared,
	round(
		regr_slope(jobs, sales)::numeric, 5) AS slope,
	round(regr_intercept(jobs, sales)::numeric, 2) AS y_intercept
FROM aggregates
WHERE sales IS NOT NULL;


-- Session Count per year for average trend
SELECT
	date_part('year', session_date) AS year,
	count(session_id) AS sessions
FROM session_data
GROUP BY year
ORDER BY year;


--eRFC count per year
SELECT
	date_part('year', session_date) AS year,
	session_type,
	count(*)
FROM session_data
WHERE session_type = 'Internet RFC'
GROUP BY year, session_type
ORDER BY year, session_type;

-- SUM sales increase by customer_id
SELECT 
	customer_id,
	sum(sales_increase) as sum_sales_increase
FROM session_data
WHERE sales_increase IS NOT NULL
GROUP BY customer_id
ORDER BY sum_sales_increase DESC;

-- Median sales per session ranked and sessions counted, median_jobs per consultant, median_capital per consultant
WITH 
	sales_median (staff_id, median_sales, session_count_sales) AS
	(SELECT staff_id, percentile_cont(.5)
	WITHIN GROUP (ORDER BY sales_increase) AS median_sales,
	count(*) AS session_count_sales
	FROM session_data
	WHERE sales_increase > 1
	GROUP BY staff_id),
	
	jobs_median (staff_id, median_jobs, session_count_jobs) AS
	(SELECT staff_id, percentile_cont(.5)
	WITHIN GROUP (ORDER BY jobs_created) AS median_jobs,
	Count(*) AS session_count_jobs
	FROM session_data
	WHERE jobs_created > 1
	GROUP BY staff_id),

	capital_median (staff_id, median_capital, session_count_capital) AS
	(SELECT staff_id, percentile_cont(.5)
	WITHIN GROUP (ORDER BY capital_formation) AS median_capital,
	count(*) AS session_count_capital
	FROM session_data
	WHERE capital_formation > 1
	GROUP BY staff_id)
	 
SELECT sales_median.staff_id,
	 sales_median.median_sales,
	 sales_median.session_count_sales,
	 jobs_median.median_jobs,
	 jobs_median.session_count_jobs,
	 capital_median.median_capital,
	 capital_median.session_count_capital
	 
FROM sales_median JOIN jobs_median
ON sales_median.staff_id = jobs_median.staff_id
JOIN capital_median ON sales_median.staff_id = capital_median.staff_id
WHERE sales_median.session_count_sales > 3 AND sales_median.staff_id IN 
			('dnoack', 'brjussel', 'dweed', 'dburgett', 'tbroadman',
			 'bjhung', 'bmatsuoka', 'brmagleby', 'dwinkler', 'aswanson',
			 'callen', 'WMueller', 'ruthschwartz', 'fwilson', 'mariebaker',
			 'klabrum')
ORDER BY median_sales, median_capital DESC;

-- Impact by consultant sum and efficiency 
WITH 
	sales_sum (staff_id, sales_sum, session_count_sales, sales_efficiency) AS
	(SELECT staff_id, sum(sales_increase) AS sales_sum,
	count(*) AS session_count_sales,
	round(sum(sales_increase)/count(*)::numeric, 2) 
	 	AS sales_efficiency
	FROM session_data
	GROUP BY staff_id),
	
	jobs_sum (staff_id, jobs_sum, session_count_jobs, jobs_efficiency) AS
	(SELECT staff_id, sum(jobs_created) AS jobs_sum,
	Count(*) AS session_count_jobs,
	round(sum(jobs_created)/count(*)::numeric, 5) 
	 	AS jobs_efficiency
	FROM session_data
	GROUP BY staff_id),

	capital_sum (staff_id, capital_sum, session_count_capital, capital_efficiency) AS
	(SELECT staff_id, sum(capital_formation) AS capital_sum,
	count(*) AS session_count_capital,
	round(sum(capital_formation)/count(*)::numeric, 2) 
	 	AS capital_efficiency
	FROM session_data
	GROUP BY staff_id)
	 
SELECT sales_sum.staff_id,
	 sales_sum.sales_sum,
	 sales_sum.session_count_sales
	 sales_sum.sales_efficiency,
	 jobs_sum.jobs_sum,
	 jobs_sum.session_count_jobs,
	 jobs_sum.jobs_efficiency,
	 capital_sum.capital_sum,
	 capital_sum.session_count_capital,
	 capital_sum.capital_efficiency
	 
FROM sales_sum JOIN jobs_sum
ON sales_sum.staff_id = jobs_sum.staff_id
JOIN capital_sum ON sales_sum.staff_id = capital_sum.staff_id
WHERE sales_sum.session_count_sales > 3 AND sales_sum.staff_id IN 
			('dnoack', 'brjussel', 'dweed', 'dburgett', 'tbroadman',
			 'bjhung', 'bmatsuoka', 'brmagleby', 'dwinkler', 'aswanson',
			 'callen', 'WMueller', 'ruthschwartz', 'fwilson', 'mariebaker',
			 'klabrum')
ORDER BY sales_efficiency DESC, capital_efficiency DESC;

-- Count of non-impact and impact sessions to see if there is a major pattern shift
WITH non_impact_sessions (non_impact_session_count, session_date) AS
	(SELECT count(session_id) AS non_impact_session_count, session_date
	FROM session_data
	WHERE jobs_created < 1 OR NULL AND sales_increase < 1 OR NULL AND capital_formation < 1 OR NULL
	GROUP BY session_date),
	
	impact_sessions (impact_session_count, session_date) AS
	(SELECT count(session_id) AS impact_session_count, session_date
	FROM session_data
	WHERE jobs_created >= 1 OR sales_increase >=1 OR capital_formation >= 1
	GROUP BY session_date)
	
SELECT non_impact_sessions.session_date,
	non_impact_sessions.non_impact_session_count,
	impact_sessions.impact_session_count
FROM non_impact_sessions JOIN impact_sessions 
	ON non_impact_sessions.session_date = impact_sessions.session_date
ORDER BY session_date;



