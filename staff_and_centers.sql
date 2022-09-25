-- creates table and sets staff_id as primary key
CREATE TABLE staff_and_centers (
	staff_id text CONSTRAINT staff_and_centers_key PRIMARY KEY,
	center text,
	staff_type text,
	staff_title text
);

-- copies data from export into postgresql server
COPY staff_and_centers
FROM '/Users/christophermcgeachin/Desktop/idaho_sbdc_session_analysis/staff_and_centers.csv'
WITH (FORMAT CSV, HEADER);

-- all 329 records copied over successfully. Only staff_title contains null values
SELECT * FROM staff_and_centers;