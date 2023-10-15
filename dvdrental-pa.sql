-- Perform transformation on status column from integer to a user-friendly string.
CREATE OR REPLACE FUNCTION status(active INTEGER)
  RETURNS CHAR(8)
  LANGUAGE plpgsql
AS
$$
DECLARE customer_status VARCHAR(8);
BEGIN
  SELECT CASE
          WHEN active = 1 THEN 'Active'
          WHEN active = 0 THEN 'Inactive'
         END INTO customer_status;
  RETURN customer_status;
END;
$$

-- Drop detailed table if already exists, then creates a new detailed table with zero rows
DROP TABLE IF EXISTS detailed;
CREATE TABLE detailed (
  store_id SMALLINT,
  status VARCHAR(8),
  first_name VARCHAR(45),
  last_name VARCHAR(45),
  email VARCHAR(50),
  phone VARCHAR(20),
  num_of_times_rented BIGINT,
  title VARCHAR(255),
  genre VARCHAR(25),
  description TEXT,
  rental_rate NUMERIC(4,2),
  address VARCHAR(50),
  address2 VARCHAR(50),
  district VARCHAR(20),
  city VARCHAR(50),
  country VARCHAR(50),
  postal_code VARCHAR(10)
);

-- Uncomment line below to check detailed table columns, data types, and no rows
-- SELECT * FROM detailed;

-- Drop summary table if already exists, then create a new summary table with zero rows
DROP TABLE IF EXISTS summary;
CREATE TABLE summary (
  store_id SMALLINT,
  first_name VARCHAR(45),
  last_name VARCHAR(45),
  status VARCHAR(8),
  total_rentals BIGINT
);

-- Uncomment line below to check summary table columns, data types, and no rows
-- SELECT * FROM summary;

-- Select the following columns from the joining of the following tables and insert the resulting rows into the detailed table grouped by and ordered by
INSERT INTO detailed
SELECT c.store_id,
       fn_status(c.active) AS status,
       c.first_name,
       c.last_name,
       c.email,
       a.phone,
       COUNT(r.rental_id) AS num_of_times_rented,
       f.title,
       cat.name AS genre,
       f.description,
       f.rental_rate,
       a.address,
       a.address2,
       a.district,
       city.city,
       country.country,
       a.postal_code
FROM customer c
JOIN address a
ON c.address_id = a.address_id
JOIN city
ON city.city_id = a.city_id
JOIN country
ON country.country_id = city.country_id
JOIN rental r
ON c.customer_id = r.customer_id
JOIN inventory i
ON i.inventory_id = r.inventory_id
JOIN film f
ON f.film_id = i.film_id
JOIN film_category fc
ON fc.film_id = f.film_id
JOIN category cat
ON cat.category_id = fc.category_id
GROUP BY status,
         c.first_name,
         c.last_name,
         c.email,
         a.address,
         a.address2,
         a.district,
         city.city,
         country.country,
         a.postal_code,
         a.phone,
         f.title,
         genre,
         f.description,
         f.rental_rate,
         c.store_id
ORDER BY c.first_name, genre;

-- Uncomment line below to ensure rows are inserted into detailed table
-- SELECT * FROM detailed;

-- Select the following columns from the detailed table and insert the resulting rows into the summary table grouped by and ordered by
INSERT INTO summary
SELECT store_id, first_name, last_name, status, SUM(num_of_times_rented) AS total_rentals
FROM detailed
GROUP BY first_name, last_name, status, store_id
ORDER BY total_rentals DESC, first_name;

-- Uncomment line below to ensure rows are inserted into summary table
-- SELECT * FROM summary;

-- Creates a trigger on the detailed table of the report that will continually update the summary table as data is added to the detailed table.

-- step 1: Trigger function
CREATE OR REPLACE FUNCTION insert_new_rental_into_summary_from_detailed()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM summary; -- delete all data from table
  INSERT INTO summary
  SELECT store_id, first_name, last_name, status, SUM(num_of_times_rented) AS total_rentals
  FROM detailed
  GROUP BY first_name, last_name, status, store_id
  ORDER BY total_rentals DESC, first_name;
  RETURN NEW;
END;
$$;

-- step 2: Create trigger statement
CREATE TRIGGER new_rental_for_summary
  AFTER INSERT
  ON detailed
  FOR EACH STATEMENT
    EXECUTE PROCEDURE insert_new_rental_into_summary_from_detailed();

-- Uncomment lines below to get current totals for each table to compare after inserting new row to test trigger function and statement work
-- SELECT SUM(num_of_times_rented) FROM detailed;
-- SELECT SUM(total_rentals) FROM summary; -- output should match output from statement above

-- Uncomment to insert test customer to ensure trigger works
-- INSERT INTO detailed (store_id, status, first_name, last_name, email, num_of_times_rented, title, genre) VALUES
--             (1, 'Active', 'Test', 'Customer', 'test@email.com', 1000, 'Test Film Title', 'Action');

-- Uncomment code below to get totals after inserting a row to make sure trigger function and statement are working
-- SELECT SUM(num_of_times_rented) FROM detailed;
-- SELECT SUM(total_rentals) FROM summary; -- output should match output from statement above
-- SELECT * FROM summary; -- this should show Test Customer as first record with 1,000 total rentals

-- Creates a stored procedure that can be used to refresh the data in both the detailed table and summary table. This will clear the detailed table, perform the data extraction and insert data in the detailed table. This will then invoke the trigger function to update the summary table.

CREATE OR REPLACE PROCEDURE refresh_detailed_and_summary()
LANGUAGE plpgsql
AS $$
BEGIN
  -- Clears table content
  DELETE FROM detailed;

  -- Select the following columns from the joining of the following tables and insert the resulting rows into the detailed table grouped by and ordered by
  INSERT INTO detailed
  SELECT c.store_id,
        status(c.active) AS status,
        c.first_name,
        c.last_name,
        c.email,
        a.phone,
        COUNT(r.rental_id) AS num_of_times_rented,
        f.title,
        cat.name AS genre,
        f.description,
        f.rental_rate,
        a.address,
        a.address2,
        a.district,
        city.city,
        country.country,
        a.postal_code
  FROM customer c
  JOIN address a
  ON c.address_id = a.address_id
  JOIN city
  ON city.city_id = a.city_id
  JOIN country
  ON country.country_id = city.country_id
  JOIN rental r
  ON c.customer_id = r.customer_id
  JOIN inventory i
  ON i.inventory_id = r.inventory_id
  JOIN film f
  ON f.film_id = i.film_id
  JOIN film_category fc
  ON fc.film_id = f.film_id
  JOIN category cat
  ON cat.category_id = fc.category_id
  GROUP BY status,
          c.first_name,
          c.last_name,
          c.email,
          a.address,
          a.address2,
          a.district,
          city.city,
          country.country,
          a.postal_code,
          a.phone,
          f.title,
          genre,
          f.description,
          f.rental_rate,
          c.store_id
  ORDER BY c.first_name, genre;
END;
$$;

-- A job scheduling tool that could be used to automate the stored procedure is pgAgent
-- For testing purposes call this manually
CALL refresh_detailed_and_summary();

-- Uncomment code below to get current totals for each table to test the stored procedure works at refereshing data
-- SELECT SUM(num_of_times_rented) FROM detailed;
-- SELECT SUM(total_rentals) FROM summary; -- output should match output from statement above
-- SELECT * FROM summary; -- this should no longer show Test Customer as first record with 1,000 total rentals
-- SELECT * FROM detailed WHERE first_name = 'Test'; -- this should not return any results
-- SELECT * FROM summary WHERE first_name = 'Test';
