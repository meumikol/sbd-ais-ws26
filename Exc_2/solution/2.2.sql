-- Activity 2.2: Why is this self-join so slow?
\timing on

-- Step 1: smaller copies
DROP TABLE IF EXISTS people_50k, people_100k, people_200k;
CREATE TABLE people_50k  AS SELECT * FROM people_big WHERE id <= 50000;
CREATE TABLE people_100k AS SELECT * FROM people_big WHERE id <= 100000;
CREATE TABLE people_200k AS SELECT * FROM people_big WHERE id <= 200000;

SELECT COUNT(*) FROM people_50k  p1 JOIN people_50k  p2 ON p1.country = p2.country;
SELECT COUNT(*) FROM people_100k p1 JOIN people_100k p2 ON p1.country = p2.country;
SELECT COUNT(*) FROM people_200k p1 JOIN people_200k p2 ON p1.country = p2.country;

-- Step 2: does an index help?
EXPLAIN ANALYZE
SELECT COUNT(*) FROM people_100k p1 JOIN people_100k p2 ON p1.country = p2.country;

CREATE INDEX idx_people_100k_country ON people_100k(country);
ANALYZE people_100k;

EXPLAIN ANALYZE
SELECT COUNT(*) FROM people_100k p1 JOIN people_100k p2 ON p1.country = p2.country;

-- Step 3: rewrite without a join: a country with k people yields k*k pairs
SELECT SUM(cnt * cnt) AS pairs
FROM (
  SELECT country, COUNT(*) AS cnt
  FROM people_100k
  WHERE country IS NOT NULL      -- NULLs never match in an equi-join
  GROUP BY country
) t;

-- Same rewrite on the full table
SELECT SUM(cnt * cnt) AS pairs
FROM (
  SELECT country, COUNT(*) AS cnt
  FROM people_big
  WHERE country IS NOT NULL
  GROUP BY country
) t;
