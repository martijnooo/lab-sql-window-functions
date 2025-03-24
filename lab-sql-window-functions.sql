USE sakila;
-- Challenge 1
-- Exercise 1: Rank films by their length and create an output table that includes 
-- the title, length, and rank columns only. Filter out any rows with null or 
-- zero values in the length column.    
SELECT 
    title, 
    length, 
    RANK() OVER (ORDER BY length DESC) AS "rank"
FROM
    film
WHERE
    length IS NOT NULL AND length != 0;

-- Exercise 2: Rank films by length within the rating category and create an output table 
-- that includes the title, length, rating, and rank columns only. Filter out 
-- any rows with null or zero values in the length column.

SELECT title, length, name as category, rank() over(partition by name order by length desc) as "rank"  
FROM film
JOIN film_category
using (film_id)
JOIN category
USING (category_id)
WHERE length IS NOT NULL AND length != 0;


-- Exercise 3: Produce a list that shows for each film in the Sakila database, 
-- the actor or actress who has acted in the greatest number of films, as well 
-- as the total number of films in which they have acted. 
-- Hint: Use temporary tables, CTEs, or Views when appropriate to simplify your queries.
WITH actor_film_count AS (
    SELECT actor_id, COUNT(film_id) AS count_actor_in_films
    FROM film_actor
    GROUP BY actor_id
),
ranked_films AS (
    SELECT title,
           count_actor_in_films,
           CONCAT(first_name, ' ', last_name) AS actor_name,
           RANK() OVER (PARTITION BY fa.film_id ORDER BY count_actor_in_films DESC) AS ranks
    FROM film_actor fa
    JOIN actor_film_count fac ON fa.actor_id = fac.actor_id
    JOIN film f ON fa.film_id = f.film_id
    JOIN actor a ON fa.actor_id = a.actor_id
)

SELECT title, count_actor_in_films, actor_name
FROM ranked_films
WHERE ranks = 1;

/*
WITH ActorFilmCount AS (
    -- Calculate the number of films each actor has acted in
    SELECT a.actor_id, a.first_name, a.last_name, fa.film_id, COUNT(fa.film_id) OVER (PARTITION BY fa.actor_id) AS film_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
)
-- Use window function to rank actors by the number of films per film
SELECT film_id, first_name, last_name, film_count
FROM (
    SELECT afc.film_id, afc.first_name, afc.last_name, afc.film_count,
           RANK() OVER (PARTITION BY afc.film_id ORDER BY afc.film_count DESC) AS ranks
    FROM ActorFilmCount afc
) AS ranked
WHERE ranks = 1
ORDER BY film_id; */

-- Challenge 2
-- Step 1: Retrieve the number of monthly active customers, i.e., the number of unique 
-- customers who rented a movie in each month.
with rental_dates as (SELECT customer_id, rental_date,
		YEAR(rental_date) AS rental_year,
		MONTH(rental_date) AS rental_month
       from rental),
rental_dates_ranked as(
select *, RANK() OVER(Partition by customer_id, rental_year, rental_month ORDER by rental_date) as "ranks"      
from rental_dates)

select rental_year, rental_month, count(customer_id) from rental_dates_ranked
WHERE ranks = 1
GROUP BY rental_year, rental_month;

/* somewhat simplified
with user_rent_in_month as
(SELECT
	date_format(rental_date, '%Y-%m') as rental_month,
    customer_id
from 
	rental
group by
	rental_month,
    customer_id)

SELECT 
	DISTINCT rental_month,
	COUNT(customer_id) over(Partition by rental_month)
FROM 
	user_rent_in_month;
    */

-- without windows function & for further use
create temporary table active_users_per_month AS(
SELECT 
    date_format(rental_date, '%Y-%m') as rental_month,
    COUNT(DISTINCT customer_id) AS active_customers
FROM rental
GROUP BY rental_month
ORDER BY rental_month);

-- Step 2: Retrieve the number of active users in the previous month.
create temporary table active_users_change as (
select *, lag(active_customers,1) over() as active_customers_previous_month 
from active_users_per_month);

select * from active_users_change;

-- Step 3: Calculate the percentage change in the number of active customers 
-- between the current and previous month.

select *, round((active_customers/active_customers_previous_month -1)*100,2) as mom_growth
 from active_users_change;

-- Step 4: Calculate the number of retained customers every month, i.e., 
-- customers who rented movies in the current and previous months. 
-- Hint: Use temporary tables, CTEs, or Views when appropriate to simplify your queries.

/*
with customer_in_month as (
select GROUP_CONCAT(DISTINCT customer_id) as customers, date_format(rental_date, '%Y-%m') as rental_month 
from rental
GROUP BY rental_month),
customer_development as(
select *, lag(customers,1) over() from customer_in_month as customers_prev_month)

select *  from customer_development;*/

-- count with filter

with active_customer_per_month as (
SELECT distinct customer_id, date_format(rental_date, '%Y%m') as rental_month 
FROM rental
GROUP BY rental_month, customer_id),

customer_development as(
SELECT *, lag(rental_month,1) over(partition by customer_id order by rental_month) as prev_period
FROM active_customer_per_month),

-- check the previous month if user is there
retention as (select 
	*, 
	case 
		when prev_period - rental_month = -1 THEN 1 
        ELSE 0
	END AS retained
from customer_development)

select rental_month, sum(retained)
from retention
group by rental_month;