CREATE TABLE airbnb_listings (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    host_id INT,
    host_name VARCHAR(255),
    neighbourhood VARCHAR(255),
    price NUMERIC
);


SELECT 
   neighbourhood, 
   ROUND(AVG(price),2) as average_price 
FROM airbnb_listings 
GROUP BY neighbourhood;


SELECT 
   neighbourhood, 
   COUNT(*) as listings 
FROM airbnb_listings 
GROUP BY neighbourhood
ORDER BY listings desc;


SELECT 
   room_type, 
   ROUND(AVG(price),2) as average_price 
FROM airbnb_listings 
GROUP BY room_type
ORDER BY average_price desc;


SELECT 
   host_id,
   host_name, 
   COUNT(*) as listings 
FROM airbnb_listings 
GROUP BY host_id, host_name
ORDER BY listings desc
LIMIT 1;