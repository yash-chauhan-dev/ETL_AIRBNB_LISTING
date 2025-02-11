CREATE TABLE airbnb_listings (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    host_id INT,
    host_name VARCHAR(255),
    neighbourhood VARCHAR(255),
    price FLOAT
);