CREATE TABLE customer_dim (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    city VARCHAR(255),
    current_job VARCHAR(255),
    previous_job VARCHAR(255),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_active BOOLEAN
);
