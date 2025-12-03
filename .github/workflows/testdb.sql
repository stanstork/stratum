-- Create Database
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

-- Create Users Table
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Products Table
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INT NOT NULL DEFAULT 0
);

-- Create Orders Table
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total DECIMAL(10,2) DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Create Order Items Table (Many-to-Many relation between Orders and Products)
CREATE TABLE order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

-- Insert Users
INSERT INTO users (name, email) VALUES
('Alice Johnson', 'alice@example.com'),
('Bob Smith', 'bob@example.com'),
('Charlie Brown', 'charlie@example.com');

-- Insert Products
INSERT INTO products (name, price, stock) VALUES
('Laptop', 1200.00, 10),
('Smartphone', 800.00, 20),
('Tablet', 400.00, 15),
('Headphones', 150.00, 30);

-- Insert Orders
INSERT INTO orders (user_id, total) VALUES
(1, 2000.00), -- Order for Alice
(2, 1200.00), -- Order for Bob
(3, 400.00);  -- Order for Charlie

-- Insert Order Items
INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
(1, 1, 1, 1200.00), -- Alice bought 1 Laptop
(1, 2, 1, 800.00),  -- Alice bought 1 Smartphone
(2, 1, 1, 1200.00), -- Bob bought 1 Laptop
(3, 3, 1, 400.00);  -- Charlie bought 1 Tablet

-- Update Order Totals based on order_items
UPDATE orders
SET total = (SELECT SUM(quantity * price) FROM order_items WHERE order_items.order_id = orders.id);

-- Verify Data
SELECT * FROM users;
SELECT * FROM products;
SELECT * FROM orders;
SELECT * FROM order_items;
