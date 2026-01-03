-- Sample PostgreSQL Data for Animal Database
-- Approximately 1000 rows per table

-- Insert 1000 animals
INSERT INTO animals (animal_id, name, species, age, weight_kg, habitat_name, last_checkup)
SELECT 
    i,
    (ARRAY['Leo', 'Bella', 'Max', 'Luna', 'Charlie', 'Daisy', 'Rocky', 'Molly', 'Buddy', 'Coco', 
           'Oliver', 'Lucy', 'Milo', 'Lola', 'Simba', 'Nala', 'Zeus', 'Athena', 'Thor', 'Freya'])[1 + (i % 20)] || '-' || i,
    (ARRAY['Lion', 'Tiger', 'Bear', 'Elephant', 'Giraffe', 'Zebra', 'Monkey', 'Gorilla', 'Panda', 'Penguin',
           'Eagle', 'Owl', 'Parrot', 'Flamingo', 'Peacock', 'Kangaroo', 'Koala', 'Wolf', 'Fox', 'Deer',
           'Hippo', 'Rhino', 'Crocodile', 'Alligator', 'Snake'])[1 + (i % 25)],
    5 + (i % 20),
    10.5 + (i % 500),
    (ARRAY['Savanna', 'Forest', 'Arctic', 'Desert', 'Rainforest', 'Mountain', 'Wetland', 'Coastal', 'Grassland', 'Tundra'])[1 + (i % 10)],
    DATE '2024-01-01' + (i % 700)
FROM generate_series(1, 1000) AS i;

-- Insert 1000 habitats
INSERT INTO habitats (habitat_id, name, climate, size_acres, capacity, built_date)
SELECT 
    i,
    (ARRAY['Savanna', 'Forest', 'Arctic', 'Desert', 'Rainforest', 'Mountain', 'Wetland', 'Coastal', 'Grassland', 'Tundra'])[1 + (i % 10)] || ' Zone ' || i,
    (ARRAY['Tropical', 'Temperate', 'Arctic', 'Arid', 'Subtropical', 'Mediterranean', 'Continental', 'Polar', 'Oceanic', 'Highland'])[1 + (i % 10)],
    5.0 + (i % 100) * 2.5,
    10 + (i % 50),
    DATE '2000-01-01' + (i % 8000)
FROM generate_series(1, 1000) AS i;

-- Insert 1000 feedings
INSERT INTO feedings (feeding_id, animal_name, food_type, quantity_kg, feeding_time, fed_by)
SELECT 
    i,
    (ARRAY['Leo', 'Bella', 'Max', 'Luna', 'Charlie', 'Daisy', 'Rocky', 'Molly', 'Buddy', 'Coco'])[1 + (i % 10)] || '-' || (1 + (i % 100)),
    (ARRAY['Meat', 'Fish', 'Vegetables', 'Fruits', 'Grains', 'Hay', 'Bamboo', 'Insects', 'Seeds', 'Nectar',
           'Rodents', 'Chicken', 'Beef', 'Carrots', 'Apples', 'Bananas', 'Lettuce', 'Berries', 'Nuts', 'Pellets'])[1 + (i % 20)],
    0.5 + (i % 50) * 0.1,
    TIMESTAMP '2024-01-01 06:00:00' + (i % 365) * INTERVAL '1 day' + (i % 12) * INTERVAL '1 hour',
    (ARRAY['John Smith', 'Jane Doe', 'Bob Johnson', 'Alice Williams', 'Charlie Brown', 
           'Diana Prince', 'Eve Adams', 'Frank Castle', 'Grace Hopper', 'Henry Ford'])[1 + (i % 10)]
FROM generate_series(1, 1000) AS i;
