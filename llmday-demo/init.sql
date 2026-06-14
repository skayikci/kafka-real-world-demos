-- This is what the chatbot queries for context

CREATE TABLE products (
  id     SERIAL PRIMARY KEY,
  name   VARCHAR(100) NOT NULL,
  status VARCHAR(20)  NOT NULL DEFAULT 'available'
);

INSERT INTO products (name, status) VALUES
  ('Widget Pro',    'available'),
  ('Gadget Plus',   'available'),
  ('Thingamajig',  'available'),
  ('Doodad',       'available'),
  ('Gizmo',       'available'),
  ('Super Gadget',  'available'),
  ('Mega Widget',  'available'),
  ('Ultra Thingamajig',  'available'),
  ('Hyper Doodad',  'available'),
  ('Mega Gizmo',  'available'),
  ('Ultra Doodad',  'available'),
  ('Hyper Gizmo',  'available'),
  ('Mega Thingamajig',  'available'),
  ('Ultra Gadget',  'available'),
  ('Hyper Widget',  'available'),
  ('Mega Device',  'available'),
  ('Ultra Device',  'available'),
  ('Hyper Device',  'available'),
  ('Mega Super Device',  'available'),
  ('Ultra Super Device',  'available'),
  ('Hyper Super Device',  'available'),
  ('Mega Super Gadget',  'available'),
  ('Ultra Super Gadget',  'available'),
  ('Hyper Super Gadget',  'available'),
  ('Mega Super Thingamajig',  'available'),
  ('Ultra Super Thingamajig',  'available'),
  ('Hyper Super Thingamajig',  'available'),
  ('Mega Super Thingamajig',  'available'),
  ('Ultra Super Thingamajig',  'available'),
  ('Hyper Super Thingamajig',  'available'),
  ('Mega Super Thingamajig',  'available'),
  ('Ultra Super Thingamajig',  'available'),
  ('Hyper Super Thingamajig',  'available'),
  ('Super Device',  'available');

-- During the demo we will run:
--   UPDATE products SET status = 'discontinued' WHERE name = 'Widget Pro';
-- and watch the CDC event appear live in Kafka UI.


-- To capture changes to the products table, we need to set the REPLICA IDENTITY to FULL. 
-- This allows Debezium to capture the entire row data for updates and deletes, 
-- which is necessary for our Kafka consumer to have the full context of the change events.
ALTER TABLE products REPLICA IDENTITY FULL;