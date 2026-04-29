CREATE USER streamflow WITH PASSWORD 'streamflow123' CREATEDB;

CREATE DATABASE streamflow_source OWNER streamflow;
CREATE DATABASE airflow_db OWNER streamflow;

\c streamflow_source

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE IF NOT EXISTS users (
    user_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email       VARCHAR(255) NOT NULL UNIQUE,
    name        VARCHAR(255) NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    country     VARCHAR(100),
    segment     VARCHAR(50) CHECK (segment IN ('bronze', 'silver', 'gold', 'platinum'))
);

CREATE TABLE IF NOT EXISTS products (
    product_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    price           NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity  INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(user_id),
    status          VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'refunded')),
    total_amount    NUMERIC(12, 2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id    UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id  UUID NOT NULL REFERENCES products(product_id),
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0)
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_segment ON users(segment);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_orders_updated_at ON orders;
CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_slot'
);

INSERT INTO users (email, name, country, segment) VALUES
('alice.johnson@example.com', 'Alice Johnson', 'US', 'gold'),
('bob.smith@example.com', 'Bob Smith', 'UK', 'silver'),
('carol.white@example.com', 'Carol White', 'DE', 'platinum'),
('david.brown@example.com', 'David Brown', 'CA', 'bronze'),
('eva.martinez@example.com', 'Eva Martinez', 'ES', 'silver'),
('frank.lee@example.com', 'Frank Lee', 'JP', 'gold'),
('grace.kim@example.com', 'Grace Kim', 'KR', 'bronze'),
('henry.wang@example.com', 'Henry Wang', 'CN', 'silver'),
('iris.taylor@example.com', 'Iris Taylor', 'AU', 'platinum'),
('james.anderson@example.com', 'James Anderson', 'US', 'gold'),
('karen.thomas@example.com', 'Karen Thomas', 'US', 'silver'),
('liam.jackson@example.com', 'Liam Jackson', 'UK', 'bronze'),
('mia.harris@example.com', 'Mia Harris', 'CA', 'gold'),
('noah.martin@example.com', 'Noah Martin', 'FR', 'platinum'),
('olivia.garcia@example.com', 'Olivia Garcia', 'MX', 'silver'),
('peter.rodriguez@example.com', 'Peter Rodriguez', 'MX', 'bronze'),
('quinn.lewis@example.com', 'Quinn Lewis', 'US', 'gold'),
('rachel.lee@example.com', 'Rachel Lee', 'SG', 'silver'),
('samuel.walker@example.com', 'Samuel Walker', 'NG', 'bronze'),
('tina.hall@example.com', 'Tina Hall', 'ZA', 'silver'),
('ulysses.allen@example.com', 'Ulysses Allen', 'BR', 'gold'),
('victoria.young@example.com', 'Victoria Young', 'AR', 'platinum'),
('william.hernandez@example.com', 'William Hernandez', 'CO', 'bronze'),
('xena.king@example.com', 'Xena King', 'IN', 'silver'),
('yusuf.wright@example.com', 'Yusuf Wright', 'PK', 'gold'),
('zoe.scott@example.com', 'Zoe Scott', 'NZ', 'silver'),
('adam.green@example.com', 'Adam Green', 'IE', 'bronze'),
('bella.adams@example.com', 'Bella Adams', 'IT', 'gold'),
('chris.baker@example.com', 'Chris Baker', 'PT', 'silver'),
('diana.nelson@example.com', 'Diana Nelson', 'GR', 'platinum'),
('ethan.carter@example.com', 'Ethan Carter', 'PL', 'bronze'),
('fiona.mitchell@example.com', 'Fiona Mitchell', 'SE', 'silver'),
('george.perez@example.com', 'George Perez', 'NO', 'gold'),
('hannah.roberts@example.com', 'Hannah Roberts', 'DK', 'silver'),
('ivan.turner@example.com', 'Ivan Turner', 'FI', 'bronze'),
('julia.phillips@example.com', 'Julia Phillips', 'CH', 'gold'),
('kevin.campbell@example.com', 'Kevin Campbell', 'AT', 'silver'),
('laura.parker@example.com', 'Laura Parker', 'BE', 'platinum'),
('michael.evans@example.com', 'Michael Evans', 'NL', 'bronze'),
('natalie.edwards@example.com', 'Natalie Edwards', 'CZ', 'silver'),
('oliver.collins@example.com', 'Oliver Collins', 'HU', 'gold'),
('patricia.stewart@example.com', 'Patricia Stewart', 'RO', 'silver'),
('robert.sanchez@example.com', 'Robert Sanchez', 'SK', 'bronze'),
('sophia.morris@example.com', 'Sophia Morris', 'HR', 'gold'),
('thomas.rogers@example.com', 'Thomas Rogers', 'BG', 'silver'),
('una.reed@example.com', 'Una Reed', 'EE', 'platinum'),
('victor.cook@example.com', 'Victor Cook', 'LV', 'bronze'),
('wendy.morgan@example.com', 'Wendy Morgan', 'LT', 'silver'),
('xavier.bell@example.com', 'Xavier Bell', 'SI', 'gold'),
('yasmine.murphy@example.com', 'Yasmine Murphy', 'LU', 'silver'),
('zachary.bailey@example.com', 'Zachary Bailey', 'MT', 'bronze'),
('anna.rivera@example.com', 'Anna Rivera', 'CY', 'gold'),
('brian.cooper@example.com', 'Brian Cooper', 'IS', 'silver'),
('chloe.richardson@example.com', 'Chloe Richardson', 'NO', 'platinum'),
('daniel.cox@example.com', 'Daniel Cox', 'UK', 'bronze'),
('elena.howard@example.com', 'Elena Howard', 'US', 'silver'),
('felix.ward@example.com', 'Felix Ward', 'CA', 'gold'),
('gina.torres@example.com', 'Gina Torres', 'DE', 'silver'),
('hector.peterson@example.com', 'Hector Peterson', 'FR', 'bronze'),
('ingrid.gray@example.com', 'Ingrid Gray', 'JP', 'gold'),
('jason.ramirez@example.com', 'Jason Ramirez', 'KR', 'silver'),
('kelly.james@example.com', 'Kelly James', 'AU', 'platinum'),
('leon.watson@example.com', 'Leon Watson', 'CN', 'bronze'),
('mary.brooks@example.com', 'Mary Brooks', 'IN', 'silver'),
('nick.kelly@example.com', 'Nick Kelly', 'SG', 'gold'),
('ophelia.sanders@example.com', 'Ophelia Sanders', 'TH', 'silver'),
('paul.price@example.com', 'Paul Price', 'VN', 'bronze'),
('qiana.bennett@example.com', 'Qiana Bennett', 'MY', 'gold'),
('rick.wood@example.com', 'Rick Wood', 'ID', 'silver'),
('stella.barnes@example.com', 'Stella Barnes', 'PH', 'platinum'),
('terry.ross@example.com', 'Terry Ross', 'BD', 'bronze'),
('ursula.henderson@example.com', 'Ursula Henderson', 'LK', 'silver'),
('vincent.coleman@example.com', 'Vincent Coleman', 'NP', 'gold'),
('wanda.jenkins@example.com', 'Wanda Jenkins', 'MM', 'silver'),
('xerxes.perry@example.com', 'Xerxes Perry', 'KH', 'bronze'),
('yolanda.powell@example.com', 'Yolanda Powell', 'LA', 'gold'),
('zero.long@example.com', 'Zero Long', 'MN', 'silver'),
('alex.patterson@example.com', 'Alex Patterson', 'KZ', 'platinum'),
('betty.hughes@example.com', 'Betty Hughes', 'UZ', 'bronze'),
('carl.flores@example.com', 'Carl Flores', 'TM', 'silver'),
('dorothy.washington@example.com', 'Dorothy Washington', 'AZ', 'gold'),
('ernest.butler@example.com', 'Ernest Butler', 'GE', 'silver'),
('florence.simmons@example.com', 'Florence Simmons', 'AM', 'bronze'),
('glen.foster@example.com', 'Glen Foster', 'TR', 'gold'),
('helen.gonzales@example.com', 'Helen Gonzales', 'IL', 'silver'),
('ira.bryant@example.com', 'Ira Bryant', 'JO', 'platinum'),
('jolene.alexander@example.com', 'Jolene Alexander', 'LB', 'bronze'),
('keith.russell@example.com', 'Keith Russell', 'SA', 'silver'),
('lorena.griffin@example.com', 'Lorena Griffin', 'AE', 'gold'),
('mario.diaz@example.com', 'Mario Diaz', 'QA', 'silver'),
('nelly.hayes@example.com', 'Nelly Hayes', 'KW', 'bronze'),
('oscar.myers@example.com', 'Oscar Myers', 'BH', 'gold'),
('pamela.ford@example.com', 'Pamela Ford', 'OM', 'silver'),
('quinton.hamilton@example.com', 'Quinton Hamilton', 'YE', 'platinum'),
('rosa.graham@example.com', 'Rosa Graham', 'IQ', 'bronze'),
('steven.sullivan@example.com', 'Steven Sullivan', 'IR', 'silver'),
('tamara.wallace@example.com', 'Tamara Wallace', 'AF', 'gold'),
('umberto.west@example.com', 'Umberto West', 'PK', 'silver')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (name, category, price, stock_quantity) VALUES
('Wireless Noise-Cancelling Headphones', 'Electronics', 299.99, 150),
('4K Ultra HD Smart TV 55"', 'Electronics', 799.99, 60),
('Mechanical Gaming Keyboard', 'Electronics', 129.99, 200),
('Ergonomic Office Chair', 'Furniture', 449.99, 80),
('Standing Desk Converter', 'Furniture', 189.99, 120),
('Python Programming Masterclass', 'Books', 49.99, 500),
('Data Engineering Fundamentals', 'Books', 59.99, 350),
('Yoga Mat Premium', 'Sports', 39.99, 300),
('Running Shoes - Pro Series', 'Sports', 149.99, 250),
('Stainless Steel Water Bottle', 'Sports', 29.99, 400),
('Air Purifier HEPA Filter', 'Home', 219.99, 100),
('Robot Vacuum Cleaner', 'Home', 349.99, 75),
('Bluetooth Speaker Portable', 'Electronics', 89.99, 180),
('USB-C Hub 10-in-1', 'Electronics', 69.99, 220),
('Webcam 4K Streaming', 'Electronics', 119.99, 160),
('Coffee Maker Programmable', 'Kitchen', 89.99, 130),
('Instant Pot 7-in-1', 'Kitchen', 99.99, 110),
('Cast Iron Skillet Set', 'Kitchen', 79.99, 90),
('Vitamin D3 + K2 Supplement', 'Health', 24.99, 600),
('Protein Powder Whey Isolate', 'Health', 54.99, 280),
('Resistance Bands Set', 'Sports', 34.99, 350),
('LED Desk Lamp with USB', 'Electronics', 44.99, 240),
('Wireless Charging Pad', 'Electronics', 34.99, 300),
('Laptop Stand Adjustable', 'Electronics', 49.99, 200),
('Monitor Arm Dual Screen', 'Electronics', 89.99, 130),
('Cotton Bedsheet Set King', 'Home', 69.99, 180),
('Memory Foam Pillow', 'Home', 54.99, 220),
('Blackout Curtains 84"', 'Home', 49.99, 160),
('Digital Kitchen Scale', 'Kitchen', 24.99, 400),
('French Press Coffee Maker', 'Kitchen', 39.99, 200),
('Himalayan Salt Lamp', 'Home', 29.99, 300),
('Aromatherapy Diffuser', 'Home', 44.99, 250),
('Noise Machine Sleep Aid', 'Health', 39.99, 200),
('Foam Roller Massage', 'Sports', 29.99, 320),
('Jump Rope Speed Cable', 'Sports', 19.99, 500),
('Stainless Steel Cookware Set', 'Kitchen', 199.99, 80),
('Air Fryer Digital 5.5L', 'Kitchen', 129.99, 140),
('Bamboo Cutting Board Set', 'Kitchen', 44.99, 260),
('Sunscreen SPF 50 Mineral', 'Health', 19.99, 800),
('Vitamin C Serum 20%', 'Health', 34.99, 450),
('Hyaluronic Acid Moisturizer', 'Health', 29.99, 500),
('Retinol Night Cream', 'Health', 49.99, 350),
('Electric Toothbrush Premium', 'Health', 79.99, 190),
('Posture Corrector Smart', 'Health', 59.99, 200),
('Fingerprint Lock Smart', 'Home', 89.99, 120),
('Security Camera Outdoor', 'Home', 119.99, 100),
('Doorbell Camera Smart', 'Home', 149.99, 90),
('Smart Plug 4-Pack WiFi', 'Home', 34.99, 380),
('LED Strip Lights 10m', 'Home', 24.99, 500),
('Power Bank 26800mAh', 'Electronics', 59.99, 270)
ON CONFLICT DO NOTHING;
