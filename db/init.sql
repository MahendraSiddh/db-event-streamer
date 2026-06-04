-- Status enum type
CREATE TYPE order_status AS ENUM ('pending', 'shipped', 'delivered');

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_name   VARCHAR(255) NOT NULL,
    product_name    VARCHAR(255) NOT NULL,
    status          order_status NOT NULL DEFAULT 'pending',
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Automatically update updated_at column on UPDATE
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_updated_at_trigger ON orders;
CREATE TRIGGER set_updated_at_trigger
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- Set replica identity for replication
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Create publication for orders table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'orders_pub') THEN
        CREATE PUBLICATION orders_pub FOR TABLE orders;
    END IF;
END
$$;

-- Create logical replication slot using the pgoutput plugin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'orders_slot') THEN
        PERFORM pg_create_logical_replication_slot('orders_slot', 'pgoutput');
    END IF;
END
$$;

