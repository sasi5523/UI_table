CREATE TABLE input_table (
    id SERIAL PRIMARY KEY,  -- Auto-incremented ID for each record
    batch_id INT NOT NULL,  -- Identifier for the batch or job
    parent_query_id INT,  -- Reference to the parent query's ID
    condition_column VARCHAR(255),  -- The column on which the condition applies
    condition_operator VARCHAR(50),  -- The operator used for the condition (e.g., =, >, BETWEEN)
    condition_value VARCHAR(255),  -- The value for the condition (could be a range for BETWEEN)
    logical_operator VARCHAR(50),  -- Logical operator (AND, OR)
    is_active BOOLEAN DEFAULT TRUE,  -- Whether the query is active
    is_parent BOOLEAN DEFAULT FALSE,  -- If it's a parent query
    order_position INT,  -- Position/order in the sequence of conditions
    aggregate_function VARCHAR(50),  -- Aggregate functions like SUM, COUNT, etc.
    query_group_id INT,  -- Identifier for grouping parent and subsequent queries
    constant_clause VARCHAR(255),  -- Any constant SQL clause (if needed)
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp for record creation
);
