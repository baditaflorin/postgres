-- Filename: generate_table_ddl.sql

-- Explanation:
-- This SQL script generates Data Definition Language (DDL) statements for all tables in the 'public' schema of the PostgreSQL database. It includes table creation statements along with column definitions and constraints (such as Primary Keys, Foreign Keys, and Unique constraints). The script iterates through each table, retrieves column information and constraints, and constructs the corresponding DDL statements. Finally, it outputs the generated DDL for each table using PostgreSQL's RAISE NOTICE.

-- This script is useful for documenting the structure of tables within the 'public' schema, aiding in database schema analysis and migration planning. It provides a clear overview of the database structure and can be used for version control or database migration purposes.
  
DO $$
DECLARE
    table_rec RECORD;
    column_rec RECORD;
    constraint_rec RECORD;
    ddl TEXT;
BEGIN
    FOR table_rec IN SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    LOOP
        ddl := 'CREATE TABLE ' || quote_ident(table_rec.tablename) || ' (';
        
        -- Columns definition
        FOR column_rec IN SELECT 
            a.attname AS column_name, 
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            CASE 
                WHEN a.attnotnull THEN ' NOT NULL' 
                ELSE '' 
            END AS not_null
        FROM 
            pg_class c
        JOIN 
            pg_attribute a ON a.attrelid = c.oid
        WHERE 
            c.relname = table_rec.tablename AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY 
            a.attnum
        LOOP
            ddl := ddl || quote_ident(column_rec.column_name) || ' ' || column_rec.data_type || column_rec.not_null || ', ';
        END LOOP;
        
        -- Constraints (PK, FK, UNIQUE)
        FOR constraint_rec IN SELECT 
            conname,
            pg_get_constraintdef(oid) AS condef
        FROM 
            pg_constraint
        WHERE 
            conrelid = (SELECT oid FROM pg_class WHERE relname = table_rec.tablename AND relkind = 'r')
        LOOP
            ddl := ddl || constraint_rec.condef || ', ';
        END LOOP;
        
        -- Remove trailing comma and space
        ddl := rtrim(ddl, ', ') || ');';
        
        -- Output the DDL
        RAISE NOTICE '%', ddl;
    END LOOP;
END$$;
