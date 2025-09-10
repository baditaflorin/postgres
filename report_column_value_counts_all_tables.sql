-- Filename suggestion: report_column_value_counts.sql
-- Purpose: For every table/column in a schema, report non-NULL counts and (for text) non-empty counts.
-- Efficient: builds one aggregate SELECT per table and unpivots via arrays, so each table is scanned only once.

DROP FUNCTION IF EXISTS util_report_column_value_counts(text);

CREATE OR REPLACE FUNCTION util_report_column_value_counts(target_schema text DEFAULT 'public')
RETURNS TABLE (
    table_schema            text,
    table_name              text,
    column_name             text,
    data_type               text,
    ordinal_position        int,
    total_rows              bigint,
    non_null_count          bigint,
    null_count              bigint,
    non_empty_text_count    bigint,
    pct_filled              numeric(6,2),
    pct_non_empty_text      numeric(6,2)
)
LANGUAGE plpgsql
AS $$
DECLARE
    tbl record;
    names_sql           text;
    types_sql           text;
    ords_sql            text;
    nonnulls_expr       text;
    nonempties_expr     text;
    dyn_sql             text;
BEGIN
    FOR tbl IN
        SELECT t.table_schema, t.table_name
        FROM information_schema.tables AS t
        WHERE t.table_schema = target_schema
          AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_schema, t.table_name
    LOOP
        SELECT
            string_agg(format('%L', c.column_name), ', ' ORDER BY c.ordinal_position) AS names_sql,
            string_agg(format('%L', c.data_type), ', ' ORDER BY c.ordinal_position)   AS types_sql,
            string_agg((c.ordinal_position)::text, ', ' ORDER BY c.ordinal_position)  AS ords_sql,
            string_agg(format('count(%1$I)::bigint', c.column_name), ', ' ORDER BY c.ordinal_position) AS nonnulls_expr,
            string_agg(
                CASE
                    WHEN c.udt_name IN ('text','varchar','bpchar','citext')
                        THEN format('count(nullif(btrim(%1$I::text), %2$L))::bigint', c.column_name, '')
                    ELSE 'NULL::bigint'
                END,
                ', ' ORDER BY c.ordinal_position
            ) AS nonempties_expr
        INTO names_sql, types_sql, ords_sql, nonnulls_expr, nonempties_expr
        FROM information_schema.columns c
        WHERE c.table_schema = tbl.table_schema
          AND c.table_name   = tbl.table_name;

        IF names_sql IS NULL THEN
            CONTINUE;
        END IF;

        dyn_sql := format($f$
            WITH agg AS (
                SELECT
                    count(*)::bigint                                  AS total_rows,
                    ARRAY[%s]::text[]                                 AS col_names,
                    ARRAY[%s]::text[]                                 AS data_types,
                    ARRAY[%s]::int[]                                  AS ordinals,
                    ARRAY[%s]::bigint[]                               AS non_null_counts,
                    ARRAY[%s]::bigint[]                               AS non_empty_text_counts
                FROM %I.%I
            )
            SELECT
                %L AS table_schema,
                %L AS table_name,
                agg.col_names[i]                 AS column_name,
                agg.data_types[i]                AS data_type,
                agg.ordinals[i]                  AS ordinal_position,
                agg.total_rows                   AS total_rows,
                agg.non_null_counts[i]           AS non_null_count,
                (agg.total_rows - agg.non_null_counts[i]) AS null_count,
                agg.non_empty_text_counts[i]     AS non_empty_text_count,
                CASE WHEN agg.total_rows > 0
                     THEN round(100.0 * agg.non_null_counts[i]::numeric / agg.total_rows, 2)
                     ELSE 0 END                  AS pct_filled,
                CASE
                    WHEN agg.total_rows > 0 AND agg.non_empty_text_counts[i] IS NOT NULL
                    THEN round(100.0 * agg.non_empty_text_counts[i]::numeric / agg.total_rows, 2)
                    ELSE NULL
                END                              AS pct_non_empty_text
            FROM agg, generate_subscripts(agg.col_names, 1) AS g(i)
            ORDER BY ordinal_position;
        $f$,
            names_sql, types_sql, ords_sql, nonnulls_expr, nonempties_expr,
            tbl.table_schema, tbl.table_name,
            tbl.table_schema, tbl.table_name
        );

        RETURN QUERY EXECUTE dyn_sql;
    END LOOP;
END;
$$;

-- Get results:
-- SELECT * FROM util_report_column_value_counts('public') ORDER BY table_schema, table_name, ordinal_position;
-- Persist Results:
-- CREATE SCHEMA IF NOT EXISTS analytics; CREATE MATERIALIZED VIEW analytics.column_value_counts AS SELECT * FROM util_report_column_value_counts('public');
