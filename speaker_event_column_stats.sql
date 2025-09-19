-- public.speaker_event_column_stats source

CREATE OR REPLACE VIEW public.speaker_event_column_stats AS
WITH unpivot AS (
  SELECT
    s.speaker_event_name,
    kv.key AS column_name,
    kv.value AS val,
    jsonb_typeof(kv.value) AS val_type,
    CASE
      WHEN jsonb_typeof(kv.value) = 'string'
        THEN btrim(regexp_replace(kv.value::text, '^"(.*)"$', '\1'))
      ELSE NULL
    END AS val_text
  FROM speaker s
  CROSS JOIN LATERAL jsonb_each(to_jsonb(s.*) - 'speaker_event_name') AS kv(key, value)
),
stats AS (
  SELECT
    speaker_event_name,
    column_name,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE val_type = 'null') AS null_count,
    COUNT(*) FILTER (WHERE val_type = 'string' AND COALESCE(val_text,'')='') AS empty_count,
    COUNT(*) FILTER (
      WHERE val_type <> 'null'
        AND NOT (val_type='string' AND COALESCE(val_text,'')='')
    ) AS non_empty_count
  FROM unpivot
  GROUP BY 1,2
),
top_values AS (
  SELECT
    speaker_event_name,
    column_name,
    -- Build a trimmed full text first, then clip to 100 chars
    LEFT(
      btrim(COALESCE(val_text, val::text)),
      100
    ) AS value_text_clip,
    COUNT(*) AS cnt,
    ROW_NUMBER() OVER (
      PARTITION BY speaker_event_name, column_name
      ORDER BY COUNT(*) DESC, LEFT(btrim(COALESCE(val_text, val::text)), 100) ASC
    ) AS rn
  FROM unpivot
  WHERE val_type <> 'null'
    AND NOT (val_type='string' AND COALESCE(val_text,'')='')
  GROUP BY 1,2, LEFT(btrim(COALESCE(val_text, val::text)), 100)
),
top_agg AS (
  SELECT
    speaker_event_name,
    column_name,
    jsonb_agg(
      jsonb_build_object('value', value_text_clip, 'count', cnt)
      ORDER BY cnt DESC, value_text_clip ASC
    ) FILTER (WHERE rn <= 5) AS top5_values
  FROM top_values
  WHERE rn <= 5
  GROUP BY 1,2
)
SELECT
  st.speaker_event_name,
  st.column_name,
  st.total_rows,
  st.null_count,
  st.empty_count,
  st.non_empty_count,
  COALESCE(ta.top5_values, '[]'::jsonb) AS top5_values
FROM stats st
LEFT JOIN top_agg ta
  ON ta.speaker_event_name = st.speaker_event_name
 AND ta.column_name = st.column_name
ORDER BY st.speaker_event_name, st.column_name;


-- Usage:
-- SELECT * FROM speaker_event_column_stats WHERE speaker_event_name = 'REPLACE_ME';
