-- public.speaker_event_column_stats source

CREATE OR REPLACE VIEW public.speaker_event_column_stats_wide AS
WITH unpivot AS (
  SELECT
    s.speaker_event_name,
    kv.key  AS column_name,
    kv.value AS val,
    jsonb_typeof(kv.value) AS val_type,
    CASE
      WHEN jsonb_typeof(kv.value) = 'string'
        THEN btrim(regexp_replace(kv.value::text,'^"(.*)"$','\1'))
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
ranked AS (
  SELECT
    speaker_event_name,
    column_name,
    -- clip to 100 chars for spreadsheet sanity
    LEFT(btrim(COALESCE(val_text, val::text)), 100) AS value_text_clip,
    -- keep the json type of the original cell
    val_type,
    COUNT(*) AS cnt,
    ROW_NUMBER() OVER (
      PARTITION BY speaker_event_name, column_name
      ORDER BY COUNT(*) DESC, LEFT(btrim(COALESCE(val_text, val::text)),100) ASC
    ) AS rn
  FROM unpivot
  WHERE val_type <> 'null'
    AND NOT (val_type='string' AND COALESCE(val_text,'')='')
  GROUP BY 1,2, LEFT(btrim(COALESCE(val_text, val::text)),100), val_type
)
SELECT
  st.speaker_event_name,
  st.column_name,
  st.total_rows,
  st.null_count,
  st.empty_count,
  st.non_empty_count,

  MAX(value_text_clip) FILTER (WHERE rn=1) AS top1_value,
  MAX(val_type)        FILTER (WHERE rn=1) AS top1_type,
  MAX(cnt)             FILTER (WHERE rn=1) AS top1_count,

  MAX(value_text_clip) FILTER (WHERE rn=2) AS top2_value,
  MAX(val_type)        FILTER (WHERE rn=2) AS top2_type,
  MAX(cnt)             FILTER (WHERE rn=2) AS top2_count,

  MAX(value_text_clip) FILTER (WHERE rn=3) AS top3_value,
  MAX(val_type)        FILTER (WHERE rn=3) AS top3_type,
  MAX(cnt)             FILTER (WHERE rn=3) AS top3_count,

  MAX(value_text_clip) FILTER (WHERE rn=4) AS top4_value,
  MAX(val_type)        FILTER (WHERE rn=4) AS top4_type,
  MAX(cnt)             FILTER (WHERE rn=4) AS top4_count,

  MAX(value_text_clip) FILTER (WHERE rn=5) AS top5_value,
  MAX(val_type)        FILTER (WHERE rn=5) AS top5_type,
  MAX(cnt)             FILTER (WHERE rn=5) AS top5_count

FROM stats st
LEFT JOIN ranked r
  ON r.speaker_event_name = st.speaker_event_name
 AND r.column_name        = st.column_name
GROUP BY
  st.speaker_event_name, st.column_name, st.total_rows, st.null_count, st.empty_count, st.non_empty_count
ORDER BY st.speaker_event_name, st.column_name;



-- Usage:
-- SELECT * FROM speaker_event_column_stats WHERE speaker_event_name = 'REPLACE_ME';
