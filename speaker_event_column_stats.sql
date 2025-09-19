-- public.speaker_event_column_stats source

CREATE OR REPLACE VIEW public.speaker_event_column_stats
AS WITH unpivot AS (
         SELECT s.speaker_event_name,
            kv.key AS column_name,
            kv.value AS val,
            jsonb_typeof(kv.value) AS val_type,
                CASE
                    WHEN jsonb_typeof(kv.value) = 'string'::text THEN btrim(regexp_replace(kv.value::text, '^"(.*)"$'::text, '\1'::text))
                    ELSE NULL::text
                END AS val_text
           FROM speaker s
             CROSS JOIN LATERAL jsonb_each(to_jsonb(s.*) - 'speaker_event_name'::text) kv(key, value)
        ), stats AS (
         SELECT unpivot.speaker_event_name,
            unpivot.column_name,
            count(*) AS total_rows,
            count(*) FILTER (WHERE unpivot.val_type = 'null'::text) AS null_count,
            count(*) FILTER (WHERE unpivot.val_type = 'string'::text AND COALESCE(unpivot.val_text, ''::text) = ''::text) AS empty_count,
            count(*) FILTER (WHERE unpivot.val_type <> 'null'::text AND NOT (unpivot.val_type = 'string'::text AND COALESCE(unpivot.val_text, ''::text) = ''::text)) AS non_empty_count
           FROM unpivot
          GROUP BY unpivot.speaker_event_name, unpivot.column_name
        ), top_values AS (
         SELECT unpivot.speaker_event_name,
            unpivot.column_name,
            COALESCE(unpivot.val_text, unpivot.val::text) AS value_text,
            count(*) AS cnt,
            row_number() OVER (PARTITION BY unpivot.speaker_event_name, unpivot.column_name ORDER BY (count(*)) DESC, (COALESCE(unpivot.val_text, unpivot.val::text))) AS rn
           FROM unpivot
          WHERE unpivot.val_type <> 'null'::text AND NOT (unpivot.val_type = 'string'::text AND COALESCE(unpivot.val_text, ''::text) = ''::text)
          GROUP BY unpivot.speaker_event_name, unpivot.column_name, (COALESCE(unpivot.val_text, unpivot.val::text))
        ), top_agg AS (
         SELECT top_values.speaker_event_name,
            top_values.column_name,
            jsonb_agg(jsonb_build_object('value', top_values.value_text, 'count', top_values.cnt) ORDER BY top_values.cnt DESC, top_values.value_text) FILTER (WHERE top_values.rn <= 5) AS top5_values
           FROM top_values
          WHERE top_values.rn <= 5
          GROUP BY top_values.speaker_event_name, top_values.column_name
        )
 SELECT st.speaker_event_name,
    st.column_name,
    st.total_rows,
    st.null_count,
    st.empty_count,
    st.non_empty_count,
    COALESCE(ta.top5_values, '[]'::jsonb) AS top5_values
   FROM stats st
     LEFT JOIN top_agg ta ON ta.speaker_event_name = st.speaker_event_name AND ta.column_name = st.column_name
  ORDER BY st.speaker_event_name, st.column_name;

-- Usage:
-- SELECT * FROM speaker_event_column_stats WHERE speaker_event_name = 'REPLACE_ME';
