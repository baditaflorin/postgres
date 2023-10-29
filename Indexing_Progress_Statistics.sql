SELECT
    pid,
    datid,
    datname,
    relid,
    index_relid,
    command,
    phase,
    lockers_total,
    lockers_done,
    current_locker_pid,
    blocks_total,
    blocks_done,
    tuples_total,
    tuples_done,
    partitions_total,
    partitions_done,
    CASE 
        WHEN blocks_total = 0 THEN NULL
        ELSE (blocks_done * 100.0 / NULLIF(blocks_total, 0)) END AS blocks_percentage_done,
    CASE 
        WHEN tuples_total = 0 THEN NULL
        ELSE (tuples_done * 100.0 / NULLIF(tuples_total, 0)) END AS tuples_percentage_done,
    CASE 
        WHEN partitions_total = 0 THEN NULL
        ELSE (partitions_done * 100.0 / NULLIF(partitions_total, 0)) END AS partitions_percentage_done
FROM (
    SELECT
        pid,
        datid,
        datname,
        relid,
        index_relid,
        command,
        phase,
        lockers_total,
        lockers_done,
        current_locker_pid,
        blocks_total,
        blocks_done,
        tuples_total,
        tuples_done,
        partitions_total,
        partitions_done
    FROM pg_stat_progress_create_index
) AS progress_data;
