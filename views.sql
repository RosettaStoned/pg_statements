CREATE OR REPLACE VIEW pg_statements AS (
SELECT
    stm.datname AS database,
    stm.pid AS backend_pid,
    stm.state AS backend_state,
    stm.wait_event AS backend_waiting,
    stm.query_start AS query_start,
    stm.query AS query,
    locks.other_pid AS locker_backend_pid,
    locks.other_query_start AS locker_query_start,
    locks.other_query AS locker_query,
    locks.other_locktype AS lock_type,
    locks.other_mode AS lock_mode,
    locks.other_granted AS lock_granted
FROM pg_stat_activity stm
LEFT JOIN (
    SELECT
        waiting_stm.pid AS waiting_stm_pid,
        other.locktype             AS other_locktype,
        other_stm.query_start AS other_query_start,
        other_stm.query            AS other_query,
        other.mode                 AS other_mode,
        other.pid                  AS other_pid,
        other.GRANTED              AS other_granted
    FROM
        pg_catalog.pg_locks AS waiting
    JOIN
        pg_catalog.pg_stat_activity AS waiting_stm
        ON (
            waiting_stm.pid = waiting.pid
        )
    JOIN
        pg_catalog.pg_locks AS other
        ON (
            (
                waiting."database" = other."database"
            AND waiting.relation  = other.relation
            )
            OR waiting.transactionid = other.transactionid
        )
    JOIN
        pg_catalog.pg_stat_activity AS other_stm
        ON (
            other_stm.pid = other.pid
        )
    WHERE
        NOT waiting.GRANTED
    AND
        waiting.pid <> other.pid
) locks
    ON stm.pid = locks.waiting_stm_pid
    WHERE stm.state <> 'idle'
);

