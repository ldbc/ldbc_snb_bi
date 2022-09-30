import duckdb
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--tool', type=str, help='Name of the SUT, e.g. PostgreSQL', required=True)
parser.add_argument('--timings_dir', type=str, help='Directory containing the timings.csv file', required=True)
parser.add_argument('--throughput_min_time', type=str, help='Minimum total execution time for throughput batches. Executions with a lower total throughput time are invalid.', default=3600)

args = parser.parse_args()
timings_dir = args.timings_dir
tool = args.tool
throughput_min_time = args.throughput_min_time

con = duckdb.connect("bi.duckdb")
con.execute(f"""
    CREATE OR REPLACE TABLE load_time(time float);
    CREATE OR REPLACE TABLE timings(tool string, sf float, day string, q string, parameters string, time float);
    CREATE OR REPLACE TABLE power_test_individual(qid int, q string, time float);

    COPY load_time FROM '{timings_dir}/load.csv'    (HEADER, DELIMITER '|');
    COPY timings   FROM '{timings_dir}/timings.csv' (HEADER, DELIMITER '|');

    INSERT INTO power_test_individual
        SELECT regexp_replace('0' || q, '\D','','g')::int AS qid, q, time
        FROM timings
        WHERE day = (SELECT min(day) FROM timings)
          AND q != 'reads';

    CREATE OR REPLACE TABLE power_test_stats AS
        SELECT
            qid,
            q,
            count(time) AS count,
            min(time) AS min_time,
            max(time) AS max_time,
            avg(time) AS avg_time,
            sum(time) AS total_time,
            percentile_disc(0.50) WITHIN GROUP (ORDER BY time) AS p50_time,
            percentile_disc(0.90) WITHIN GROUP (ORDER BY time) AS p90_time,
            percentile_disc(0.95) WITHIN GROUP (ORDER BY time) AS p95_time,
            percentile_disc(0.99) WITHIN GROUP (ORDER BY time) AS p99_time,
        FROM power_test_individual
        GROUP BY qid, q;

    CREATE OR REPLACE TABLE throughput_test AS
        SELECT *
        FROM timings
        WHERE q IN ('reads', 'writes');

    CREATE OR REPLACE TABLE throughput_batches AS
        SELECT count(day)/2 AS n_batches, sum(time) AS t_batches -- /2 is needed because writes+reads are counted separately 
        FROM throughput_test
        WHERE day <= (
            SELECT day
            FROM
            (
                SELECT DISTINCT day, sum(time) OVER (ORDER BY day) AS throughput_runtime_running_total
                FROM throughput_test
                WHERE q IN ('reads', 'writes')
            )
            WHERE throughput_runtime_running_total >= {throughput_min_time}
            LIMIT 1
        );

    CREATE OR REPLACE TABLE all_throughput_batches AS
        SELECT count(day)/2 AS n_batches, sum(time) As t_batches
        FROM throughput_test;

    CREATE OR REPLACE TABLE throughput_score AS
        SELECT
            CASE WHEN n_batches = 0
                THEN null
                ELSE (24 - (SELECT time/3600 FROM load_time)) * (n_batches / (t_batches/3600))
                END
              AS score
        FROM throughput_batches;

    -- order as t_load, w, followed by q_1, ..., q_20
    CREATE OR REPLACE TABLE results_table_sorted AS
        SELECT *
        FROM (
            SELECT -1 AS qid, NULL AS q, (SELECT printf('%.1f', time) FROM load_time) AS t
          UNION ALL
            SELECT 0 AS qid, NULL AS q, printf('%.1f', total_time) AS t
            FROM power_test_stats
            WHERE q = 'write'
          UNION ALL
            SELECT regexp_replace('0' || q, '\D','','g')::int AS qid, q, printf('%.1f', total_time) AS t
            FROM power_test_stats
            WHERE q != 'write'
          UNION ALL
            SELECT 21 AS qid, NULL AS q, CASE WHEN n_batches = 0 THEN 'n/a' ELSE n_batches END AS t
            FROM throughput_batches
          UNION ALL
            SELECT 22 AS qid, NULL AS q, CASE WHEN t_batches IS NULL THEN 'n/a' ELSE printf('%.1f', t_batches) END AS t
            FROM throughput_batches
          UNION ALL
            SELECT 23 AS qid, NULL AS q, CASE WHEN score IS NULL THEN 'n/a' ELSE printf('%.2f', score) END
            FROM throughput_score
        )
        ORDER BY qid, q;
    """)

con.execute("""SELECT sf FROM timings LIMIT 1;""");
sf = con.fetchone()[0];
sf_string = str(round(sf, 3))
print(f"SF: {sf_string}")

con.execute("""
    SELECT 3600 / ( exp(sum(ln(total_time::real)) * (1.0/count(total_time))) ) as power
    FROM power_test_stats;
    """)
p = con.fetchone()[0]
print(f"power score:    {p:.02f}")
print(f"power@SF score: {p*sf:.02f}")

con.execute("""
    SELECT time
    FROM timings
    WHERE day = (SELECT min(day) FROM timings)
      AND q = 'reads';
    """)
r = con.fetchone()[0];
print(f"power test read time: {r:.01f}")
print()

con.execute(f"""
    COPY 
        (SELECT string_agg(t, ' \n')
        FROM results_table_sorted)
    TO 'runtimes-{tool}-sf{sf_string}.tex' (HEADER false, QUOTE '');
    """)

con.execute(f"""
    COPY
        (SELECT
            q,
            count,
            printf('\\numprint{{%.3f}}', min_time) AS min,
            printf('\\numprint{{%.3f}}', max_time) AS max,
            printf('\\numprint{{%.3f}}', avg_time) AS mean,
            printf('\\numprint{{%.3f}}', p50_time) AS p50,
            printf('\\numprint{{%.3f}}', p90_time) AS p90,
            printf('\\numprint{{%.3f}}', p95_time) AS p95,
            printf('\\numprint{{%.3f}} \\\\', p99_time) AS 'p99 \\\\',
        FROM power_test_stats
        ORDER BY qid, q
        )
    TO 'stats-{tool}-sf{sf_string}.tex' (HEADER true, QUOTE '', DELIMITER ' & ');
    """)


con.execute("""
    SELECT * FROM throughput_batches
    """)
s = con.fetchone()
if s[0] == 0:
    print(f"throughput score: n/a (the throughput run was <{throughput_min_time}s)")
else:
    con.execute("""
        SELECT (24 - (SELECT time/3600 FROM load_time)) * (n_batches / (t_batches/3600)) AS throughput
        FROM throughput_batches;
        """)
    t = con.fetchone()[0];
    print(f"throughput score: {t:.02f}")
    print(f"throughput@SF score: {t*sf:.02f}")

con.execute("""
    SELECT * FROM all_throughput_batches
    """)
tb = con.fetchone()
print()
print(f"total throughput batches executed: {tb[0]} batches in {tb[1]:.1f}s")
