import duckdb
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--timings_dir', type=str, help='Directory containing the timings.csv file', required=True)
args = parser.parse_args()
timings_dir = args.timings_dir

con = duckdb.connect("bi.duckdb")
con.execute(f"""
    DROP TABLE IF EXISTS timings;
    DROP TABLE IF EXISTS load_time;
    DROP TABLE IF EXISTS power_test;
    DROP TABLE IF EXISTS throughput_test;
    DROP TABLE IF EXISTS throughput_score;
    DROP TABLE IF EXISTS throughput_batches;
    DROP TABLE IF EXISTS all_throughput_batches;
    DROP TABLE IF EXISTS results_table_sorted;

    CREATE TABLE load_time         (time float);
    CREATE TABLE timings           (tool string, sf float, day string, q string, parameters string, time float);
    CREATE TABLE power_test        (q string, total_time float);

    COPY load_time FROM '{timings_dir}/load.csv'    (HEADER, DELIMITER '|');
    COPY timings   FROM '{timings_dir}/timings.csv' (HEADER, DELIMITER '|');

    INSERT INTO power_test
        SELECT q, sum(time) AS total_time
        FROM timings
        WHERE day = (SELECT min(day) FROM timings)
          AND q != 'reads'
        GROUP BY q;

    CREATE TABLE throughput_test AS
        SELECT *
        FROM timings
        WHERE q IN ('reads', 'writes');

    CREATE TABLE throughput_batches AS
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
            WHERE throughput_runtime_running_total >= 3600
            LIMIT 1
        );

    CREATE TABLE all_throughput_batches AS
        SELECT count(day)/2 AS n_batches, sum(time) As t_batches
        FROM throughput_test;

    CREATE TABLE throughput_score AS
        SELECT
            CASE WHEN n_batches = 0
                THEN null
                ELSE (24 - (SELECT time/3600 FROM load_time)) * (n_batches / (t_batches/3600))
                END
              AS score
        FROM throughput_batches;

    -- order as t_load, w, followed by q_1, ..., q_20
    CREATE TABLE results_table_sorted AS
        SELECT *
        FROM (
            SELECT -1 AS qid, NULL AS q, (SELECT printf('%.1f', time) FROM load_time) AS t
          UNION ALL
            SELECT 0 AS qid, NULL AS q, printf('%.1f', total_time) AS t
            FROM power_test
            WHERE q = 'write'
          UNION ALL
            SELECT regexp_replace('0' || q, '\D','','g')::int AS qid, q, printf('%.1f', total_time) AS t
            FROM power_test
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
print(f"SF: {sf.rstrip('.0')}")

con.execute("""
    SELECT 3600 / ( exp(sum(ln(total_time::real)) * (1.0/count(total_time))) ) as power
    FROM power_test;
    """)
p = con.fetchone()[0]
print(f"power: {p:.02f}")
print(f"power@SF: {p*sf:.02f}")

con.execute("""
    SELECT time
    FROM timings
    WHERE day = (SELECT min(day) FROM timings)
      AND q = 'reads';
    """)
r = con.fetchone()[0];
print(f"power test read time: {r:.01f}")
print()

con.execute("""
    SELECT string_agg( t, ' \n') AS power_test
    FROM results_table_sorted;
    """)
s = con.fetchone();
print(f"""results (TeX code):
{s[0]}
""")

con.execute("""
    SELECT * FROM throughput_batches
    """)
s = con.fetchone()
if s[0] == 0:
    print(f"throughput score: n/a (throughput run was <1 hour)")
else:
    con.execute("""
        SELECT (24 - (SELECT time/3600 FROM load_time)) * (n_batches / (t_batches/3600)) AS throughput
        FROM throughput_batches;
        """)
    t = con.fetchone()[0];
    print(f"throughput: {t:.02f}")
    print(f"throughput@SF: {t*sf:.02f}")

con.execute("""
    SELECT * FROM all_throughput_batches
    """)
tb = con.fetchone()
print()
print(f"total throughput batches run: {tb[0]}, {tb[1]:.1f}s")
