#!/bin/bash
#SBATCH --job-name=enrich_uid
#SBATCH --partition=ssd
#SBATCH --account=ssd
#SBATCH --qos=ssd
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32GB
#SBATCH --time=06:00:00
#SBATCH --array=0-4
#SBATCH --output=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/enrich_uid_%A_%a.out
#SBATCH --error=/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex/country_filtered_cdindex/slurm/enrich_uid_%A_%a.err

set -euo pipefail

# ---------- CONFIG ----------
: "${PYTHON:=/project/jevans/tip/disruption/code_wos_2023/cdindex-benchmark-env/bin/python}"

PROJECT_DIR="/project/jevans/tip/disruption/code_wos_2023/enhanced-fast-cdindex"
VERTICES_PARQUET="${PROJECT_DIR}/country_filtered_cdindex/data_cache/paper_years.parquet"
ID_MAP="${PROJECT_DIR}/country_filtered_cdindex/data_cache/id_map.parquet"

# earlier results (input shards) and enriched outputs
SCORES_GLOB="/project/jevans/tip/disruption/code_wos_2023/filtered_scores/scores_filtered*.csv.gz"
OUT_DIR="/project/jevans/tip/disruption/code_wos_2023/filtered_scores_remapped/scores_filtered_with_uid"

# overwrite existing enriched shards? 1=yes, 0=no
: "${FORCE:=0}"

# DuckDB threads (joins are light; 8–16 is plenty)
: "${DUCKDB_THREADS:=16}"

export OMP_NUM_THREADS=${DUCKDB_THREADS}
export MKL_NUM_THREADS=${DUCKDB_THREADS}
export OMP_PROC_BIND=close
export OMP_PLACES=cores

TASKS="${SLURM_ARRAY_TASK_COUNT:-1}"
TASK_ID="${SLURM_ARRAY_TASK_ID:-0}"

echo "== Enrich scores with UID =="
echo "Job: $SLURM_JOB_ID  Task: ${TASK_ID}/${TASKS}"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "PYTHON: ${PYTHON}"
echo "VERTICES_PARQUET: ${VERTICES_PARQUET}"
echo "ID_MAP: ${ID_MAP}"
echo "SCORES_GLOB: ${SCORES_GLOB}"
echo "OUT_DIR: ${OUT_DIR}"
echo "THREADS: ${DUCKDB_THREADS}"
echo

mkdir -p "${OUT_DIR}"
mkdir -p "$(dirname "${ID_MAP}")"

# ---------- 1) Build id_map once (task 0), others wait ----------
build_needed=0
if [[ ! -f "${ID_MAP}" ]]; then
  build_needed=1
elif [[ "${VERTICES_PARQUET}" -nt "${ID_MAP}" ]]; then
  echo "[warn] vertices parquet newer than id_map → will rebuild"
  build_needed=1
fi

if (( build_needed )); then
  if (( TASK_ID == 0 )); then
    echo "[map] Task 0 creating id_map from ${VERTICES_PARQUET}"
    "${PYTHON}" - <<PY
import duckdb, os
vp   = r"""${VERTICES_PARQUET}"""
out  = r"""${ID_MAP}"""
threads = int(os.environ.get("DUCKDB_THREADS","8"))

con = duckdb.connect()
con.execute(f"PRAGMA threads = {threads}")
con.execute(f"""
  COPY (
    SELECT UID, paper_id
    FROM parquet_scan('{vp}')
  ) TO '{out}'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);
""")
print("Wrote id_map:", out)
PY
  else
    echo "[map] Task ${TASK_ID} waiting for id_map to be created by task 0..."
  fi
fi

# All non-zero tasks spin-wait (and task 0 will pass instantly after writing)
if (( TASK_ID != 0 || build_needed )); then
  tries=0
  until [[ -f "${ID_MAP}" ]]; do
    ((tries++)) || true
    if (( tries % 30 == 1 )); then
      echo "  waiting for id_map... (try ${tries})"
    fi
    sleep 2
  done
  # sanity: ensure DuckDB can read it before continuing
  "${PYTHON}" - <<PY
import duckdb, sys, os
p = r"""${ID_MAP}"""
con = duckdb.connect()
try:
  con.execute(f"SELECT COUNT(*) FROM parquet_scan('{p}')").fetchone()
except Exception as e:
  print("id_map not readable yet:", e)
  sys.exit(2)
print("id_map is ready")
PY
fi

echo "[map] Using id_map: ${ID_MAP}"
echo

# ---------- 2) Expand shards and select this task's subset ----------
mapfile -t SHARDS < <(ls -1 ${SCORES_GLOB} 2>/dev/null | sort || true)
NUM_SHARDS=${#SHARDS[@]}
if (( NUM_SHARDS == 0 )); then
  echo "No shards match: ${SCORES_GLOB}"
  exit 0
fi
echo "Found ${NUM_SHARDS} shards; fanout ${TASKS}; this task handles i%${TASKS}==${TASK_ID}"
echo

# ---------- 3) Process subset ----------
processed=0 skipped=0 failed=0

for ((i=0; i<NUM_SHARDS; ++i)); do
  (( (i % TASKS) == TASK_ID )) || continue

  in="${SHARDS[$i]}"
  base="$(basename "${in}")"
  out="${OUT_DIR}/${base%.csv.gz}.with_uid.csv.gz"

  if [[ -f "${out}" && "${FORCE}" -ne 1 ]]; then
    echo "[skip] ${i}/${NUM_SHARDS} exists: ${out}"
    ((skipped++)) || true
    continue
  fi

  echo "[join] ${i}/${NUM_SHARDS}  $(date +%T)"
  echo "       in : ${in}"
  echo "       out: ${out}"

  if "${PYTHON}" - <<PY
import duckdb, os, sys
in_path  = r"""${in}"""
id_map   = r"""${ID_MAP}"""
out_path = r"""${out}"""
threads  = int(os.environ.get("DUCKDB_THREADS","8"))

con = duckdb.connect()
con.execute(f"PRAGMA threads = {threads}")

# Simple join using paper_id column that exists in the CSV files
sql = f"""
COPY (
  SELECT m.UID,
         r.paper_id,
         r.cd, r.cd_only_us, r.cd_excl_us,
         r.cd_only_eu, r.cd_excl_eu,
         r.cd_only_cn, r.cd_excl_cn
  FROM read_csv_auto(
    '{in_path}',
    delim='\\t',
    header=TRUE,
    nullstr='NaN'
  ) AS r
  JOIN parquet_scan('{id_map}') AS m
  USING (paper_id)
) TO '{out_path}'
(FORMAT CSV, COMPRESSION GZIP, DELIM '\\t', HEADER TRUE);
"""
con.execute(sql)
PY
  then
    echo "[ok]   wrote ${out}"
    ((processed++)) || true
  else
    echo "[ERR]  failed for ${in}" >&2
    ((failed++)) || true
  fi
done

echo
echo "== Summary =="
echo "Processed: ${processed}"
echo "Skipped:   ${skipped}"
echo "Failed:    ${failed}"
echo "Done: $(date)"
