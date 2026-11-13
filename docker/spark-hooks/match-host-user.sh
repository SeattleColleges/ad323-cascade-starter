# Runs inside the Spark container at start-up, as root, before Jupyter starts.
# Jupyter then runs as the same user id that owns your repo (the mounted
# spark/ folder), so everything Spark writes to engine_out belongs to you, and
# your own scripts (spark/week07_duckdb.py) can write there too.
host_uid="$(stat -c %u /home/jovyan/work)"
if [ "${host_uid}" != "0" ]; then
    export NB_UID="${host_uid}"
fi
