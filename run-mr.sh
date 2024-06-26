MR_APP_FILE="${MR_APP_FILE:-mrapps/word_count.exs}"
WORKER_N="${WORKER_N:-5}"
FILES_GLOB="${FILES_GLOB:-texts/*}"

MR_COORD_NAME=coordinator@0.0.0.0
COOKIE=cookie

RUN_WORKER="MR_APP_FILE=${MR_APP_FILE} MR_TYPE=worker MR_COORD_NAME=${MR_COORD_NAME} elixir --name worker{}@0.0.0.0 --cookie ${COOKIE} --no-halt -S mix"

MR_TYPE=coordinator MR_FILES_GLOB=${FILES_GLOB} elixir --name ${MR_COORD_NAME} --cookie ${COOKIE} --no-halt -S mix \
    & parallel --lb "${RUN_WORKER}" ::: $(seq ${WORKER_N})

wait