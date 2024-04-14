MR_COORD_NAME=coordinator@0.0.0.0
COOKIE=cookie

RUN_WORKER="MR_APP_FILE=mrapps/grep.exs MR_TYPE=worker MR_COORD_NAME=${MR_COORD_NAME} elixir --name worker{}@0.0.0.0 --cookie ${COOKIE} --no-halt -S mix"

MR_TYPE=coordinator elixir --name ${MR_COORD_NAME} --cookie ${COOKIE} --no-halt -S mix \
    & parallel --lb "${RUN_WORKER}" ::: $(seq 5)

wait