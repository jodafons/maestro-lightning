
export VIRTUALENV_NAMESPACE='novacula-env'
export LOGURU_LEVEL="DEBUG"
export VIRTUALENV_PATH=$PWD/$VIRTUALENV_NAMESPACE

export ADM_PASSWORD=${MASTER_PASSWORD}

export DB_PATH=$STORAGE_PATH/db
export DB_HOST=10.1.1.51
export DB_PROTOCOL=postgresql
export DB_IMAGE=postgres
export DB_TYPE=postgres
export DB_PASSWORD=cluster123456789
export DB_DATABASE=app
export DB_USER=cluster
export DB_PORT=5433
#export DB_PG4_PORT=5433
export DB_STRING="$DB_PROTOCOL://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_DATABASE"



if [ -d "$VIRTUALENV_PATH" ]; then
    echo "$VIRTUALENV_PATH exists."
    source $VIRTUALENV_PATH/bin/activate
else
    virtualenv -p python ${VIRTUALENV_PATH}
    source $VIRTUALENV_PATH/bin/activate
    pip install -e .
fi