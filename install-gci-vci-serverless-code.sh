# please run this script at the same directory from terminal

eval $(egrep -v '^#' .env | xargs)

if [ -z "${GCI_VCI_SERVERLESS_RELATIVE_PATH}"]; then
    echo "Environment variant GCI_VCI_SERVERLESS_RELATIVE_PATH is required. Please specofy it in .env"
    return
else
    echo "GCI_VCI_SERVERLESS_RELATIVE_PATH is ${GCI_VCI_SERVERLESS_RELATIVE_PATH}"
fi

# if any error occur, abort immediately
set -e

cp setup.py ${GCI_VCI_SERVERLESS_RELATIVE_PATH}/

. ./venv/bin/activate

pip install ${GCI_VCI_SERVERLESS_RELATIVE_PATH}

pip freeze | grep gcivcisls

rm ${GCI_VCI_SERVERLESS_RELATIVE_PATH}/setup.py