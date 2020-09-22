# please run this script at the same directory from terminal
GCI_VCI_SERVERLESS_RELATIVE_PATH=../gci-vci-aws/gci-vci-serverless/src

# if any error occur, abort immediately
set -e

cp setup.py ${GCI_VCI_SERVERLESS_RELATIVE_PATH}/

. ./venv/bin/activate

pip install ${GCI_VCI_SERVERLESS_RELATIVE_PATH}

pip freeze | grep gcivcisls

rm ${GCI_VCI_SERVERLESS_RELATIVE_PATH}/setup.py