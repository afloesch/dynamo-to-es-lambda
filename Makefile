DIR=$(PWD)
PY_PKG=venv/lib/python3.8/site-packages

deps: 
	pip install -r requirements.txt
build: deps
	zip -g lambda.zip main.py
	cd ${PY_PKG} && zip -r9 ${DIR}/lambda.zip ./