# Definição do ambiente virtual
VENV_NAME=energia-env
PYTHON_BIN=python3
PYTHON=$(VENV_NAME)/bin/python
PIP=$(VENV_NAME)/bin/pip

# Cria o ambiente virtual Python
venv:
	$(PYTHON_BIN) -m venv $(VENV_NAME)

# Instala dependências
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

# Atualiza requirements.txt
freeze:
	$(PIP) freeze > requirements.txt

# Inicialização da API localmente
run-api:
	uvicorn app.main:app --reload --port 8001

# Remove arquivos temporários do Python
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete