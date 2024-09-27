# Usar uma imagem base oficial do Python
FROM python:3.9-slim

# Definir o diretório de trabalho no container
WORKDIR /app

# Copiar os arquivos de dependências e instalar as dependências
COPY requirements.txt ./

RUN pip install  -r requirements.txt

# Instalar curl usando apt-get, necessário atualizar e limpar o cache
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Copiar o restante dos arquivos do aplicativo para o container
COPY . .


# Comando para iniciar o aplicativo quando o container iniciar
CMD ["python","-u", "./example_storageIntegration.py"]

