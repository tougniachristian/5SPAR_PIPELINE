# Utiliser une image Python légère
FROM python:3.10-slim

# Définir le répertoire de travail
WORKDIR /app

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier ton script d'ingestion
COPY ingest_mastodon.py .

# Commande par défaut
CMD ["python", "ingest_mastodon.py"]
