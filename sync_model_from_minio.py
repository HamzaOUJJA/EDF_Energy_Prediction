from minio import Minio
from pathlib import Path

# Connexion MinIO
client = Minio(
    "localhost:9000",
    access_key="edfadmin",
    secret_key="edfpassword123",
    secure=False
)

bucket_name = "models"
prefix = "rte/best/"
local_dir = Path("data/models/rte/best")

local_dir.mkdir(parents=True, exist_ok=True)

print("Téléchargement du modèle depuis MinIO...")

objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)

count = 0

for obj in objects:
    object_name = obj.object_name

    # exemple object_name = rte/best/metadata/part-00000
    relative_path = object_name.replace(prefix, "")
    local_path = local_dir / relative_path

    local_path.parent.mkdir(parents=True, exist_ok=True)

    client.fget_object(bucket_name, object_name, str(local_path))
    print(f"Téléchargé : {object_name} -> {local_path}")

    count += 1

print(f"\nTerminé. {count} fichiers téléchargés.")
print(f"Modèle disponible ici : {local_dir.resolve()}")