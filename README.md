# arm-duplisearcher-sqlite
duplisearcher with SQLite DB

привязка папки с конфигурационным файлом:
https://stackoverflow.com/questions/30652299/having-docker-access-external-files


если запускать из папки проекта:
source="$(pwd)"/data - папка на сервере
target=/app/data - папка внутри контейнера

running script:
sudo docker run -p 4005:4002 -d --restart unless-stopped --mount type=bind,source="$(pwd)"/data,target=/app/data -it expert-support-bot-onemodule

