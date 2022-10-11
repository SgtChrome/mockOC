DOCKERFILE_OC=DockerfileOCInterlocking DOCKERFILE_RASTA=DockerfileRastaInterlocking EXP_NAME=Test docker compose up --build -d
docker exec -it sender_rasta ./interlocking