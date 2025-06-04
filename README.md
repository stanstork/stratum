# stratum
Data Migration Tool

## Docker

### 1. Build the Docker image

From the project root run:

```bash
docker build -t stratum-engine:latest .
```

### 2. Run the image

Source and destination DBs should be in the same network as the engine.
If they are not, you can use the Docker host IP (e.g. 172.17.0.1) to access them.

```bash
docker run \
  --rm \
  -v "$(pwd)/data/configs/single_table.smql:/home/stratum/config.smql:ro" \
  stratum-engine:latest \
  migrate --config /home/stratum/config.smql
```
