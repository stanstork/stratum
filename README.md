# Stratum

A high-performance, containerized data migration tool.

## Docker Usage

Stratum is designed to be run as a Docker container, providing a consistent and isolated environment for data migration tasks.

### 1. Build the Docker Image

From the project root, run the following command to build the Docker image:

```bash
docker build -t stratum-engine:latest .
```

### 2. Run a Migration Task

To perform a migration, run the container with configuration file mounted as a volume. The engine will execute the migration and then exit.

Note: Source and destination DBs should be in the same network as the engine.
If they are not, you can use the Docker host IP (e.g., 172.17.0.1 on Linux, or host.docker.internal on Mac/Windows) to access the databases.

```bash
docker run \
  --rm \
  -v "$(pwd)/data/configs/single_table.smql:/home/stratum/config.smql:ro" \
  -e "REPORT_CALLBACK_URL=http://your-backend-api/migrations/complete" \
  stratum-engine:latest \
  migrate --config /home/stratum/config.smql
```

### 3. Run the Engine in Detached Mode

For interactive tasks like validating connections or inspecting table metadata, you can run the engine container in a detached, long-running mode. This allows you to execute commands against the running container.

```bash
docker run -d \
  --name stratum-engine \
  --restart=always \
  --entrypoint tail \
  stratum-engine:latest \
  -f /dev/null

# Example: Execute a command inside the running container
docker exec stratum <your-command-here>
```

## Configuration
### Environment Variables

#### REPORT_CALLBACK_URL

Purpose: At the end of a migration, the engine sends a POST request to this URL with a final report containing the migration status and key metrics (e.g., records processed, bytes transferred).

Example:

```bash
export REPORT_CALLBACK_URL="http://your-backend-service:8080/api/migrations/report"
```

#### AUTH_TOKEN

Purpose: This token is used to authenticate requests sent to the REPORT_CALLBACK_URL.

Example:

```bash
export AUTH_TOKEN="your-secure-auth-token"