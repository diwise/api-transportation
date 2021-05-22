# Introduction 

This service is responsible for storing road, and road segment information, and provide it to consumers via an API.

# Building and tagging

```bash
# Build and tag an image with Docker
docker build -f deployments/Dockerfile -t diwise/api-transportation:latest .

# Build and run using docker compose for local testing
docker compose -f deployments/docker-compose.yaml build
docker compose -f deployments/docker-compose.yaml up
```

# Request data from the service

```sh
# Get all roadsegments within a rectangle described by three GeoJSON positions in [lon,lat]-format:
curl http://localhost:8088/ngsi-ld/v1/entities?type=RoadSegment&georel=within&geometry=Polygon&coordinates=[[17.230700,62.430242],[17.444075,62.353557],[17.444075,62.353557]]

# Get all roadsegments within a distance (30 meters) from a [lon,lat] point:
curl http://localhost:8088/ngsi-ld/v1/entities?type=RoadSegment&georel=near;maxDistance==30&geometry=Point&coordinates=[17.342553,62.377022]
```
