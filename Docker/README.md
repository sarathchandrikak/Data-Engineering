# Docker

1. **Cross-Platform Consistency**  
   - Runs on any operating system with Docker installed.  
   - Eliminates the “it works on my machine” problem by making applications behave the same everywhere.

2. **Simplified Application Management**  
   - Easy to start, stop, and manage apps with simple commands (`docker run`, `docker stop`, etc.).  
   - Complex app environments can be described in `Dockerfile` or `docker-compose.yml` and reproduced easily.

3. **Containers and Their Own Environment**  
   - Each container has its own filesystem, libraries, and tools.  
   - Containers share the host OS kernel but remain isolated from each other.  
   - Can be built from scratch or pulled from base images.
   - Container runs on a docker engine which has customised / pulled images.

4. **Docker Images**  
   - A **Docker image** is a read-only template used to create containers.  
   - Images can be built via a `Dockerfile` or pulled from registries like Docker Hub.  
   - Example: `python:3.11-slim` is a minimal Linux image with Python 3.11 installed.  
   - Images are versioned and reusable for consistent deployments.
