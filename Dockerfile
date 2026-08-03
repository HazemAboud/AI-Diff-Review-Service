# Single container: MySQL 8.4 + the FastAPI app.
#
#   docker pull hazemaboud/mysql:latest
#   docker build -t hazemaboud/diffrev-all:latest .
#   docker push hazemaboud/diffrev-all:latest
#
#   docker run -d --name diffrev-all -p 8000:8000 \
#     -e MYSQL_ROOT_PASSWORD=diffrev_root \
#     -e MYSQL_DATABASE=diffrev_db \
#     -e MYSQL_USER=diffrev \
#     -e MYSQL_PASSWORD=diffrev_pass \
#     -e API_BEARER_TOKEN=your-secret-token-here \
#     hazemaboud/diffrev-all:latest

FROM hazemaboud/mysql:latest

# Python runtime on the MySQL image's own distro (OL9), so glibc versions match.
RUN microdnf -y --setopt=install_weak_deps=0 install python3.12 python3.12-pip \
    && microdnf clean all \
    && rm -rf /var/cache/yum

WORKDIR /app
COPY requirements.txt ./
RUN python3.12 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY . .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/entrypoint.sh"]
