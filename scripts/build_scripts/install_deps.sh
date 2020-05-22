export DEBIAN_FRONTEND=noninteractive
apt update
yes | apt install -y cmake g++ libcurl4-openssl-dev rapidjson-dev
