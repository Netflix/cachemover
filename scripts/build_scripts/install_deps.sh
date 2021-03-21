export DEBIAN_FRONTEND=noninteractive
apt update
yes | apt install -y cmake g++ libcurl4-openssl-dev rapidjson-dev libssl-dev

# For the AWS SDK
yes | apt install uuid-dev zlib1g-dev libpulse-dev

CWD=$(pwd)
echo $CWD
cd extern/
git clone --branch yaml-cpp-0.6.3 https://github.com/jbeder/yaml-cpp.git

git clone --branch 1.7.332 https://github.com/aws/aws-sdk-cpp.git
mkdir -p aws-sdk-cpp/build
cd aws-sdk-cpp/build
cmake -DBUILD_ONLY="s3;sqs" -DBUILD_SHARED_LIBS=OFF ..
make
make install
cd $CWD
