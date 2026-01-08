#!/bin/bash
set -euo pipefail

git submodule update --init --recursive third_party/prometheus-cpp

sudo apt install -y python3.10-venv

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt

mkdir -p build

python3 -m grpc_tools.protoc \
  -I proto \
  --python_out=build \
  --pyi_out=build \
  --grpc_python_out=build \
  proto/queue.proto

sed -i '7s/.*/from . import queue_pb2 as queue__pb2/' build/queue_pb2_grpc.py

curl -Lo ./kind "https://github.com/kubernetes-sigs/kind/releases/download/v0.30.0/kind-linux-amd64"
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

sudo apt update && sudo apt install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gpg \
  build-essential \
  cmake \
  pkg-config \
  git \
  libprotobuf-dev \
  protobuf-compiler \
  protobuf-compiler-grpc \
  libgrpc++-dev \
  libabsl-dev \
  libspdlog-dev \
  zlib1g-dev \
  libssl-dev \
  docker.io

sudo mkdir -p /etc/apt/keyrings

curl -fsSL "https://pkgs.k8s.io/core:/stable:/v1.30/deb/Release.key" \
  | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg

sudo chmod 644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg

echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] \
https://pkgs.k8s.io/core:/stable:/v1.30/deb/ /" \
  | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null

sudo chmod 644 /etc/apt/sources.list.d/kubernetes.list

curl -fsSL https://packages.buildkite.com/helm-linux/helm-debian/gpgkey \
  | gpg --dearmor \
  | sudo tee /usr/share/keyrings/helm.gpg > /dev/null

echo "deb [signed-by=/usr/share/keyrings/helm.gpg] \
https://packages.buildkite.com/helm-linux/helm-debian/any/ any main" \
  | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list > /dev/null

sudo apt update && sudo apt install -y \
  kubectl \
  helm
