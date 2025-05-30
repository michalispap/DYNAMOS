#!/bin/bash

cd ruby || { echo "Failed to enter ruby directory. Are you in the DYNAMOS directory?"; exit 1; }

echo "Running make all..."
make all
if [ $? -ne 0 ]; then
  echo "make all failed. Are you sure Docker Engine is running?"
  exit 1
fi

echo "Running uninstall_adapter..."
helm uninstall mbt-adapter
if [ $? -ne 0 ]; then
  echo "uninstall_adapter failed"
  exit 1
fi

echo "Waiting for a bit just be sure..."
sleep 40

echo "Running deploy_adapter..."
chart="${DYNAMOS_ROOT}/charts/mbt-adapter/values.yaml"
helm upgrade -i -f "${chart}" mbt-adapter ${DYNAMOS_ROOT}/charts/mbt-adapter
if [ $? -ne 0 ]; then
  echo "deploy_adapter failed"
  exit 1
fi

echo "Success!"
