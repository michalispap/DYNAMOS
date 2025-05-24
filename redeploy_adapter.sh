#!/bin/bash

cd ruby || { echo "Failed to enter ruby directory. Are you in the DYNAMOS directory?"; exit 1; }

echo "Running make all..."
make all
if [ $? -ne 0 ]; then
  echo "make all failed. Is the Docker Engine running?"
  exit 1
fi

echo "Running uninstall_adapter..."
uninstall_adapter
if [ $? -ne 0 ]; then
  echo "uninstall_adapter failed"
  exit 1
fi

echo "Waiting for a minute just be sure..."
sleep 60

echo "Running deploy_adapter..."
deploy_adapter
if [ $? -ne 0 ]; then
  echo "deploy_adapter failed"
  exit 1
fi

echo "Success!"
