#!/bin/bash

echo "Downloading data..."
wget https://www.dropbox.com/s/y38wgxbpvydls28/20200702-dend-captone-data.zip -q --show-progress

echo "Unzip data..."
unzip -qqo 20200702-dend-captone-data.zip

echo "Clean up..."
rm 20200702-dend-captone-data.zip
