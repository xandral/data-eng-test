#!/bin/bash

mkdir -p raw_data

# Download the segments
wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-06/segments/1684441872672.1/warc/CC-MAIN-20240612140424-20240612170424-00000.warc.gz
wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-06/segments/1684441872672.1/warc/CC-MAIN-20240612140424-20240612170424-00001.warc.gz
wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-06/segments/1684441872672.1/warc/CC-MAIN-20240612140424-20240612170424-00002.warc.gz
