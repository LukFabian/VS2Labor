#!/bin/bash
source ../../venv/bin/activate
python splitter.py input.txt &
python mapper.py 1 &
python mapper.py 2 &
python mapper.py 3 &
python reducer.py 1 &
python reducer.py 2