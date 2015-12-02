#!/bin/bash

source ~/.bashrc

LOG_FILE="api.log"

make test.byte >> $LOG_FILE 2>&1
