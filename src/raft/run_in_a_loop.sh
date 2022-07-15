#!/usr/bin/env bash

while true; do
    if ! DEBUG=1 go test > log; then
        break
    fi
    (( COUNTER++ ))
    echo -en "\r\033[K$COUNTER"
    sleep 1
done
echo "The command output changed"
