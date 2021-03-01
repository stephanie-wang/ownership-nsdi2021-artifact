#!/bin/bash

pid=$(pgrep ray::process | tail -n 1)
echo "Killing owner with PID $pid on $(hostname -i)"
kill -9 $pid
