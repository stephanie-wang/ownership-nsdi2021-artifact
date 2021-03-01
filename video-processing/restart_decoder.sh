#!/bin/bash

pid=$(pgrep ray::Decoder)
echo "Killing ray::Decoder with PID $pid on $(hostname -i)"
kill -9 $pid
