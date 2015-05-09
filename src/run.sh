#!/usr/bin/env bash

# use forever to restart node with a sleep of 1 seconds if spinning

./node_modules/.bin/forever --spinSleepTime 1000 index.js