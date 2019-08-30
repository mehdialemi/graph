#!/usr/bin/env bash

ps aux | grep pattern | awk '{print $2 }' | xargs kill
