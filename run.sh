#!/bin/bash

# Run main.py and bot.py together

# Run main.py in the background
python3 main.py &

# Run bot.py in the background
python3 bot.py &

# Wait for both to finish
wait