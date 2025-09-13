#!/bin/bash

# Update and upgrade the system
sudo apt update && sudo apt upgrade -y

# Install the required external Python libraries using pip3
pip3 install schedule
pip3 install pytz
pip3 install requests
pip3 install yfinance
pip3 install talib
pip3 install pandas_market_calendars
pip3 install sqlalchemy
pip3 install ratelimit
pip3 install numpy
pip3 install pandas

echo "All external packages have been successfully installed!"
