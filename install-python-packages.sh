#!/bin/bash

# Exit on any error
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "Starting installation of dependencies for the trading bot in Anaconda3 base environment..."

# Update and upgrade the system
echo "Updating and upgrading the system..."
sudo apt update && sudo apt upgrade -y

# Check if Anaconda3 is installed
if ! command -v conda &> /dev/null; then
    echo "${RED}Anaconda3 is not installed. Please install Anaconda3 and try again.${NC}"
    exit 1
else
    echo "${GREEN}Anaconda3 is installed: $(conda --version)${NC}"
fi

# Activate Anaconda3 base environment
echo "Activating Anaconda3 base environment..."
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate base
echo "${GREEN}Activated Anaconda3 base environment.${NC}"

# Check if pip3 is installed in the base environment
if ! command -v pip3 &> /dev/null; then
    echo "${RED}pip3 is not installed in the Anaconda3 base environment. Installing...${NC}"
    conda install pip -y
else
    echo "${GREEN}pip3 is already installed in base environment: $(pip3 --version)${NC}"
fi

# Upgrade pip3 in the Anaconda3 base environment
echo "Upgrading pip3 in Anaconda3 base environment..."
pip3 install --upgrade pip

# Install system dependencies for ta-lib
echo "Installing system dependencies for ta-lib..."
sudo apt install -y build-essential wget libatlas-base-dev

# Install ta-lib (version 0.6.4 from GitHub)
TA_LIB_VERSION="0.6.4"
TA_LIB_DIR="ta-lib"
if [ ! -d "$TA_LIB_DIR" ]; then
    echo "Downloading and installing ta-lib version $TA_LIB_VERSION..."
    wget https://github.com/TA-Lib/ta-lib/releases/download/v$TA_LIB_VERSION/ta-lib-$TA_LIB_VERSION-src.tar.gz
    tar -xzf ta-lib-$TA_LIB_VERSION-src.tar.gz
    mv ta-lib-$TA_LIB_VERSION $TA_LIB_DIR
    cd $TA_LIB_DIR
    ./configure --prefix=/usr
    make
    sudo make install
    cd ..
    rm ta-lib-$TA_LIB_VERSION-src.tar.gz
else
    echo "${GREEN}ta-lib directory already exists, skipping installation.${NC}"
fi

# Install required Python libraries using pip3 in Anaconda3 base environment
echo "Installing required Python libraries with pip3..."
pip3 install schedule
pip3 install pytz
pip3 install requests
pip3 install yfinance
pip3 install ta-lib
pip3 install pandas_market_calendars
pip3 install sqlalchemy
pip3 install ratelimit
pip3 install numpy
pip3 install pandas
pip3 install traceback

# Verify installations
echo "Verifying installed packages..."
pip3 list | grep -E "schedule|pytz|requests|yfinance|ta-lib|pandas-market-calendars|sqlalchemy|ratelimit|numpy|pandas"

# Check for environment variable setup (without modifying .bashrc)
echo "Checking for environment variable setup..."
if [ -z "$YOUR_SECRET_KEY" ] || [ -z "$CALLMEBOT_API_KEY" ] || [ -z "$CALLMEBOT_PHONE" ]; then
    echo "${RED}Warning: Some environment variables are not set.${NC}"
    echo "YOUR_SECRET_KEY is the API secret key from Public.com (obtain from https://public.com/developers or your account settings)."
    echo "CALLMEBOT_API_KEY and CALLMEBOT_PHONE are for CallMeBot WhatsApp alerts (obtain from https://www.callmebot.com)."
    echo "Please set these variables in your current session before running the script, e.g.:"
    echo "export YOUR_SECRET_KEY='your-public-com-api-secret-key'"
    echo "export CALLMEBOT_API_KEY='your-callmebot-api-key'"
    echo "export CALLMEBOT_PHONE='your-callmebot-phone'"
else
    echo "${GREEN}Environment variables are set.${NC}"
fi

# Deactivate Anaconda3 base environment
conda deactivate
echo "${GREEN}Deactivated Anaconda3 base environment.${NC}"

echo "${GREEN}All external packages and dependencies have been successfully installed in the Anaconda3 base environment!${NC}"
echo "To run the trading bot, activate the base environment with: conda activate base"
echo "Then execute: python your_script.py"
