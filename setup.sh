# run with chmod +x setup.sh

#!/bin/bash
sudo yum update -y
sudo yum install python3 -y
python3 -m ensurepip --upgrade
pip3 install --upgrade pip
pip3 install -r requirements.txt
