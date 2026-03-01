# mqtt-mqtt-mirror
Mirror topics multiple remote MQTT brokers to a local broker

see mirror.ini.example for details.
copy configured mirror.ini.example to mirror.ini

installation on linux:
after configuring mirror.ini run install_service.sh
start the service with
service mqtt-mirror start;journalctl -u mqtt-mirror -f

run locally:
pip install -r requirements.txt
python mirror.py


