#!/bin/bash

sudo service hadoop-hdfs-namenode stop
sudo rm -rf /var/lib/hadoop-hdfs/cache/*
sudo -u hdfs hdfs namenode -format
sudo service hadoop-hdfs-namenode start

echo "Waiting for HDFS to start up"
sleep 10

sudo -u hdfs hadoop fs -mkdir -p /user/vagrant
sudo -u hdfs hadoop fs -chown vagrant /user/vagrant

