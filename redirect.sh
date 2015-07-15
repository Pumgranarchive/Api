#!/bin/bash

iptables -t nat -A PREROUTING -i eth0 -p tcp --dport 8081 -j REDIRECT --to-port 80
