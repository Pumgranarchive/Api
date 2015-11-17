#!/bin/bash

iptables -t nat -A PREROUTING -i eth0 -p tcp --dport http -j REDIRECT --to-port 8081
iptables -t nat -A PREROUTING -i eth0 -p tcp --dport https -j REDIRECT --to-port 4443
