#!/bin/sh

curl -X POST -d '{"name":"benchmark"}' 'http://localhost:8086/db?u=root&p=root'
curl -X POST -d '{"name":"reports"}' 'http://localhost:8086/db?u=root&p=root'
curl -X POST -d '{"name":"paul","password":"pass"}' 'http://localhost:8086/db/benchmark/users?u=root&p=root'
curl -X POST -d '{"name":"user","password":"pass"}' 'http://localhost:8086/db/reports/users?u=root&p=root'