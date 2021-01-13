#!/bin/bash
bin/avalanche --remote-writers-slice 0 --remote-writers-count 1 --metric-count 10 --series-count 10 --value-interval 10 --series-interval 3600 --metric-interval 360000 --remote-requests-count 1000 --remote-url https://beacon.prom-beacon.dev.draios.com/api/prometheus/write --remote-api-token 5c564ce1-7eb9-48ba-a58f-8d59dd95c3d3
