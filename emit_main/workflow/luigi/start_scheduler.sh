#!/usr/bin/env bash

luigid --port 8082 --background --pidfile /var/run/luigid.pid --state-path /var/lib/luigid/state --logdir /store/shared/luigi/ops/log
