#!/usr/bin/env bash
kill $(pgrep -f nginx)

rm -r /tmp/volume1/ /tmp/volume2/ /tmp/volume3/ /tmp/volume4/ /tmp/volume5/ /tmp/indexdb/
