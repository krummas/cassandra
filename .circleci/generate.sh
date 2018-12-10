#!/bin/sh

if [ -s config-2_1.yml ]; then
    circleci config process config-2_1.yml > config.yml.LOWRES
    patch -o config-2_1.yml.HIGHRES config-2_1.yml config-2_1.yml.high_res.patch
    circleci config process config-2_1.yml.HIGHRES > config.yml.HIGHRES
    rm config-2_1.yml.HIGHRES
else
    echo "Run this in cassandra/.circleci directory"
    exit -1
fi
