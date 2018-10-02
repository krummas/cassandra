#!/usr/bin/env bash

CCIDIR=.circleci
PATCH_CONFIG=1

# The following line is needed by the CircleCI Local Build Tool (due to Docker interactivity)
exec < /dev/tty

test -f build.xml || { echo "Must be run in Cassandra base dir"; exit 1; }

cp ${CCIDIR}/config-2_1.yml ${CCIDIR}/config-pre.yml
if [ ${PATCH_CONFIG} ]; then
    patch ${CCIDIR}/config-pre.yml ${CCIDIR}/*.patch > /dev/null
fi

# Abort commit on circle CLI error
circleci config process ${CCIDIR}/config-pre.yml > ${CCIDIR}/config-new.yml || exit 2;
rm ${CCIDIR}/config-pre.yml

diff ${CCIDIR}/config.yml ${CCIDIR}/config-new.yml
if [ $? -ne 0 ]; then
    # Abort if there have been changes in the config that needs to be committed first
    echo "CircleCI config has been modified and needs to be committed first to become effective."
    mv ${CCIDIR}/config-new.yml ${CCIDIR}/config.yml
    exit 3
else
    # No changes to existing config
    rm ${CCIDIR}/config-new.yml
fi

