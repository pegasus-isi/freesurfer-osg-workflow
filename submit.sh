#!/bin/bash

set -e

export WORK_DIR=$HOME/workflows
mkdir -p $WORK_DIR

export RUN_ID=freesurfer-`date +'%s'`

# generate the workflow
./workflow-generator.py "$@"

# plan and submit the  workflow
pegasus-plan \
    --conf pegasus.conf \
    --dir $WORK_DIR \
    --relative-dir $RUN_ID \
    --sites condorpool \
    --output-site local \
    --dax freesurfer-osg.xml \
    --cluster horizontal \
    --submit



