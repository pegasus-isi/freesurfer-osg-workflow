#!/bin/bash

# arguments
# $1 - subject name
# $2 - subject file
# $3 - num of cores to use
# $... freesurfer args

set -e

SUBJECT=$1
SUBJECT_FILE=$2
CORES=$3
# make $@ hold only arguments to freesurfer
shift 3

START_DIR=$PWD

# osgvo-freesurfer environment
. /opt/setup.sh

# license file comes with the job
FS_LICENSE=`pwd`/license.txt

export SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
echo "Will use SUBJECTS_DIR=$SUBJECTS_DIR"

cp ${SUBJECT_FILE} $SUBJECTS_DIR/

################################################################# run all steps

recon-all                                                               \
        -all                                                            \
        -s $SUBJECT                                                     \
        -i $SUBJECT_FILE                                                \
        $@                                                              \
        -openmp $CORES

cd $SUBJECTS_DIR

cd $SUBJECTS_DIR
tar cjf $START_DIR/${SUBJECT}_output.tar.gz *
cp ${SUBJECT}/scripts/recon-all.log $START_DIR/${SUBJECT}_recon-all.log
cd $START_DIR

