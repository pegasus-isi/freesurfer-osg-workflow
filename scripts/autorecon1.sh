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

############################################################ 1st stage - serial
recon-all                                                               \
        -s $SUBJECT                                                     \
        -i $SUBJECT_FILE                                                \
        $@                                                              \
        -autorecon1                                                     \
        -openmp $CORES

############################################################ 2nd stage - serial
recon-all                                                               \
        -s $SUBJECT                                                     \
        -autorecon2-volonly                                             \
        -openmp $CORES

cd ${SUBJECTS_DIR}
mv $SUBJECT/scripts/recon-all.log $SUBJECT/scripts/recon-all-step1.log
tar cJf ${START_DIR}/${SUBJECT}_recon1_output.tar.xz *
cd ${START_DIR}

