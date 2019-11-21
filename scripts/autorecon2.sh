#!/usr/bin/env bash
# arguments
# $1 - subject name
# $2 - hemisphere to analyze
# $3 - num of cores to use

set -e

SUBJECT=$1
HEMI=$2
CORES=$3
# make $@ hold only arguments to freesurfer
shift 3

START_DIR=$PWD

# osgvo-neuroimaging environment
. /opt/setup.sh

# license file comes with the job
FS_LICENSE=`pwd`/license.txt

export SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
echo "Will use SUBJECTS_DIR=$SUBJECTS_DIR"

cp ${SUBJECT}_recon1_output.tar.xz $SUBJECTS_DIR
cd $SUBJECTS_DIR
tar xvaf ${SUBJECT}_recon1_output.tar.xz
rm ${SUBJECT}_recon1_output.tar.xz

recon-all                                                               \
        -s $SUBJECT                                                     \
        $@                                                              \
        -autorecon2-perhemi                                             \
        -hemi $HEMI                                                     \
        -openmp $CORES

cd $SUBJECTS_DIR
mv $SUBJECT/scripts/recon-all.log $SUBJECT/scripts/recon-all-step2-$HEMI.log
tar cJf $START_DIR/${SUBJECT}_recon2_${HEMI}_output.tar.xz *
cd $START_DIR


