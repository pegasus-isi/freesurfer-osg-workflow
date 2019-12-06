#!/usr/bin/env bash
# arguments
# $1 - subject name
# $2 - num of cores to use

set -e

SUBJECT=$1
CORES=$2
# make $@ hold only arguments to freesurfer
shift 2

START_DIR=$PWD

# osgvo-freesurfer environment
. /opt/setup.sh

# license file comes with the job
FS_LICENSE=`pwd`/license.txt

export SUBJECTS_DIR=`mktemp -d --tmpdir=$PWD`
echo "Will use SUBJECTS_DIR=$SUBJECTS_DIR"


cp ${SUBJECT}_recon2_*.tar.xz $SUBJECTS_DIR
cd $SUBJECTS_DIR
if [ -e "${SUBJECT}_recon2_lh_output.tar.xz" ]; then
    tar xvaf ${SUBJECT}_recon2_lh_output.tar.xz
    tar xvaf ${SUBJECT}_recon2_rh_output.tar.xz
    rm ${SUBJECT}_recon2_lh_output.tar.xz
    rm ${SUBJECT}_recon2_rh_output.tar.xz
elif [ -e "${SUBJECT}_recon2_output.tar.xz" ]; then
    tar xvaf ${SUBJECT}_recon2_output.tar.xz
    rm ${SUBJECT}_recon2_output.tar.xz
fi

recon-all                                                               \
        -s $SUBJECT                                                     \
        $@                                                              \
        -autorecon3                                                     \
        -openmp $CORES

cd $SUBJECTS_DIR
mv ${SUBJECT}/scripts/recon-all.log ${SUBJECT}/scripts/recon-all-step3.log
cat ${SUBJECT}/scripts/recon-all-step1.log ${SUBJECT}/scripts/recon-all-step2*.log ${SUBJECT}/scripts/recon-all-step3.log > ${SUBJECT}/scripts/recon-all.log
rm -f fsaverage lh.EC_average rh.EC_average
tar cjf $START_DIR/${SUBJECT}_output.tar.bz2 *
cp ${SUBJECT}/scripts/recon-all.log $START_DIR
cd $START_DIR

