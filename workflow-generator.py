#!/usr/bin/env python

import sys
import os
import argparse
import time
import re

from Pegasus.DAX3 import *


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SCRIPT_DIR = os.path.join(BASE_DIR, 'scripts')


def job(name, run_on_submit_node=False, cores=1, memory=1700, disk=10000):
    """
    Wrapper for a Pegasus job, also sets resource requirement profiles. Memory and
    disk units are in MBs.
    """

    job = Job(name)
    job.addProfile(Profile(Namespace.CONDOR, 'request_cpus', str(cores)))
    job.addProfile(Profile(Namespace.CONDOR, 'request_disk', str(disk)))

    # increase memory if the first attempt fails
    memory = 'ifthenelse(isundefined(DAGNodeRetry) || DAGNodeRetry == 0, %d, %d)' \
             %(memory, memory * 3)
    job.addProfile(Profile(Namespace.CONDOR, 'request_memory', memory))

    if run_on_submit_node:
        job.addProfile(Profile(Namespace.HINTS, 'execution.site', 'local'))

    return job


def add_license_file(dax, job):
    """
    Attach the required license file to a job
    """
    license_file = File("license.txt")
    license_file.addPFN(PFN("file://{0}".format(os.path.join(BASE_DIR, "license.txt")), "local"))
    if not dax.hasFile(license_file):
        dax.addFile(license_file)
    job.uses(license_file, Link.INPUT)


def create_single_job(dax, version, cores, subject_files, subject):
    """
    Create a workflow with a single job that runs entire freesurfer workflow

    :param dax: Pegasus ADAG
    :param version: string indicating version of Freesurfer to use
    :param cores: number of cores to use
    :param subject_files: list egasus File object pointing to the subject mri files
    :param subject: name of subject being processed
    :return: exit code (0 for success, 1 for failure)
    :return: True if errors occurred, False otherwise
    """
    errors = False
    full_recon_job = Job(name="autorecon-all.sh".format(subject))
    full_recon_job.addArguments(version, subject, str(cores))
    for subject_file in subject_files:
        full_recon_job.addArguments(subject_file)
        full_recon_job.uses(subject_file, link=Link.INPUT)
    output = File("{0}_output.tar.gz".format(subject))
    full_recon_job.uses(output, link=Link.OUTPUT, transfer=True)
    if version != '5.1.0':
        full_recon_job.addProfile(Profile(Namespace.CONDOR, "request_memory", "4G"))
        full_recon_job.addProfile(Profile(Namespace.CONDOR, "request_cpus", cores))
    dax.addJob(full_recon_job)
    return errors


def create_custom_job(dax, version, cores, subject_dir, subject, options):
    """
    Create a workflow with a single job that runs freesurfer workflow
    with custom options

    :param dax: Pegasus ADAG
    :param version: string indicating version of Freesurfer to use
    :param cores: number of cores to use
    :param subject_dir: pegasus File object pointing tarball containing
                         subject dir
    :param subject: name of subject being processed
    :param options: options to FreeSurfer
    :return: exit code (0 for success, 1 for failure)
    :return: False if errors occurred, True otherwise
    """
    custom_job = Job(name="freesurfer-process.sh".format(subject))
    custom_job.addArguments(version, subject, subject_dir, str(cores), options)
    custom_job.uses(subject_dir, link=Link.INPUT)
    output = File("{0}_output.tar.bz2".format(subject))
    custom_job.uses(output, link=Link.OUTPUT, transfer=True)
    logs = File("recon-all.log".format(subject))
    custom_job.uses(logs, link=Link.OUTPUT, transfer=True)

    custom_job.addProfile(Profile(Namespace.CONDOR, "request_memory", "4G"))
    custom_job.addProfile(Profile(Namespace.CONDOR, "request_cpus", cores))
    dax.addJob(custom_job)
    return True


def create_recon2_job(dax, version, cores, subject):
    """
    Set up jobs for the autorecon2 process for freesurfer

    :param dax: Pegasus ADAG
    :param version: string indicating version of Freesurfer to use
    :param cores: number of cores to use
    :param subject: name of subject being processed
    :return: True if errors occurred, the pegasus job otherwise
    """
    recon2_job = Job(name="autorecon2-whole.sh".format(subject))
    recon2_job.addArguments(version, subject, str(cores))
    output = File("{0}_recon1_output.tar.xz".format(subject))
    recon2_job.uses(output, link=Link.INPUT)
    output = File("{0}_recon2_output.tar.xz".format(subject))
    recon2_job.uses(output, link=Link.OUTPUT, transfer=True)
    recon2_job.addProfile(Profile(Namespace.CONDOR, "request_memory", "4G"))
    recon2_job.addProfile(Profile(Namespace.CONDOR, "request_cpus", cores))
    return recon2_job


def create_initial_job(dax, version, subject_file, subject, options=None):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param version: string indicating version of Freesurfer to use
    :param subject_file: list of pegasus File objects pointing to the subject mri files
    :param subject: name of subject being processed
    :param options: If not None, options to pass to FreeSurfer
    :return: True if errors occurred, False otherwise
    """
    autorecon1_job = job("autorecon1.sh", memory=3500)

    # autorecon1 doesn't get any benefit from more than one core
    autorecon1_job.addArguments(subject, subject_file, '1')
    if options:
        # need quotes to keep options together
        autorecon1_job.addArguments("'{0}'".format(options))
   
    # inputs
    add_license_file(dax, autorecon1_job)
    autorecon1_job.uses(subject_file, link=Link.INPUT)

    # outputs
    output = File("{0}_recon1_output.tar.xz".format(subject))
    autorecon1_job.uses(output, link=Link.OUTPUT, transfer=False)
    dax.addFile(output)

    return autorecon1_job


def create_hemi_job(dax, version, cores, hemisphere, subject, options=None):
    """
    Set up job for processing a given hemisphere

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param hemisphere: hemisphere to process (should be rh or lh)
    :param subject: name of subject being processed
    :param options: If not None, options to pass to FreeSurfer
    :return: True if errors occurred, False otherwise
    """
    if hemisphere not in ['rh', 'lh']:
        return True

    autorecon2_job = job("autorecon2.sh", cores=cores, memory=4000)
    autorecon2_job.addArguments(subject, hemisphere, str(cores))
    if options:
        # need quotes to keep options together
        autorecon2_job.addArguments("'{0}'".format(options))
    output = File("{0}_recon1_output.tar.xz".format(subject))
    autorecon2_job.uses(output, link=Link.INPUT)
    output = File("{0}_recon2_{1}_output.tar.xz".format(subject, hemisphere))
    autorecon2_job.uses(output, link=Link.OUTPUT, transfer=False)
    add_license_file(dax, autorecon2_job)
    return autorecon2_job


def create_final_job(dax, version, subject, serial_job=False, options=None):
    """
    Set up jobs for the autorecon3 process for freesurfer

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param subject: name of subject being processed
    :param serial_job: boolean indicating whether this is a serial workflow or not
    :param options: If not None, options to pass to FreeSurfer
    :return: True if errors occurred, False otherwise
    """

    autorecon3_job = job("autorecon3.sh", memory=4000)

    # only use one core on final job, more than 1 core doesn't help things
    autorecon3_job.addArguments(subject, '1')
    if options:
        # need quotes to keep options together
        autorecon3_job.addArguments("'{0}'".format(options))

    if serial_job:
        recon2_output = File("{0}_recon2_output.tar.xz".format(subject))
        autorecon3_job.uses(recon2_output, link=Link.INPUT)
    else:
        lh_output = File("{0}_recon2_lh_output.tar.xz".format(subject))
        autorecon3_job.uses(lh_output, link=Link.INPUT)
        rh_output = File("{0}_recon2_rh_output.tar.xz".format(subject))
        autorecon3_job.uses(rh_output, link=Link.INPUT)
    output = File("{0}_output.tar.bz2".format(subject))
    logs = File("recon-all.log".format(subject))
    autorecon3_job.uses(output, link=Link.OUTPUT, transfer=True)
    autorecon3_job.uses(logs, link=Link.OUTPUT, transfer=True)
    add_license_file(dax, autorecon3_job)
    return autorecon3_job


def create_serial_workflow(dax, version, cores, subject_file, subject,
                           skip_recon=False):
    """
    Create a workflow that processes MRI images using a serial workflow
    E.g. autorecon1 -> autorecon2 -> autorecon3

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param subject_file: pegasus File object pointing to the subject mri file
    :param subject: name of subject being processed
    :param skip_recon: True to skip initial recon1 step
    :return: True if errors occurred, False otherwise
    """
    # setup autorecon1 run
    if not skip_recon:
        initial_job = create_initial_job(dax, version, subject_file, subject)
        if initial_job is True:
            return True
        dax.addJob(initial_job)
    recon2_job = create_recon2_job(dax, version, cores, subject)
    if recon2_job is True:
        return True
    dax.addJob(recon2_job)
    dax.addDependency(Dependency(parent=initial_job, child=recon2_job))
    final_job = create_final_job(dax, version, subject, serial_job=True)
    if final_job is True:
        return True
    dax.addJob(final_job)
    dax.addDependency(Dependency(parent=recon2_job, child=final_job))
    return False


def create_single_workflow(dax, version, cores, subject_files, subject):
    """
    Create a workflow that processes MRI images using a single job

    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param subject_files: list of pegasus File object pointing to the
                          subject mri files
    :param subject: name of subject being processed
    :return: True if errors occurred, False otherwise
    """
    return create_single_job(dax, version, cores, subject_files, subject)


def create_diamond_workflow(dax, version, cores, subject_file, subject,
                            skip_recon=False, options=None):
    """
    Create a workflow that processes MRI images using a diamond workflow
    E.g. autorecon1 -->   autorecon2-lh --> autorecon3
                     \->  autorecon2-rh /
    :param dax: Pegasus ADAG
    :param version: String with the version of FreeSurfer to use
    :param cores: number of cores to use
    :param subject_file:pegasus File object pointing to the subject mri file
    :param subject: name of subject being processed
    :param skip_recon: True to skip initial recon1 step
    :param options: If not None, options to pass to FreeSurfer
    :return: False if errors occurred, True otherwise
    """
    # setup autorecon1 run
    if not skip_recon:
        initial_job = create_initial_job(dax, version, subject_file, subject, options=options)
        if not initial_job:
            return False
        dax.addJob(initial_job)
    recon2_rh_job = create_hemi_job(dax, version, cores, 'rh', subject, options=options)
    if not recon2_rh_job:
        return False
    dax.addJob(recon2_rh_job)
    dax.addDependency(Dependency(parent=initial_job, child=recon2_rh_job))
    recon2_lh_job = create_hemi_job(dax, version, cores, 'lh', subject, options=options)
    if not recon2_lh_job:
        return False
    dax.addJob(recon2_lh_job)
    dax.addDependency(Dependency(parent=initial_job, child=recon2_lh_job))
    final_job = create_final_job(dax, version, subject, options=options)
    if not final_job:
        return False
    dax.addJob(final_job)
    dax.addDependency(Dependency(parent=recon2_rh_job, child=final_job))
    dax.addDependency(Dependency(parent=recon2_lh_job, child=final_job))
    return True


def generate_dax():
    """
    Main function that parses arguments and generates the pegasus
    workflow

    :return: True if any errors occurred during DAX generaton
    """
    version = "6.0.1"
    errors = False
    parser = argparse.ArgumentParser(description="Generate a pegasus workflow")
    parser.add_argument('--subject_dir', dest='subject_dir', default=None,
                        required=True, help='Directory with subject data files (mgz)')
    parser.add_argument('--options', dest='options', default=None,
                        help='options to pass to Freesurfer commands')
    parser.add_argument('--cores', dest='num_cores', default=2, type=int,
                        help='number of cores to use')
    parser.add_argument('--skip-recon', dest='skip_recon',
                        action='store_true',
                        help='Skip recon processing')
    parser.add_argument('--single-job', dest='single_job', default=False,
                        action='store_true',
                        help='Do all processing in a single job')
    parser.add_argument('--serial-job', dest='serial_job', default=False,
                        action='store_true',
                        help='Do all processing as a serial workflow')
    parser.add_argument('--hemi', dest='hemisphere', default=None,
                        choices=['rh', 'lh'],
                        help='hemisphere to process (rh or lh)')
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    #args = parser.parse_args(sys.argv[1:])
    try:
        args = parser.parse_args()
    except SystemExit:
        sys.exit(1)

    dax = ADAG('freesurfer')

    # add our scripts as executables
    for entry in os.listdir(SCRIPT_DIR):
        if entry == "README.md":
            continue
        exe = Executable(name=entry, arch="x86_64", installed=False)
        exe.addPFN(PFN("file://{0}".format(os.path.join(SCRIPT_DIR, entry)), "local"))
        dax.addExecutable(exe)

    # setup data file locations
    subject_dir = args.subject_dir

    for fname in os.listdir(subject_dir):

        if not re.search('\.mgz$', fname):
            continue

        print('Found MGZ file: ' + fname)

        subject_file = os.path.join(subject_dir, fname)
        subject_file = os.path.abspath(subject_file)
        dax_subject_file = File(fname)
        dax_subject_file.addPFN(PFN("file://{0}".format(subject_file), "local"))
        dax.addFile(dax_subject_file)

        subject = re.sub('\.mgz$', '', fname)

        if args.single_job:
            errors &= create_single_workflow(dax,
                                             version,
                                             args.num_cores,
                                             dax_subject_file,
                                             subject)
        elif args.serial_job:
            errors &= create_serial_workflow(dax,
                                             version,
                                             args.num_cores,
                                             dax_subject_file,
                                             subject,
                                             args.skip_recon,
                                             args.options)
        else:
            errors &= create_diamond_workflow(dax,
                                              version,
                                              args.num_cores,
                                              dax_subject_file,
                                              subject,
                                              args.skip_recon,
                                              args.options)
    if not errors:  # no problems while generating DAX
        dax_name = "freesurfer-osg.xml"
        with open(dax_name, 'w') as f:
            dax.writeXML(f)
    return errors


if __name__ == '__main__':
    failed = generate_dax()
    sys.exit(int(failed))
