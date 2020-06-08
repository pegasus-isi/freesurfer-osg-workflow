#!/usr/bin/env python

import argparse
import os
import re
import string
import sys
import time
import yaml

from pprint import pprint
from Pegasus.DAX3 import *


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SCRIPT_DIR = os.path.join(BASE_DIR, 'scripts')

DEFAULT_VERSION = '7.0.0'

license_file = None
job_id = 0


def subs(s, t2=''):
    '''
    Substitue variables into command line arguments
    '''
    return string.Template(s).substitute(locals())


def job(name, id=None, run_on_submit_node=False, cores=1, memory=1700, disk=10000):
    """
    Wrapper for a Pegasus job, also sets resource requirement profiles. Memory and
    disk units are in MBs.
    """
    global job_id
    
    # ever increasing
    job_id += 1
    
    job = Job(name, id='{}_{:05d}'.format(id, job_id))
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
    global license_file
    if not license_file:
        license_file = File("license.txt")
        license_file.addPFN(PFN("file://{0}".format(os.path.join(BASE_DIR, "license.txt")), "local"))
        dax.addFile(license_file)
    job.uses(license_file, Link.INPUT)


def create_single_job(dax, sample, cores):
    """
    Create a workflow with a single job that runs entire freesurfer workflow

    :param dax: Pegasus ADAG
    :param sample: a sample dict
    :param cores: number of cores to use
    """

    full_recon_job = job("autorecon-all.sh",
                         id=sample['subject_name'],
                         cores=cores, memory=3500)

    full_recon_job.addArguments(sample['subject_name'], sample['input_lfn'], str(cores))
    full_recon_job.uses(sample['input_lfn'], link=Link.INPUT)

    t2 = ''
    if 'T2' in sample:
        full_recon_job.addArguments('-T2', sample['T2_lfn'])
        full_recon_job.uses(sample['T2_lfn'], link=Link.INPUT)
        t2 = sample['T2_lfn']

    if 'autorecon-options' in sample:
        full_recon_job.addArguments(subs(sample['autorecon-options'], t2=t2))

    add_license_file(dax, full_recon_job)

    # outputs
    output = File("{0}_output.tar.gz".format(sample['subject_name']))
    full_recon_job.uses(output, link=Link.OUTPUT, transfer=True)
    logs = File("{0}_recon-all.log".format(sample['subject_name']))
    full_recon_job.uses(logs, link=Link.OUTPUT, transfer=True)
    
    dax.addJob(full_recon_job)
    return full_recon_job


def create_initial_job(dax, sample, cores):
    """
    Set up jobs for the autorecon1 process for freesurfer

    :param dax: Pegasus ADAG
    :param sample: the sample dict
    :return: the generated job
    """
    autorecon1_job = job("autorecon1.sh", id=sample['subject_name'], cores=cores, memory=3500)

    autorecon1_job.addArguments(sample['subject_name'], sample['input_lfn'], str(cores))
    autorecon1_job.uses(sample['input_lfn'], link=Link.INPUT)

    t2 = ''
    if 'T2' in sample:
        autorecon1_job.addArguments('-T2', sample['T2_lfn'])
        autorecon1_job.uses(sample['T2_lfn'], link=Link.INPUT)
        t2 = sample['T2_lfn']

    if 'autorecon-options' in sample:
        autorecon1_job.addArguments(subs(sample['autorecon-options'], t2=t2))
   
    # inputs
    add_license_file(dax, autorecon1_job)

    # outputs
    output = File("{0}_recon1_output.tar.xz".format(sample['subject_name']))
    autorecon1_job.uses(output, link=Link.OUTPUT, transfer=False)
    dax.addFile(output)

    return autorecon1_job


def create_hemi_job(dax, sample, hemisphere, cores):
    """
    Set up job for processing a given hemisphere

    :param dax: Pegasus ADAG
    :param sample: the sample dict
    :param hemisphere: hemisphere to process (should be rh or lh)
    :param cores: number of cores to use
    :return: True if errors occurred, False otherwise
    """
    if hemisphere not in ['rh', 'lh']:
        return True

    autorecon2_job = job("autorecon2.sh",
                         id=sample['subject_name'] + '-' + hemisphere,
                         cores=cores, memory=4000)
    autorecon2_job.addArguments(sample['subject_name'], hemisphere, str(cores))

    #t2 = ''
    #if 'T2' in sample:
    #    autorecon2_job.addArguments('-T2', sample['T2_lfn'])
    #    autorecon2_job.uses(sample['T2_lfn'], link=Link.INPUT)
    #    t2 = sample['T2_lfn']

    if 'autorecon-options' in sample:
        autorecon2_job.addArguments(subs(sample['autorecon-options']))
        
    input = File("{0}_recon1_output.tar.xz".format(sample['subject_name']))
    autorecon2_job.uses(input, link=Link.INPUT)
    
    output = File("{0}_recon2_{1}_output.tar.xz".format(sample['subject_name'], hemisphere))
    autorecon2_job.uses(output, link=Link.OUTPUT, transfer=False)
    
    add_license_file(dax, autorecon2_job)
    
    return autorecon2_job


def create_final_job(dax, sample, cores):
    """
    Set up jobs for the autorecon3 process for freesurfer

    :param dax: Pegasus ADAG
    :param sanple: sample dict
    :return: True if errors occurred, False otherwise
    """

    autorecon3_job = job("autorecon3.sh", id=sample['subject_name'], cores=cores, memory=4000)

    # only use one core on final job, more than 1 core doesn't help things
    autorecon3_job.addArguments(sample['subject_name'], str(cores))

    #t2 = ''
    #if 'T2' in sample:
    #    autorecon3_job.addArguments('-T2', sample['T2_lfn'])
    #    autorecon3_job.uses(sample['T2_lfn'], link=Link.INPUT)
    #    t2 = sample['T2_lfn']

    if 'autorecon-options' in sample:
        autorecon3_job.addArguments(subs(sample['autorecon-options']))

    lh_output = File("{0}_recon2_lh_output.tar.xz".format(sample['subject_name']))
    autorecon3_job.uses(lh_output, link=Link.INPUT)
    rh_output = File("{0}_recon2_rh_output.tar.xz".format(sample['subject_name']))
    autorecon3_job.uses(rh_output, link=Link.INPUT)
    output = File("{0}_output.tar.bz2".format(sample['subject_name']))
    logs = File("{0}_recon-all.log".format(sample['subject_name']))
    autorecon3_job.uses(output, link=Link.OUTPUT, transfer=True)
    autorecon3_job.uses(logs, link=Link.OUTPUT, transfer=True)
    add_license_file(dax, autorecon3_job)
    return autorecon3_job


def create_single_workflow(dax, sample, cores):
    """
    Create a workflow that processes MRI images using a single job

    :param dax: Pegasus ADAG
    :param sample: the sample dict
    :param cores: number of cores to use
    :return: True if errors occurred, False otherwise
    """
    create_single_job(dax, sample, cores)
    return True


def create_diamond_workflow(dax, sample, cores, skip_recon=False):
    """
    Create a workflow that processes MRI images using a diamond workflow
    E.g. autorecon1 -->   autorecon2-lh --> autorecon3
                     \->  autorecon2-rh /
    :param dax: Pegasus ADAG
    :param sample: sample dict
    :param cores: number of cores to use
    :param skip_recon: True to skip initial recon1 step
    :param options: If not None, options to pass to FreeSurfer
    :return: False if errors occurred, True otherwise
    """
    # setup autorecon1 run
    if not skip_recon:
        initial_job = create_initial_job(dax, sample, cores)
        if not initial_job:
            return False
        dax.addJob(initial_job)
        
    recon2_rh_job = create_hemi_job(dax, sample, 'rh', cores)
    if not recon2_rh_job:
        return False
    dax.addJob(recon2_rh_job)
    dax.addDependency(Dependency(parent=initial_job, child=recon2_rh_job))

    recon2_lh_job = create_hemi_job(dax, sample, 'lh', cores)
    if not recon2_lh_job:
        return False
    dax.addJob(recon2_lh_job)
    dax.addDependency(Dependency(parent=initial_job, child=recon2_lh_job))

    final_job = create_final_job(dax, sample, cores)
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

    errors = False
    parser = argparse.ArgumentParser(description="Generate a pegasus workflow")
    parser.add_argument('--inputs-def', dest='inputs_def', default=None,
                        required=True, help='yaml based description of inputs')
    parser.add_argument('--cores', dest='num_cores', default=4, type=int,
                        help='number of cores to use')
    parser.add_argument('--skip-recon', dest='skip_recon',
                        action='store_true',
                        help='Skip recon processing')
    parser.add_argument('--single-job', dest='single_job', default=False,
                        action='store_true',
                        help='Do all processing in a single job per subject (recon-all)')
    parser.add_argument('--version', dest='version', default='7.0.0',
                        help='Freesurfer version to use. Valid choices: 6.0.0, 6.0.1, 7.0.0'+
                             ' Current default: ' + DEFAULT_VERSION)
    parser.add_argument('--debug', dest='debug', default=False,
                        action='store_true',
                        help='Enable debugging output')
    #args = parser.parse_args(sys.argv[1:])
    try:
        args = parser.parse_args()
    except SystemExit:
        sys.exit(1)
        
    # license file
    if not os.path.exists("license.txt"):
        print("Please put your own license.txt in this directory")
        sys.exit(1)
    
    # read the yaml and validate inputs
    inputs = {}
    try:
        f = open(args.inputs_def)
        inputs = yaml.load(f)
        f.close()
    except Exception as e:
        print(e)
        sys.exit(1)        
        
    pprint(inputs)
    
    dax = ADAG('freesurfer')

    # add our scripts as executables
    for entry in os.listdir(SCRIPT_DIR):
        if entry == "README.md":
            continue
        exe = Executable(name=entry, arch="x86_64", installed=False)
        exe.addPFN(PFN("file://{0}".format(os.path.join(SCRIPT_DIR, entry)), "local"))
        dax.addExecutable(exe)


    for sample_name, sample in inputs.items():
        
        # modify sample data so it holds everything we will need
        sample['subject_name'] = sample_name

        # input files (lfn has to be unique)
        sample['input_lfn'] =  sample['subject_name'] + '-' + os.path.basename(sample['input'])
        sample['input_pfn'] = os.path.abspath(sample['input'])
        f = File(sample['input_lfn'])
        f.addPFN(PFN("file://{0}".format(sample['input_pfn']), "local"))
        dax.addFile(f)
        if 'T2' in sample:
            sample['T2_lfn'] =  sample['subject_name'] + '-' + os.path.basename(sample['T2'])
            sample['T2_pfn'] = os.path.abspath(sample['T2'])
            f = File(sample['T2_lfn'])
            f.addPFN(PFN("file://{0}".format(sample['T2_pfn']), "local"))
            dax.addFile(f)
       
        if args.single_job:
            create_single_workflow(dax,
                                   sample,
                                   args.num_cores)
        else:
            create_diamond_workflow(dax,
                                    sample,
                                    args.num_cores,
                                    args.skip_recon)

    dax_name = "freesurfer-osg.xml"
    with open(dax_name, 'w') as f:
        dax.writeXML(f)

    os.environ['VERSION'] = args.version
    os.system('envsubst < sites.xml.template > sites.xml')


if __name__ == '__main__':
    generate_dax()
    sys.exit(0)
