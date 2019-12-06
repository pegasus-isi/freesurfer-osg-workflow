# FreeSurfer workflow for Open Science Grid

This is a Pegasus workflow for running FreeSurfer on the Open Science Grid, and replaces the *fsurf* service. The workflow is set up to take a directory with `.mgz` files as input and process each subject in parallel, using multicore jobs where appropriate.

Please run the workflow from your [OSG Connect](https://osgconnect.net/) account. Anybody with a U.S. research affiliation can get access.

You will need your own license file. See the [FreeSurfer documentation](https://surfer.nmr.mgh.harvard.edu/fswiki/DownloadAndInstall#License) for details on how to obtain the license. Once you have it, name it `license.txt` and put it in the same directory as you are submitting the workflow from, as Pegasus will pick up the file and send it with the jobs.

## submit.sh usage

```
$ ./submit.sh --help
usage: submit.sh [-h] --inputs-def INPUTS_DEF [--cores NUM_CORES]
                 [--skip-recon] [--single-job] [--serial-job]
                 [--hemi {rh,lh}] [--debug]

Generate a pegasus workflow

optional arguments:
  -h, --help            show this help message and exit
  --inputs-def INPUTS_DEF
                        yaml based description of inputs
  --cores NUM_CORES     number of cores to use
  --skip-recon          Skip recon processing
  --single-job          Do all processing in a single job
  --serial-job          Do all processing as a serial workflow
  --hemi {rh,lh}        hemisphere to process (rh or lh)
  --debug               Enable debugging output

```

## Specifying Inputs

What to process is specified in an YAML input file with the format:

```
samplename:
    input: /some/path/to/input-mgz-or-nii
    T2: /optional/path/to/T2
    autorecon-options: 
```

Only `samplename` and `input` is required. Multiple inputs can be provided.

An example is provided in the `example-run.yml` file:

```
THP0001:
    input: tests/sub-THP0001_ses-THP0001UCI1_run-01_T1w.nii.gz
    T2: tests/sub-THP0001_ses-THP0001UCI1_run-01_T2w.nii.gz
    autorecon-options: -cw256

sample-001:
    input: /cvmfs/singularity.opensciencegrid.org/opensciencegrid/osgvo-freesurfer:latest/opt/freesurfer-6.0.1/subjects/sample-001.mgz

```


## Submitting an Example Workflow

Check out this repository to your OSG Connect account $HOME directory. Put your `license.txt` in the top level directory and run:

```
$ ./submit.sh --inputs-def example-run.yml
```

The workflow will pick up the two samples as specified in the `example-run.yml` file, create a workflow and submit it. Once the workflow is running, you can check the status with `pegasus-status [wfdir]`. 

## Getting Help

Please contact [OSG Connect Support](https://osgconnect.net) for any questions.


