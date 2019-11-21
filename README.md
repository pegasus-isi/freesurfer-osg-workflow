# FreeSurfer workflow for Open Science Grid

This is a Pegasus workflow for running FreeSurfer on the Open Science Grid, and replaces the *fsurf* service. The workflow is set up to take a directory with `.mgz` files as input and process each subject in parallel, using multicore jobs where appropriate.

Please run the workflow from your [OSG Connect](https://osgconnect.net/) account. Anybody with a U.S. research affiliation can get access.

You will need your own license file. See the [FreeSurfer documentation](https://surfer.nmr.mgh.harvard.edu/fswiki/DownloadAndInstall#License) for details on how to obtain the license. Once you have it, name it `license.txt` and put it in the same directory as you are submitting the workflow from, as Pegasus will pick up the file and send it with the jobs.

## submit.sh usage

```
$ ./submit.sh --help
usage: workflow-generator.py [-h] --subject_dir SUBJECT_DIR
                             [--options OPTIONS] [--cores NUM_CORES]
                             [--skip-recon] [--single-job] [--serial-job]
                             [--hemi {rh,lh}] [--debug]

Generate a pegasus workflow

optional arguments:
  -h, --help            show this help message and exit
  --subject_dir SUBJECT_DIR
                        Directory with subject data files (mgz)
  --options OPTIONS     options to pass to Freesurfer commands
  --cores NUM_CORES     number of cores to use
  --skip-recon          Skip recon processing
  --single-job          Do all processing in a single job
  --serial-job          Do all processing as a serial workflow
  --hemi {rh,lh}        hemisphere to process (rh or lh)
  --debug               Enable debugging output
```

## Example

Check out this repository to your OSG Connect account $HOME directory. Put your `license.txt` in the top level directory and run:

```
$ 
./submit.sh --options="-cw256" --subject_dir /cvmfs/singularity.opensciencegrid.org/opensciencegrid/osgvo-neuroimaging:latest/opt/freesurfer-6.0.1/subjects
```

Once the workflow is running, you can check the status with `pegasus-status [wfdir]`. 

## Getting Help

Please contact [OSG Connect Support](https://osgconnect.net) for any questions.


