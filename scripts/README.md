# Scripts used in Pegasus workflow to process MRI data using Freesurfer

| Script              | Description | Arguments |
|---------------------|-------------|-----------|
| autorecon1.sh       | does the recon1 and recon2-volonly steps | subject name, path to mgz file, # of cores to use |
| autorecon2.sh       | does recon2 steps on a single hemisphere | subject name, hemisphere to process, # of cores |
| autorecon2-whole.sh | does recon2 steps on both hemispheres | subject name, # of cores |
| autorecon3.sh       | does recon3 steps | subject name,  # of cores| 
| autorecon-all.sh    | does all recon steps | subject name, path to mgz file, # of cores to use |  
