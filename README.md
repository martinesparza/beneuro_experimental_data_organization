![Tests](https://github.com/BeNeuroLab/beneuro_experimental_data_organization/actions/workflows/run_tests.yml/badge.svg)

This is a collection of functions for managing the experimental data recorded in the BeNeuro Lab, and a CLI tool called `bnd` for easy access to this functionality.

Features so far:
- validating raw experimental data on the computers doing the recording
- uploading experimental data to the RDS server
- downloading experimental data from the RDS server

Features on the way:
- checking and uploading automatically on a schedule
- running spike sorting
- converting processed data to NWB
- converting NWB to TrialData

# Setting up
## Installation
1. Create an empty conda environment in which you will install the tool. The environment only needs Python and pip (and poetry, but you can also install that with pip).
   You can do this with `mamba create -n bnd python pip poetry`.

   Alternatively you can install with the system Python, but that's not really recommended. If you have conda/mamba on the computer, just use that for a peace of mind.
1. Clone this repo
   
    `git clone https://github.com/BeNeuroLab/beneuro_experimental_data_organization.git`
1. Navigate into the folder you just downloaded (`beneuro_experimental_data_organization`)
1. Activate the environment you installed in.

     `conda activate bnd`
1. Install the package with

     `poetry install`
1. Test that the install worked with

     `poetry run pytest`

   Hopefully you'll see green on the bottom (some yellow is fine) meaning that all tests pass :)

## Configuring the local and remote data storage
The tool needs to know where the experimental data is stored locally and remotely.

(This part might be replaced by a `bnd init` in the near future.)

0. Mount the RDS server. (If you're able to access the data on it from the file browser, it's probably already mounted.)

1. Create a file called `.env` in the `beneuro_experimental_data_organization` folder and add the following content:
```
LOCAL_PATH = /path/to/the/root/of/the/experimental/data/storage/on/the/local/computer
REMOTE_PATH = /path/to/the/root/of/the/experimental/data/storage/where/you/mounted/RDS/to
```

These refer to the folders where you have `raw` and `processed` folders.

2. Run `bnd show-config` to check if the tool sees settings you just entered.


# Usage
## Help
- To see the available commands: `bnd --help`
- To see the help of a command (e.g. `rename-videos`): `bnd rename-videos --help`
## Data validation
- You can validate the structure of raw data for an individual session:
  - `bnd validate-session . <subject-name>` if you're in the session's directory
  - `bnd validate-session /absolute/path/to/session/folder <subject-name>` from anywhere
 
  This will give you an error if there is a problem with the file structure.

  The name of the subject is used for confirmation, but might be removed in the future if it's too annoying.

- or for all sessions of a subject:
  - `bnd validate-sessions <subject-name>`
 
  This will give you an overview which sessions look good and which ones have a problem.

By default behavioral, ephys, and video data are all checked. To control which kind of data you want to check:
- To exclude checking something: `--ignore-behavior`, `--ignore-ephys`, `--ignore-videos`
- To explicitly include something: `--check-behavior`, `--check-ephys`, `--check-videos`

Please note that running validation will only give you the first problem that pops up. Once you fixed that, run it again to see if there are others ;)
 
## Renaming the videos
The default naming Jarvis uses for the video folder and files doesn't match the convention we want to follow.

Files can be renamed with `bnd rename-videos . <subject-name>`.

Add `--verbose` to the end to see what files were renamed.

## Uploading the data
Once you're done recording a session, you can upload that session to the server with:
  
  `bnd upload-session . <subject-name>`

This should first validate the data, then copy it to the server, and complain if it's already there.

# Please file an issue if something doesn't work or is just annoying to use!
