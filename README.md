# emit-main

NOTE - at this time the EMIT repositories are not supporting Pull Requests from members outside of the EMIT-SDS Team.  This is expected to change in March, and guidance on branches will be provided at that time. At present, public migration of this repository is a work-in-progress, and not all features are yet fully up to date.  See the **develop** branch - set as default - for the latest code._

## Description

Welcome to the emit-main repository.  This is the governing repository for the EMIT SDS which manages the throughput of data between level-specific repositories.  This repo includes the workflow manager, database manager, and monitor code.

To execute a section of the SDS, use the emit_main/run_workflow.py script, and to update / modify individual PGEs see the specific pge within emit_main/workflow/*_task.py

The implemented workflow is based on dependencies, meaning that if you call a routine that requires output from a prior series of routines, everything will be executed to get to the specified point.

To understand how this repository is linked to the rest of the emit-sds repositories, please see the [repository guide](https://github.com/emit-sds/emit-main/wiki/Repository-Guide).

## Installation Instructions

Clone the repository:
```
git clone https://github.com/emit-sds/emit-main.git
```
Set up and activate the conda environment:
```
cd emit-main
conda env create -f environment.yml -n emit-main
conda activate emit-main
```
*Note that the conda environment contains dependencies for the entire EMIT SDS processing chain.*

Run pip install:
```
cd emit-main
pip install -e .
```
Clone the emit-utils repository:
```
git clone https://github.com/emit-sds/emit-utils.git
```
Run pip install:
```
cd emit-utils
pip install -e .
```

## Dependency Requirements

This repository is based on Python 3.x.  See `emit-main/setup.py` for specific dependencies.

## Example Execution Commands

Example call to process ACQUISITION_ID thorugh to l2a surface reflectance:

```
python run_workflow.py -c config/dev_sds_config.json -a ACQUISITION_ID -p l2arefl
```
Where:
* ACQUISITION_ID: The acquisition identifier in the format "emitYYYYMMDDtHHMMSS"
