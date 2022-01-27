<h1 align="center">emit-main </h1>

<span style="color:orange">NOTE - at this time the EMIT repositories are not supporting Pull Requests from members outside of the EMIT-SDS Team.  This is expected to change in March, and guidance on branches will be provided at that time. At the moment, see the **develop** branch for latest updates. </span>

This is the governing repository for the EMIT SDS which manages the throughput of data between level-specific repositories.  This repo includes the workflow manager, database manager, and file monitor code.

To execute a section of the SDS, use the emit_main/run_workflow.py script, and to update / modify individual PGEs see the specific pge within emit_main/workflow/*_task.py

The implemented workflow is based on dependencies, meaning that if you call a routine that requires output from a prior series of routines, everything will be executed to get to the specified point.

Example call to process ACQUISITION_ID thorugh to l2a surface reflectance:

```
python run_workflow.py -c config/dev_sds_config.json -a ACQUISITION_ID -p l2arefl
```
