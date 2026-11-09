

# postgrstools
This repository is for storing tools used to manipulate and run benchmarks against posgres databases
#### Database dispositions
There are two kinds of databases that will need to be worked upon
* Standard postgres databases - postgres as installed as standard
* X-Ray databases - postgres databases that were installed and loaded by the Nutanix X-Ray tool
* Cloud Databases

### X-Ray Databases
   X-Ray ships with a few loader scripts and some specific locations for datasets that can be imported

### Standard Postgres databases
These are typically installed on Linux in the standard locations
### Cloud databases
These are cloud services that may not provide a unix shell


## Benchmarks
Many of these tools are focussed on running benchmarks.  We mostly use pgbench and hammerDB
* pgbench
* HammerDB
##### pgbench conventions
For pgbench I wish to name the databases as "pgbench-sfNNN" where NNN is the scale factor of the database e.g. pgbench-sf100 is a pgbench database with scale-factor of 100.

### Running the tools
Many of these tools were designed to run on the host where the database runs.  I am moving away from that method - to one where the tools run on a controlling machine - either a MacOS laptop or a Linux jump server.  Should run on Linux or MacOS. 

The tools should take the hostname of the server running postgres as the final parameter.

We can assume that the python based tools can run remotely on a jumpbox and the bash script should run directly on the host where the DB is running
#### python tools
The python tools should use python 3 and use standard python libraries to connect to the server over the postgres port
#### initial setup for python tools
We will need a script (possibly bash but could also be python) that connects to the postgres host and manipulates the postgres configuration files so that remote connections are allowed.

### Types of tools
* show the postgres configuration
* start/stop the database
* set/unset/display the cgroups configurations of the database
* display running queries and performance statistics
* display storage space used by the DB
* show select postgres parameters
* count the rows of tables in the database and display them
* display whether postrgres is 
  * Installed
  * Running
*