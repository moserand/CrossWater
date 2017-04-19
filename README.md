# CrossWater #

The CrossWater framework allows the modelling of micropollutants in a large river basin such as the Rhine. 
The framework calculates loads and concentrations at any selectable points within the basin. For this purpose 
the river basin is be subdivided into small scale subcatchments for which  the input data must be supplied. 
At the scale of the subcatchments, a substance transfer module ([iWaQua model](https://github.com/hontimar/iWaQa)) is applied to calculate 
the substance load released to the aquatic system. Subsequently two routing options are available, the first approach aggregates the loads
from the substance transfer model for the upstream areas of the selected outlets. The second 
option is setting up a system file for the [AQUASIM model](http://www.eawag.ch/de/abteilung/siam/software/) with the input from the substance transfer 
model.

### Installation 

Clone the repository and set the `PYTHONPATH`.

### Usage ###

* Import data into a HDF5 file to make it usable for further steps
* Calculation of loads and concentrations from catchments
* Output conversion for routing model
* Routing model

## Dependencies

* NumPy
* pandas
* PyTables

## Version

* v1.0.0 : [![DOI](https://zenodo.org/badge/88661468.svg)](https://zenodo.org/badge/latestdoi/88661468)

