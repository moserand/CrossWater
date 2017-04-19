# Crosswater #

The CrossWater model framework allows the modelling of micropollutants in a large river basin such as the Rhine. 
The framework calculates loads and concentrations at any selectable points within the basin. For this purpose 
the river basin is be subdivided into small scale subcatchments for which  the input data must be supplied. 
At the scale of the subcatchments, the substance transfer module (Honti et al. 2016) is applied. Two modelling 
options are available, the first is a straightforward approach of load aggregation where the output of 
the substance transfer model are aggregated for the upstream areas of the selected outlets. With the second 
option is setting up a system file for the AQUASIM model (Reichert 1994) with input from the substance transfer 
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
