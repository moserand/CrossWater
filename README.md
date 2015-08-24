# Crosswater #

Calculation of substances loads from a large catchments.
Currently only working for the River Rhine. 

### Installation 

Currently, you need to clone the repository and set the `PYTHONPATH`.

### What das the model do? ###

* Import data into a HDF5 file to make it usable for further steps
* Calculation of concentrations from catchments (more than 18,000 for the River Rhine)
* Output conversion for routing model
* Routing model is forthcoming
* Version 0.1

## Dependencies

* NumPy
* pandas
* PyTables
* py.test for running tests