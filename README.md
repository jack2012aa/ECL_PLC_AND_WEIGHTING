# ECL_PLC_AND_WEIGHTING
> Read data from PLC and scales.

## PLCW
This is used to read data of scales from modbus gateways, either TCP or RTU. 40 records will be taken with a duration of 0.2s, continue with an 9s break, then next loop. 

Use `weight.bat` under Windows.

## To Do
* Put configurations into a config file.
* The influence of pushing of pigs is more severe than expect. Must analyze a new filter.