# gpkgimport

Python package with functions and scripts for importing geopackages to postgis and subsequently as geoserver layers. This functionality is specific to geopackages made by CUNY ASRC ESI, not for general purpose. 

## Install

Easiest way is to use conda for your base environment to circumvent tricky gdal issues.

```
conda create -n gpkg python=3.9.2 gdal=3.2.1
conda activate gpkg
pip -m pip install git+https://github.com/dvignoles/gpkgimport
```