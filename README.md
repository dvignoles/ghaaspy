# ghaaspy

Catch-all toolbox for reading, transforming, and distributing ASRC GHAAS datasets. 
## Install

Easiest way is to use conda for your base environment to circumvent tricky gdal issues.

```
conda create -n gpkg python=3.9.2 gdal=3.2.1
conda activate gpkg
pip -m pip install git+https://github.com/dvignoles/ghaaspy
```