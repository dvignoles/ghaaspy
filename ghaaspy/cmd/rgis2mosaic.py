"""Convert gbdc.gz rasters to directories of single band tiff files suitable for geoserver image mosaic layers. 
"""

from pathlib import Path
import subprocess as sp
import argparse
import re

from osgeo import gdal
from ..rgis.rgis2x import rgisdir2tiff


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('inputdir', type=Path,
                        help="terminal directory of rgis rasters (gdbc.gz)")
    args = parser.parse_args()

    rgisdir2tiff(args.inputdir.resolve())

if __name__ == '__main__':
    main()