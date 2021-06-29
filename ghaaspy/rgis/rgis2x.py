"""Conversion of rgis files to various"""

from .rgis import GHAASBIN, MOSAICS_ROOT, RGISRESULTS_NATIVE

def rgis2netcdf(inputpath:Path, outputpath:Path) -> None:


    cmd = "rgis2netcdf {} {}".format(inputpath, outputpath)
    sp.run(cmd.split())
    
def rgis2tiff(inputpath:Path, output_dir:Path=MOSAICS_ROOT) -> Path:

    # netcdf is intermediate product

    if output_dir is MOSAICS_ROOT:
        output_dir = copy_dirstruct(inputpath, MOSAICS_ROOT, RGISRESULTS_NATIVE)

    assert(output_dir.exists())

    netcdf_name = str(inputpath.name).split('.')[0] + '.nc'
    output_nc = Path('/tmp').joinpath(netcdf_name)
    rgis2netcdf(inputpath, output_nc)

    # save year for monthly data folder structure
    year = re.search(r'[0-9]{4}', netcdf_name).group()

    rast = gdal.Open(output_nc.__str__())
    band_count = rast.RasterCount

    if band_count > 1:
        output_dir = output_dir.joinpath(year)
        output_dir.mkdir()
        
        tiff_template = '{}{}.tiff'
        for b in range(1,band_count+1):
            tiff_name = tiff_template.format(output_nc.stem, str(b).zfill(2))
            tiff_path = output_dir.joinpath(tiff_name)
            # extract single bands as tiffs
            gdal.Translate(tiff_path.__str__(), rast, options="-b {} -a_srs EPSG:4326".format(b))
            print(tiff_name)
    else:
        tiff_name = output_nc.stem + '.tiff'
        tiff_path = output_dir.joinpath(tiff_name)
        gdal.Translate(tiff_path.__str__(), rast)
        print(tiff_name)
    
    # delete netcdf
    output_nc.unlink()

    return output_nc

def rgisdir2tiff(inputpath:Path) -> Path:
    assert(inputpath.is_dir() and inputpath.exists())

    for child in inputpath.iterdir():
        rgis2tiff(child)
