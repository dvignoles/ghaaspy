from setuptools import setup, find_packages

setup(name='gpkgimport',
      version='0.1',
      description='geopackage import tools for ASRC GHAAS dataset',
      url='https://github.com/dvignoles/gpkgimport',
      author='Daniel Vignoles',
      author_email='dvignoles@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=['geoserver-rest', 'gdal',],
      python_requires='>=3.9.2',      
      entry_points = {
          'console_scripts': ['gpkg2postgis=gpkgimport.gpkg2postgis:main', 
          'postgis2geoserver=gpkgimport.postgis2geoserver:main',
          'postgis_pivot=gpkgimport.postgis_pivot:main'],
      },
      package_data={'': ['*.txt']},
        )
