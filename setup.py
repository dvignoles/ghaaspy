from setuptools import setup, find_packages

setup(name='ghaaspy',
      version='0.1',
      description='Tools for accessing, transforming, and distributng GHAAS datasets',
      url='https://github.com/dvignoles/ghaaspy',
      author='Daniel Vignoles',
      author_email='dvignoles@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=['geoserver-rest', 'gdal',],
      python_requires='>=3.9.2',      
      entry_points = {
          'console_scripts': ['gpkg2postgis=ghaaspy.cmd.gpkg2postgis:main', 
          'postgis2geoserver=ghaaspy.cmd.postgis2geoserver:main',
          'postgis_pivot=ghaaspy.cmd.postgis_pivot:main',
          'rgis2mosaic=ghaaspy.cmd.rgis2mosaic:main'],
      },
      package_data={'': ['ghaas_*.txt']},
        )
