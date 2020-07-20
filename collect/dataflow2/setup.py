import setuptools
REQUIRED_PACKAGES = []

PACKAGE_NAME = 'pp-load-rawdata'
PACKAGE_VERSION = '0.0.1'

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Loading Raw Data from GCS to Bigquery',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    package_data= {},
)