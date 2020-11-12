from setuptools import setup, find_packages
setup(
    # refer to python doc for config:
    # https://docs.python.org/3/distutils/setupscript.html#distutils-additional-files
    name='gcivcisls', version='1.3', packages=['src', 'src.models', 'src.utils'],
    package_data={'src.models': ['*.json']},
)
