from setuptools import setup, find_packages
setup(
    name='gcivcisls', version='1.3', packages=['src', 'src.models', 'src.utils'],
    package_data={'src.models': ['*.json']},
)
