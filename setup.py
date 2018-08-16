# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-astro',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Astronomy plugins for Intake',
    url='https://github.com/ContinuumIO/intake-astro',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    py_modules=['intake_astro'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False, )
