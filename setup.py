from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
    name='ckanext-dfmp',
    version=version,
    description="DFMP extension",
    long_description='''
    ''',
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Sergey Motornyuk',
    author_email='sergey.motornyuk@linkdigital.com.au',
    url='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.dfmp'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'celery',
        'tweepy',
        'ckanapi',
        'requests==2.4.3'
    ],
    entry_points='''
        [ckan.plugins]
        dfmp=ckanext.dfmp.plugin:DFMPPlugin
        
        [paste.paster_command]
        initdb = ckanext.dfmp.command:InitDB

        [ckan.celery_task]
        tasks = ckanext.dfmp.celery_import:task_imports
    ''',
)
