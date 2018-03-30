#!/usr/bin/env python

from distutils.core import setup

setup(name='server',
      version='1.0',
      description='Python Distribution Utilities',
      author='Scott Swindell',
      author_email='scottswindell@email.arizona.edu',
	  py_modules = ['imgtasks', ],
	  scripts = ['dataserver.py']
     )
