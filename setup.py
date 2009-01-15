# -*- coding: utf-8 -*-
import os

from setuptools import setup, find_packages

version = '0.1'

entry_point = 'sact.recipe.postgresql:Recipe'
entry_points = {"zc.buildout": ["default = %s" % entry_point]}

setup(name='sact.recipe.postgresql',
      version=version,
      description="ZC.buildout recipe to build Postgresql.",
      # Get more strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
        'Framework :: Buildout',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Zope Public License',
        ],
      keywords='buildout postgresql',
      author='SecurActive',
      author_email='dev@securactive.net',
      url='http://hg.securactive.lan/internal/sact.recipe.postgresql',
      license='ZPL',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['sact', 'sact.recipe'],
      include_package_data=True,
      package_data = {
        'sact.recipe.postgresql.templates': ['*.tmpl'],
      },
      zip_safe=False,
      install_requires=['setuptools',
                        'zc.buildout',
                        # -*- Extra requirements: -*-
                        'hexagonit.recipe.cmmi',
			'Cheetah'
                        ],
      entry_points = dict(console_scripts=[
          'upload = sact.recipe.postgresql.pload:main',
          'update-tree = zope.release.tree:main',
          ]),

      )
