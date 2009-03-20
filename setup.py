from setuptools import setup, find_packages

def get_version(version):
    try:
        if "dev" in version:
            import os
            from mercurial import hg, ui
            repo = hg.repository(ui.ui(),os.path.abspath(os.curdir))
            return "%s-r%d" % (version, len(repo.changelog)-1)
        else:
            return version
    except:
        return version

version = '0.1'

setup(name='sact.recipe.postgresql',
      version=get_version(version),
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
      install_requires=[
        'setuptools',
        # -*- Extra requirements: -*-
        'hexagonit.recipe.cmmi',
	'Cheetah'
      ],
      tests_require=['zope.testing',
        # -*- Extra requirements: -*-
        'zc.buildout',
      ],
      extras_require={'test':[
        'zope.testing',
        # -*- Extra requirements: -*-
        'zc.buildout',
      ]},
      entry_points={
      'zc.buildout': ["default = sact.recipe.postgresql:Recipe"],
      },
      )
