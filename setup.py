from setuptools import setup, find_packages


setup(
    name='sact.recipe.postgresql',
    version="99+develop",
    description="zc.buildout recipe to build PostgreSQL.",
    long_description=open("docs/source/overview.rst").read() + \
                     open("docs/source/changelog.rst").read(),
    # Get more strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
      'Development Status :: 4 - Beta',
      'Framework :: Buildout :: Recipe',
      'Intended Audience :: Developers',
      'Topic :: Software Development :: Build Tools',
      'Topic :: Software Development :: Libraries :: Python Modules',
      'License :: OSI Approved :: BSD License',
      'Operating System :: OS Independent',
      ],
    keywords='buildout postgresql',
    author='SecurActive SA',
    author_email='opensource@securactive.net',
    url='http://github.com/securactive/sact.recipe.postgresql',
    license='BSD License',
    packages=find_packages('src'),
    package_dir={'':'src'},
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
        'jinja2',
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
    entry_points="""
    [zc.buildout]
    default = sact.recipe.postgresql:Recipe

    [zc.buildout.uninstall]
    default = sact.recipe.postgresql:uninstall_postgresql
    """,
    )
