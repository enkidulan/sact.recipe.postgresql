This package is a `zc.buildout`_ recipe which allows to install a custom version
of the `PostgreSQL`_ database server, locally to the package you are working on.

It can install the database server from:

    * source: this is handy if you want to quickly test your application with a
      new release of `PostgreSQL`_ or with a new development snapshot;

    * pre-compiled binary: if you already have a compiled version of PostgreSQL,
      you can also reuse it to speed up the buildout process.


The recipe will give you several tools in the ``bin/`` directory to control the
server. Thus, you will be able to start and stop it, launch a command line
utility on the server, and so on.

.. _zc.buildout: http://www.buildout.org
.. _PostgreSQL: http://www.postgresql.org


INSTALLATION
============

For installation issue please refer to INSTALL.


DEVELOPPEMENT
=============

You can test or use this code simply by launching::

  python bootstrap.py
  buildout

This will install all dependency needed by this code to start coding.


DOC
---

Complete documentation can be generated thanks to::

  bin/doc
