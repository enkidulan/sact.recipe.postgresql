Overview
========

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

It makes use of `hexagonit.recipe.cmmi`_ and `hexagonit.recipe.download`_
behind the scenes, so all options for these recipes are also valid.

.. _zc.buildout: http://www.buildout.org
.. _PostgreSQL: http://www.postgresql.org
.. _hexagonit.recipe.cmmi: http://pypi.python.org/pypi/hexagonit.recipe.cmmi
.. _hexagonit.recipe.download: http://pypi.python.org/pypi/hexagonit.recipe.download

Supported options
=================

The recipe supports the following options:

admin
    Aministrator accounts to create. Defaults to ``postgres``.

superusers
    Super-users accounts to create. Defaults to ``root``.

users
    User accounts to create.

location
   Destination of Postgresql. Defaults to the buildout section name.

url
   Download URL for the target source version of Postgresql (required if
   url-bin is empty).

url-bin
   Download URL for the target binary version of Postgresql. This option is
   always used if it is set.

conf_dir
    Folder of configuration files (the folder must exist). Defaults to ${location}.

postgresql.conf
    Custom Postgresql configuration. Two options are required:
    - ``data_directory``
    - ``unix_socket_directories`` (``unix_socket_directory`` for older versions
      of PostgreSQL).

verbose-conf
    If ``true``, include defaults reported by postgres server into generated
    configuration file. Defaults to false.

Binary url
==========

The recipe can detect automatically your platform within *(arch)s* in the url.
The syntax must follow the Python convention (read the sys.platform documentation).
The goal is to use a CI tool on various platforms without create an buildout
file to each one.

The binary mode is useful when you use a CI tool: you can test quickly the new
code.

Examples
========

Simple example::

    [pg92]
    recipe = sact.recipe.postgresql
    url = http://ftp.postgresql.org/pub/source/v9.2.2/postgresql-9.2.2.tar.bz2
    conf-dir = /etc/postgresql/9.2
    postgresql.conf =
        data_directory = '/srv/postgresql/9.2/db'
        unix_socket_directories = '/var/run'

More options::

    [pg92]
    recipe = sact.recipe.postgresql
    url = http://ftp.postgresql.org/pub/source/v9.2.2/postgresql-9.2.2.tar.bz2
    configure-options =
        --without-readline
        --with-python
    make-options =
        -j10
    conf-dir = /etc/postgresql/9.2
    postgresql.conf =
        data_directory = '/srv/postgresql/9.2/db'
        unix_socket_directories = '/var/run'
        listen_addresses = ''
        fsync = off
        synchronous_commit = off
        full_page_writes = off
        wal_buffers = 1024kB
        wal_writer_delay = 5000ms


