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

.. _zc.buildout: http://www.buildout.org
.. _PostgreSQL: http://www.postgresql.org

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

bin_dir
    Folder of binaries. Defaults to ${location}.

data_dir
    Folder of data. Defaults ${location}.

pid_file
    The pid file of postgresql. Defaults to
    ${location}/db/postgresql.pid

listen_adresses
    Listen adresses. Defaults to all.

unix_socket_directory
    Folder of the Unix socket. Defaults to ${location}

port
    The port number Postgresql listen to. Default to 5432.

postgresql.conf
    Custom Postgresql configuration.


Binary url
==========

The recipe can detect automatically your platform within *(arch)s* in the url.
The syntax must follow the Python convention (read the sys.platform documentation).
The goal is to use a CI tool on various platforms without create an buildout
file to each one.

