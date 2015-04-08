import zc.buildout
import logging
import subprocess
import os
import sys
import platform
import time
import textwrap

import hexagonit.recipe.cmmi
from tempita import Template

current_dir = os.path.dirname(__file__)


class Recipe:
    """zc.buildout recipe for Postgresql"""

    def __init__(self, buildout, name, options):
        self.options = options
        self.buildout = buildout
        self.name = name
        self.log = logging.getLogger(self.name)
        self.pgconf = {}

        self.options['location'] = os.path.join(buildout['buildout']['parts-directory'], self.name)
        self.options['bin-dir'] = os.path.join(self.options['location'], "bin")

        self.options['admin'] = options.get("admin", "postgres")
        self.options['superusers'] = options.get("superusers", "root")
        self.options['users'] = options.get("users", "")
        self.options['url'] = options.get("url", "")
        self.options['url-bin'] = options.get("url-bin", "")
        self.options['conf-dir'] = options.get("conf-dir", self.options['location'])
        self.options['postgresql.conf'] = options.get('postgresql.conf', "")
        self.options['verbose-conf'] = options.get('verbose-conf', "")

    def install(self):
        # Exit if PostgreSQL is already installed
        if not os.path.exists(self.options['location']):
            self.log.info('No Postgresql found')
            if self.options['url-bin']:
                self._install_compiled_pg()
            else:
                self._install_cmmi_pg()
        else:
            self.log.info('Postgresql already compiled')

        # # Parse the PG configuration from the recipe and extract socket and data folders
        self.pgconf = self._parse_pg_conf()
        self._make_pg_config()
        self._create_cluster()

        cmd = '%s -D %s -o "-c config_file=%s" start' % (os.path.join(self.options['bin-dir'], 'pg_ctl'),
                                                         self.datadir,
                                                         os.path.join(self.options['conf-dir'], 'postgresql.conf'))

        p_start = subprocess.Popen(cmd,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   shell=True)
        p_start.wait()

        self._wait_for_startup()

        self._create_superusers()
        self._create_users()

        if not self.options['verbose-conf'] == "":
            self._update_pg_config()

        cmd = '%s -D %s -o "-c config_file=%s" stop' % (os.path.join(self.options['bin-dir'], 'pg_ctl'),
                                                        self.datadir,
                                                        os.path.join(self.options['conf-dir'], 'postgresql.conf'))

        p_stop = subprocess.Popen(cmd,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT)
        p_stop.wait()

        return self.options['location']

    update = install

    def _parse_pg_conf(self):
        # Read the postgreSQL configuration from the Buildout recipe.
        # Extract data_directory and unix_socket_{directory|directories}

        parsed_conf = {}
        self.datadir = self.socketdir = None

        lines = self.options['postgresql.conf'].split('\n')
        for line in lines:
            if line != "":
                argument, content = line.split('=')
                parsed_conf[argument.strip()] = content.strip()

                if argument.strip() == 'data_directory':
                    self.datadir = content.strip().strip("'")

                if argument.strip() == 'unix_socket_directory' or argument.strip() == 'unix_socket_directories':
                    self.socketdir = content.strip().strip("'")

        if self.datadir is None:
            self.log.error('data_directory option in postgresql.conf not found')
            sys.exit(1)

        if self.datadir is None and self.socketdir is None:
            self.log.error('unix_socket_directory (PG < 9.0) or unix_socket_directories (PG 9+) option not found')
            sys.exit(1)

        return parsed_conf

    def _install_cmmi_pg(self):
        try:
            self.log.info('Compiling PostgreSQL')
            opt = self.options.copy()  # Mutable object, updated by hexagonit
            cmmi = hexagonit.recipe.cmmi.Recipe(self.buildout, self.name, opt)
            cmmi.install()
        except:
            raise zc.buildout.UserError("Unable to install source version of postgresql")

    def _install_compiled_pg(self):
        """Download the binaries using hexagonit.recipe.download"""

        try:
            opt = self.options.copy()
            opt['url'] = self.options['url-bin'] % {'arch': platform.machine()}
            self.log.info("Will download using %s", opt['url'])
            opt['destination'] = self.options['location']
            name = self.name + '-hexagonit.download'
            hexagonit.recipe.download.Recipe(self.buildout, name, opt).install()
        except:
            raise zc.buildout.UserError("Unable to download binaries version of postgresql")

    def _wait_for_startup(self, max_try=10, wait_time=0.5):
        """Wait for the database to start.

        It tries to connect to the database a certain number of time, waiting
        a lap of time before connecting again.

        As long as we do not receive an error code of 0, it means the database
        is still trying to launch itself. If the server is OK to start, we can
        still receive an error message while connecting, saying something like
        "Please wait while the database server is starting up..."
        """

        self.log.info("Wait for the database to startup...")
        cmd = [
            os.path.join(self.options['bin-dir'], 'psql'),
            '-h', self.socketdir,
            '-U', self.options['admin'],
            '-l'
        ]

        count = 0
        while count < max_try:
            proc = subprocess.Popen(cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
            proc.wait()
            if proc.returncode == 0:
                # Ok, we were able to connect, the server should have started
                break

            count += 1
            time.sleep(wait_time)

        else:
            # Stop the buildout, we have waited too much time and it means their
            # should be some kind of problem.
            raise zc.buildout.UserError("Unable to communicate with PostgreSQL:\n%s" %
                                        proc.stdout.read())

    def _create_superusers(self):
        superusers = self.options['superusers'].split()
        for superuser in superusers:
            self.log.info('create superuser %s' % superuser)
            cmd = '%s/createuser -s -d -r -h %s -U %s %s' % (self.options['bin-dir'],
                                                             self.socketdir,
                                                             self.options['admin'],
                                                             superuser)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

    def _create_users(self):
        users = self.options['users'].split()
        for user in users:
            self.log.info('create user %s' % user)
            cmd = '%s/createuser -S -D -R -h %s -U %s %s' % (self.options['bin-dir'],
                                                             self.socketdir,
                                                             self.options['admin'],
                                                             user)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

    def _create_cluster(self):
        """Create a new PostgreSQL cluster into the data directory."""

        if os.path.exists(self.datadir):
            self.log.warning("Cluster directory already exists, skipping "
                             "cluster initialization...")
            return

        self.log.info('Initializing a new PostgreSQL database cluster')
        os.mkdir(self.datadir)
        cmd = [
            os.path.join(self.options['bin-dir'], 'initdb'),
            '-D', self.datadir,
            '-U', self.options['admin']
        ]
        proc = subprocess.Popen(cmd)
        proc.wait()

    def _read_pg_version(self):
        try:
            version = open(os.path.join(self.datadir,
                                        'PG_VERSION')).read()
        except IOError:
            version = None

        return version

    def _make_pg_config(self):
        self.log.info("Creating initial PostgreSQL configuration")

        pg_version = self._read_pg_version()

        def template_data(template_name):
            file_name = os.path.join(current_dir, 'templates', template_name)
            return open(file_name).read()

        # Minimal configuration file used to bootstrap the server. Will be
        # replaced with all default values soon after.
        pg_fd = open(os.path.join(self.options['conf-dir'], "postgresql.conf"), 'w')
        pg_fd.write(self.options['postgresql.conf'])

        pghba_tpl = Template(template_data('pg_hba.conf.tmpl'))
        pghba_fd = open(os.path.join(self.options['conf-dir'], "pg_hba.conf"), 'w')
        pghba_fd.write(pghba_tpl.substitute(PG_VERSION=pg_version,
                                            superusers=self.options['superusers'].split(),
                                            users=self.options['users'].split(),
                                            admin=self.options['admin']
                                            ))

    def _update_pg_config(self):
        """Update the PostgreSQL configuration file with our settings.

        It reads default configuration values from the server itself, and then,
        rewrite a configuration file with those default values and our values
        just after.

        It needs a running database server in order to retrieve default
        values.
        """

        self.log.info("Updating PostgreSQL configuration")

        # http://www.postgresql.org/docs/current/static/view-pg-settings.html
        query = "SELECT name, setting, category, short_desc FROM pg_settings "\
                "WHERE context != 'internal' ORDER BY name;"

        cmd = [os.path.join(self.options['bin-dir'], 'psql'),
               '-h', self.socketdir,
               '-U', self.options['admin'],
               '--no-align', '--quiet', '--tuples-only', 'template1']

        p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        out, err = p.communicate(query)

        # This should return immediately, since communicate() close the process
        p.wait()

        if err != '':
            raise ValueError("Unable to get settings from PostgreSQL: %s" %
                             (err,))

        lines = [line.split('|') for line in out.strip().split('\n')]

        self.log.info("Re-writting the PostgreSQL configuration file with default "
                      "values...")

        pg_fd = open(os.path.join(self.options['conf-dir'], "postgresql.conf"), 'w')
        pg_fd.write("# Default configuration from PostgreSQL\n")

        old_category = None
        for opt, value, category, desc in lines:

            if category != old_category:
                header = "## %s ##" % category
                dashes = "#" * len(header)
                pg_fd.write("\n%s\n%s\n%s\n" % (dashes, header, dashes))
                old_category = category

            # Patch some values which are wrongly returned
            if opt == 'lc_messages' and value == '':
                value = 'C'

            desc = "\n# ".join(textwrap.wrap(desc, width=78))

            pg_fd.write("# %s\n%s = %r\n\n" % (desc, opt, value))

        self.log.info("Updating the PostgreSQL configuration with the settings "
                      "from buildout configuration file...")

        pg_fd.write("\n\n# Override default values here\n")
        pg_fd.write(self.options['postgresql.conf'])
        pg_fd.close()
