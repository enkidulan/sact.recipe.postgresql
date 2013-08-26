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
        self.options['postgresql.conf'] = options.get('postgresql.conf', "")

    def install(self):
        self._parse_pg_conf()

        # Hardcoded parameters. Automatic discover would be better.
        if 'unix_socket_directory' in self.pgconf:
            self.options['socket-dir'] = self.pgconf['unix_socket_directory'].strip("'").strip('"')
        elif 'unix_socket_directories' in self.pgconf:
            self.options['socket-dir'] = self.pgconf['unix_socket_directories'].strip("'").strip('"')
        else:
            self.log.error('Cannot find the socket directory')
            sys.exit(1)

        self.options['data-dir'] = self.pgconf['data_directory'].strip("'").strip('"')

        if os.path.exists(self.options['location']):
            self.log.info('Postgresql detected, make nothing')
        else:
            if self.options['url-bin']:
                self._install_compiled_pg()
            else:
                self._install_cmmi_pg()

            self._create_cluster()
            self._make_pg_config()

            cmd = '%s/pgctl start' % self.buildout['buildout']['bin-directory']
            p_start = subprocess.Popen(cmd, shell=True)
            p_start.wait()

            self.wait_for_startup()

            self._create_superusers()
            self._create_users()

            self._update_pg_config()

            cmd = '%s/pgctl stop' % self.buildout['buildout']['bin-directory']
            p_stop = subprocess.Popen(cmd, shell=True)
            p_stop.wait()

        return self.options['location']

    def update(self):
        pass

    def _parse_pg_conf(self):
        parsed_conf = {}

        lines = self.options['postgresql.conf'].split('\n')

        for line in lines:
            if line != "":
                argument, content = line.split('=')
                parsed_conf[argument.strip()] = content.strip()

        self.pgconf = parsed_conf

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

    def wait_for_startup(self, max_try=10, wait_time=0.5):
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
            '-h', self.options['socket-dir'],
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
                                                             self.options['socket-dir'],
                                                             self.options['admin'],
                                                             superuser)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

    def _create_users(self):
        users = self.options['users'].split()
        for user in users:
            self.log.info('create user %s' % user)
            cmd = '%s/createuser -S -D -R -h %s -U %s %s' % (self.options['bin-dir'],
                                                             self.options['socket-dir'],
                                                             self.options['admin'],
                                                             user)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

    def _create_cluster(self):
        """Create a new PostgreSQL cluster into the data directory."""

        cluster_dir = self.options['data-dir']
        if os.path.exists(cluster_dir):
            self.log.warning("Cluster directory already exists, skipping "
                             "cluster initialization...")
            return

        self.log.info('Initializing a new PostgreSQL database cluster')
        os.mkdir(cluster_dir)
        cmd = [
            os.path.join(self.options['bin-dir'], 'initdb'),
            '-D', cluster_dir,
            '-U', self.options['admin']
        ]
        proc = subprocess.Popen(cmd)
        proc.wait()

    def _make_pg_config(self):
        self.log.info("Creating initial PostgreSQL configuration")

        try:
            PG_VERSION = open(os.path.join(self.options['data-dir'],
                                           'PG_VERSION')).read()
        except IOError:
            PG_VERSION = None

        def template_data(template_name):
            file_name = os.path.join(current_dir, 'templates', template_name)
            return open(file_name).read()

        # Minimal configuration file used to bootstrap the server. Will be
        # replaced with all default values soon after.
        pg_fd = open(os.path.join(self.options['data-dir'], "postgresql.conf"), 'w')
        pg_fd.write(self.options['postgresql.conf'])

        pghba_tpl = Template(template_data('pg_hba.conf.tmpl'))
        pghba_fd = open(os.path.join(self.options['data-dir'], "pg_hba.conf"), 'w')
        pghba_fd.write(pghba_tpl.substitute(PG_VERSION=PG_VERSION,
                                            superusers=self.options['superusers'].split(),
                                            users=self.options['users'].split(),
                                            admin=self.options['admin']
                                            ))

        # Scripts to be copied into the bin/ directory created by buildout
        buildout_bin_dir = os.path.join(self.buildout["buildout"]["bin-directory"])
        templates = [('pgctl.py.tmpl', 'pgctl'),
                     ('psql.sh.tmpl', 'psql'),
                     ('pgctl.py.tmpl', 'pgctl'),
                     ('createuser.sh.tmpl', 'createuser'),
                     ('createdb.sh.tmpl', 'createdb')]

        for template_name, output_name in templates:
            full_output_name = os.path.join(buildout_bin_dir, output_name)

            template = Template(template_data(template_name))
            output = open(full_output_name, 'w')

            output.write(
                template.substitute(bin_dir=self.options['bin-dir'],
                                    socket_dir=self.options['socket-dir'],
                                    data_dir=self.options['data-dir']))

            output.close()
            os.chmod(full_output_name, 0755)

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
               '-h', self.options['socket-dir'],
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

        pg_fd = open(os.path.join(self.options['data-dir'], "postgresql.conf"), 'w')
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
