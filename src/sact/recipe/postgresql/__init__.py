import zc.buildout
import logging
import subprocess
import os
import time
import textwrap

import psycopg2
import hexagonit.recipe.cmmi
from Cheetah.Template import Template

current_dir = os.path.dirname(__file__)

class Recipe:
    """zc.buildout recipe for Postgresql"""

    def __init__(self, buildout, name, options):
        self.options = options
        self.buildout = buildout
        self.name = name
        self.log = logging.getLogger(self.name)

        self.options['admin'] = options.get("admin", "postgres")
        self.options['superusers'] = options.get("superusers", "root")
        self.options['users'] = options.get("users", "")

        self.options['location'] = os.path.join(buildout['buildout']['parts-directory'], self.name)
        self.options['url-bin'] = options.get("url-bin", "")
        self.options['bin_dir'] = options.get("bin-dir", os.path.join(self.options['location'], "bin"))


        # Options specific to the PostgreSQL location
        default_config_dir = os.path.join(self.options['location'], "db")

        self.options['data_dir'] = options.get("config-dir", default_config_dir)
        self.options['pid_file'] = options.get(
            "pid-file", os.path.join(default_config_dir, "postgresql.pid"))
        self.options['socket_dir'] = options.get("pid-file", default_config_dir)
        self.options['listen_addresses'] = options.get('listen_addresses', '')
        self.options['unix_socket_directory'] = options.get('unix_socket_directory', self.options['location'])
        self.options['port'] = options.get('port', '5432')
        self.options['postgresql.conf'] = options.get('postgresql.conf', '')


    def install(self):
        if self.options['url-bin']:
            self._install_compiled_pg()
        else:
            self._install_cmmi_pg()
            self.log.info('Create database')
            self._make_db()

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
        self._make_pg_config()

    def _install_cmmi_pg(self):
        try:
            self.log.info('Compiling PostgreSQL')
            opt = self.options.copy() # Mutable object, updated by hexagonit
            cmmi = hexagonit.recipe.cmmi.Recipe(self.buildout, self.name, opt)
            cmmi.install()
        except:
            raise zc.buildout.UserError("Unable to install source version of postgresql")

    def _install_compiled_pg(self):
        # Download the binaries using hexagonit.recipe.download

        try:
            opt = self.options.copy()
            opt['url'] = self.options['url-bin']
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
            os.path.join(self.buildout['buildout']['bin-directory'], 'psql'),
            '-h', self.options['socket_dir'],
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
            cmd = '%s/createuser -s -d -r -h %s -U %s %s' % (self.buildout['buildout']['bin-directory'],
                                                       self.options['socket_dir'],
                                                       self.options['admin'],
                                                       superuser)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

    def _create_users(self):
        users = self.options['users'].split()
        for user in users:
            self.log.info('create user %s' % user)
            cmd = '%s/createuser -S -D -R -h %s -U %s %s' % (self.buildout['buildout']['bin-directory'],
                                                 self.options['socket_dir'],
                                                 self.options['admin'],
                                                 user)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

    def _make_db(self):
        os.mkdir(self.options['data_dir'])
        cmd = '%s/initdb -D %s -U %s' % (self.options['bin_dir'], self.options['data_dir'], self.options['admin'])
        os.chdir(self.options['bin_dir'])
        os.system(cmd)

    def _make_pg_config(self):
        self.log.info("Updating PostgreSQL configuration")

        try:
            PG_VERSION = open(os.path.join(self.options['data_dir'],
                                           'PG_VERSION')).read()
        except IOError:
            PG_VERSION = None

        pg_tpl = Template(file=os.path.join(current_dir,'templates', 'postgresql.conf.tmpl'))
        pghba_tpl = Template(file=os.path.join(current_dir,'templates', 'pg_hba.conf.tmpl'))
        pgctl_tpl = Template(file=os.path.join(current_dir,'templates', 'pgctl.py.tmpl'))
        psql_tpl = Template(file=os.path.join(current_dir,'templates', 'psql.sh.tmpl'))
        createuser_tpl = Template(file=os.path.join(current_dir,'templates', 'createuser.sh.tmpl'))
        createdb_tpl = Template(file=os.path.join(current_dir,'templates', 'createdb.sh.tmpl'))

        # Minimal configuration file used to bootstrap the server. Will be
        # replaced with all default values soon after.
        pg_tpl.data_dir = self.options['data_dir']
        pg_tpl.config_dir = self.options['data_dir']
        pg_tpl.pid_file = self.options['pid_file']
        pg_tpl.socket_dir = self.options['socket_dir']
        pg_tpl.listen_addresses = self.options['listen_addresses']
        pg_tpl.unix_socket_directory = self.options['unix_socket_directory']
        pg_tpl.port = self.options['port']

        pghba_tpl.PG_VERSION = PG_VERSION
        pghba_tpl.superusers = self.options['superusers'].split()
        pghba_tpl.users = self.options['users'].split()
        pghba_tpl.admin = self.options['admin']

        pg_fd = open(os.path.join(self.options['data_dir'], "postgresql.conf"),'w')
        pghba_fd = open(os.path.join(self.options['data_dir'], "pg_hba.conf"),'w')

        target=os.path.join(self.buildout["buildout"]["bin-directory"])
        pgctl_fd = open(os.path.join(target, "pgctl"),'w')
        psql_fd = open(os.path.join(target, "psql"),'w')
        createuser_fd = open(os.path.join(target, "createuser"),'w')
        createdb_fd = open(os.path.join(target, "createdb"),'w')

        pgctl_tpl.bin_dir = self.options['bin_dir']
        pgctl_tpl.data_dir = self.options['socket_dir']
        psql_tpl.bin_dir = self.options['bin_dir']
        psql_tpl.socket_dir = self.options['socket_dir']
        createuser_tpl.bin_dir = self.options['bin_dir']
        createuser_tpl.socket_dir = self.options['socket_dir']
        createdb_tpl.bin_dir = self.options['bin_dir']
        createdb_tpl.socket_dir = self.options['socket_dir']

        print  >> pg_fd, pg_tpl
        print  >> pghba_fd, pghba_tpl
        print  >> pgctl_fd, pgctl_tpl
        print  >> psql_fd, psql_tpl
        print  >> createdb_fd, createdb_tpl
        print  >> createuser_fd, createuser_tpl

        os.chmod(os.path.join(target, "pgctl"), 0755)
        os.chmod(os.path.join(target, "psql"), 0755)
        os.chmod(os.path.join(target, "createuser"), 0755)
        os.chmod(os.path.join(target, "createdb"), 0755)

    def _update_pg_config(self):
        """Update the PostgreSQL configuration file with our settings.

        It reads default configuration values from the server itself, and then,
        rewrite a configuration file with those default values and our values
        just after.

        It needs a running database server in order to retrieve default
        values.
        """

        conn = psycopg2.connect(
            host=self.options['data_dir'],
            user=self.options['admin']
        )
        cursor = conn.cursor()
        # http://www.postgresql.org/docs/current/static/view-pg-settings.html
        cursor.execute("SELECT name, setting, category, short_desc FROM pg_settings WHERE"
                       " context != 'internal' ORDER BY name")

        self.log.info("Re-writting the PostgreSQL configuration file with default "
                   "values...")

        pg_fd = open(os.path.join(self.options['data_dir'], "postgresql.conf"), 'w')
        pg_fd.write("# Default configuration from PostgreSQL\n")

        old_category = None
        for opt, value, category, desc in cursor:

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




def uninstall_postgresql(name, options):
    """Shutdown PostgreSQL server before uninstalling it."""

    logger = logging.getLogger(name)

    cmd = [os.path.join(options['bin_dir'], 'pg_ctl'),
           '-D', options['socket_dir'],
           '-w',
           '-t', '1',
           '-m', 'immediate',
           'stop']

    if not os.path.exists(cmd[0]):
        logger.info("No PostgreSQL binaries, will not try to stop the server.")
    else:
        logger.info("Trying to stop PostgreSQL server...")

        try:
            subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT
                            ).wait()
        except OSError, e:
            # For some reason, it fails, continue anyway...
            logger.warning("Could not stop PostgreSQL server (%s), "
                           "uninstalling it anyway.", e)
            pass
