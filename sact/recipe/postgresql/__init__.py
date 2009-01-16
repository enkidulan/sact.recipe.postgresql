import zc.buildout
import urlparse
import tempfile
import logging
import urllib
import shutil
import md5
import imp
import os

import hexagonit.recipe.cmmi
from Cheetah.Template import Template

current_dir = os.path.dirname(__file__)

class Recipe:
    """zc.buildout recipe for configure Postgresql"""

    def __init__(self, buildout, name, options):
        self.options = options
        self.buildout = buildout
        self.name = name
        self.log = logging.getLogger(self.name)
       
        self.options['location'] = os.path.join(buildout['buildout']['parts-directory'], self.name)
        self.options['bin_dir'] = options.get("bin-dir", os.path.join(self.options['location'], "bin"))
        self.options['data_dir'] = options.get("config-dir", os.path.join(self.options['location'], "db"))
        self.options['pid_file'] = options.get("pid-file", os.path.join(self.options['location'], "db", "postgresql.pid"))
        self.options['socket_dir'] = options.get("pid-file", os.path.join(self.options['location'], "db"))
        self.options['listen_addresses'] = options.get('listen_addresses', 'localhost')
        self.options['port'] = options.get('port', '5432')
        self.options['unix_socket_directory'] = options.get('unix_socket_directory', self.options['location'])
        self.options['ssl'] = options.get('ssl', 'off')
        self.options['shared_buffers'] = options.get('shared_buffers', '24MB')
        self.options['work_mem'] = options.get('work_mem', '1MB')
        self.options['temp_buffers'] = options.get('temp_buffers', '8MB')
        self.options['fsync'] = options.get('fsync', 'on')
        self.options['synchronous_commit'] = options.get('synchronous_commit', 'on')
        self.options['wal_sync_method'] = options.get('wal_sync_method', 'fsync')
        self.options['wal_buffers'] = options.get('wal_buffers', '64kB')
        self.options['client_min_messages'] = options.get('client_min_messages', 'notice')
        self.options['log_min_messages'] = options.get('log_min_messages', 'notice')
        self.options['log_error_verbosity'] = options.get('log_error_verbosity', 'default')
        self.options['log_min_error_statement'] = options.get('log_min_error_statement', 'error')
        self.options['log_min_duration_statement'] = options.get('log_min_duration_statement', '-1')
        self.options['silent_mode'] = options.get('silent_mode', 'off')
        self.options['log_line_prefix'] = options.get('log_line_prefix', '%t ')
        self.options['track_activities'] = options.get('track_activities', 'on')
        self.options['track_counts'] = options.get('track_counts', 'on')
        self.options['log_parser_stats'] = options.get('log_parser_stats', 'off')
        self.options['log_planner_stats'] = options.get('log_planner_stats', 'off')
        self.options['log_executor_stats'] = options.get('log_executor_stats', 'off')
        self.options['log_statement_stats'] = options.get('log_statement_stats', 'off')
        self.options['admin'] = options.get("admin", "postgres")
        self.options['superusers'] = options.get("superusers", "root")
        self.options['users'] = options.get("users", "")
        self.options['install'] = options.get("install", "yes")

    def install(self):
        if self.options['install'] == "yes":  
            self._install_cmmi_pg()
            self._make_db()
            self._make_pg_config()

            # FIXME: users / superusers not working
            #os.system('%s/pgctl.py start' % (self.options['bin_dir']))
            #self._create_superusers()        
            #self._create_users()
            #os.system('%s/pgctl.py stop' % (self.options['bin_dir']))       
        
        elif self.options['install'] == "first":
            if self.is_first_install():
                self._install_cmmi_pg()
                self._make_db()
                self._make_pg_config()

            else:
                self._make_pg_config()
                
        elif self.options['install'] == "no":
            self._make_pg_config()
    
    def is_first_install(self):
        installed_file = self.buildout['buildout']['installed']
        if os.path.exists(installed_file):
            import ConfigParser
            config = ConfigParser.ConfigParser()
            config.readfp(open(installed_file))
            if config.has_section('postgresql'):
                return False
            else:
                return True
      
    def _install_cmmi_pg(self):
        try:
            self.log.info('Install postgresql')
            cmmi = hexagonit.recipe.cmmi.Recipe(self.buildout, self.name, self.options)
            cmmi.install()
            
        except:
            raise
                    
    def _create_superusers(self):
        superusers = self.options['superusers'].split()
        for superuser in superusers:
            print 'create superuser: %s' % superuser
            cmd = '%s/createuser -s -d -r -h %s -U %s %s' % (self.options['bin_dir'],
                                                       self.options['socket_dir'],
                                                       self.options['admin'],
                                                       superuser)
            print cmd
            os.system(cmd)
            
    def _create_users(self):
        users = self.options['users'].split()
        for user in users:
            print 'create user: %s' % user
            cmd = '%s/createuser -S -D -R -h %s -U %s %s' % (self.options['bin_dir'],
                                                 self.options['socket_dir'],
                                                 self.options['admin'],
                                                 user)
            print cmd
            os.system(cmd)
            
    def _make_db(self):
        os.mkdir(self.options['data_dir'])
        cmd = '%s/initdb -D %s -U %s' % (self.options['bin_dir'], self.options['data_dir'], self.options['admin'])
        os.chdir(self.options['bin_dir'])
        os.system(cmd)
                                                                           
    def _make_pg_config(self):
        pg_tpl = Template(file=os.path.join(current_dir,'templates', 'postgresql.conf.tmpl'))
        pghba_tpl = Template(file=os.path.join(current_dir,'templates', 'pg_hba.conf.tmpl'))
        pgctl_tpl = Template(file=os.path.join(current_dir,'templates', 'pgctl.py.tmpl'))
        psql_tpl = Template(file=os.path.join(current_dir,'templates', 'psql.sh.tmpl'))
        
        pg_tpl.data_dir = self.options['data_dir']
        pg_tpl.config_dir = self.options['data_dir']                
        pg_tpl.pid_file = self.options['pid_file']
        pg_tpl.socket_dir = self.options['socket_dir']
        pg_tpl.listen_addresses = self.options['listen_addresses']
        pg_tpl.port = self.options['port']
        pg_tpl.unix_socket_directory = self.options['unix_socket_directory']
        pg_tpl.ssl = self.options['ssl']
        pg_tpl.shared_buffers = self.options['shared_buffers']
        pg_tpl.work_mem = self.options['work_mem']
        pg_tpl.temp_buffers = self.options['temp_buffers']
        pg_tpl.fsync = self.options['fsync']
        pg_tpl.synchronous_commit = self.options['synchronous_commit']
        pg_tpl.wal_sync_method = self.options['wal_sync_method']
        pg_tpl.wal_buffers = self.options['wal_buffers']
        pg_tpl.client_min_messages = self.options['client_min_messages']
        pg_tpl.log_min_messages = self.options['log_min_messages']
        pg_tpl.log_error_verbosity = self.options['log_error_verbosity']
        pg_tpl.log_min_error_statement = self.options['log_min_error_statement']
        pg_tpl.log_min_duration_statement = self.options['log_min_duration_statement']
        pg_tpl.silent_mode = self.options['silent_mode']
        pg_tpl.log_line_prefix = self.options['log_line_prefix']
        pg_tpl.track_activities = self.options['track_activities']
        pg_tpl.track_counts = self.options['track_counts']
        pg_tpl.log_parser_stats = self.options['log_parser_stats']
        pg_tpl.log_planner_stats = self.options['log_planner_stats']
        pg_tpl.log_executor_stats = self.options['log_executor_stats']
        pg_tpl.log_statement_stats = self.options['log_statement_stats']
        pghba_tpl.superusers = self.options['superusers'].split()     
        pghba_tpl.users = self.options['users'].split()
        pghba_tpl.admin = self.options['admin']
        
        pg_fd = open(os.path.join(self.options['data_dir'], "postgresql.conf"),'w')
        pghba_fd = open(os.path.join(self.options['data_dir'], "pg_hba.conf"),'w')
        pgctl_fd = open(os.path.join(self.options['bin_dir'], "pgctl.py"),'w')
        psql_fd = open(os.path.join(self.options['bin_dir'], "psql.sh"),'w')

        pgctl_tpl.bin_dir = self.options['bin_dir']
        pgctl_tpl.data_dir = self.options['socket_dir']
        psql_tpl.bin_dir = self.options['bin_dir']
        psql_tpl.socket_dir = self.options['socket_dir']

        print  >> pg_fd, pg_tpl
        print  >> pghba_fd, pghba_tpl
        print  >> pgctl_fd, pgctl_tpl
        print  >> psql_fd, psql_tpl
        
        os.chmod(os.path.join(self.options['bin_dir'], "pgctl.py"), 0755) 
        os.chmod(os.path.join(self.options['bin_dir'], "psql.sh"), 0755)
