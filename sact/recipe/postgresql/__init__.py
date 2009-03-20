import zc.buildout
import urlparse
import tempfile
import logging
import urllib
import shutil
import subprocess
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
    
        self.options['admin'] = options.get("admin", "postgres")
        self.options['superusers'] = options.get("superusers", "root")
        self.options['users'] = options.get("users", "")
        self.options['install'] = options.get("install", "yes")

        self.options['location'] = os.path.join(buildout['buildout']['parts-directory'], self.name)
        self.options['url-bin'] = options.get("url-bin", "")
        self.options['bin_dir'] = options.get("bin-dir", os.path.join(self.options['location'], "bin"))
        self.options['data_dir'] = options.get("config-dir", os.path.join(self.options['location'], "db"))
        self.options['pid_file'] = options.get("pid-file", os.path.join(self.options['location'], "db", "postgresql.pid"))
        self.options['socket_dir'] = options.get("pid-file", os.path.join(self.options['location'], "db"))
        self.options['listen_addresses'] = options.get('listen_addresses', '')
        self.options['port'] = options.get('port', '5432')
        self.options['unix_socket_directory'] = options.get('unix_socket_directory', self.options['location'])
        self.options['ssl'] = options.get('ssl', 'off')

        self.options['shared_buffers'] = options.get('shared_buffers', '24MB')
        self.options['work_mem'] = options.get('work_mem', '1MB')
        self.options['maintenance_work_mem'] = options.get('maintenance_work_mem', '16MB')        
        self.options['temp_buffers'] = options.get('temp_buffers', '8MB')
        self.options['fsync'] = options.get('fsync', 'on')
        self.options['synchronous_commit'] = options.get('synchronous_commit', 'on')
        self.options['wal_sync_method'] = options.get('wal_sync_method', 'fsync')
        self.options['wal_buffers'] = options.get('wal_buffers', '64kB')
        self.options['wal_writer_delay'] = options.get('wal_writer_delay', '200ms')
        self.options['client_min_messages'] = options.get('client_min_messages', 'notice')
        self.options['update_process_title'] = options.get('update_process_title', 'on')        
        self.options['bgwriter_delay'] = options.get('bgwriter_delay', '200ms')
        self.options['bgwriter_lru_maxpages'] = options.get('bgwriter_lru_maxpages', '100')
        self.options['bgwriter_lru_multiplier'] = options.get('bgwriter_lru_multiplier', '2.0')
        self.options['max_fsm_pages'] = options.get('max_fsm_pages', '153600')
        self.options['max_fsm_relations'] = options.get('max_fsm_relations', '1000')
        self.options['max_files_per_process'] = options.get('max_files_per_process', '1000')
        self.options['silent_mode'] = options.get('silent_mode', 'off')
        self.options['track_activities'] = options.get('track_activities', 'on')
        self.options['track_counts'] = options.get('track_counts', 'on')
        self.options['commit_delay'] = options.get('commit_delay', '0')
        self.options['commit_siblings'] = options.get('commit_siblings', '5')
        self.options['debug_print_parse'] = options.get('debug_print_parse', 'off')
        self.options['debug_print_rewritten'] = options.get('debug_print_rewritten', 'off')
        self.options['debug_print_plan'] = options.get('debug_print_plan', 'off')
        self.options['debug_pretty_print'] = options.get('debug_pretty_print', 'off')
        self.options['log_min_messages'] = options.get('log_min_messages', 'notice')
        self.options['log_error_verbosity'] = options.get('log_error_verbosity', 'default')
        self.options['log_min_error_statement'] = options.get('log_min_error_statement', 'error')
        self.options['log_min_duration_statement'] = options.get('log_min_duration_statement', '-1')
        self.options['log_parser_stats'] = options.get('log_parser_stats', 'off')
        self.options['log_planner_stats'] = options.get('log_planner_stats', 'off')
        self.options['log_executor_stats'] = options.get('log_executor_stats', 'off')
        self.options['log_statement_stats'] = options.get('log_statement_stats', 'off')       
        self.options['log_checkpoints'] = options.get('log_checkpoints', 'off')
        self.options['log_connections'] = options.get('log_connections', 'off')
        self.options['log_disconnections'] = options.get('log_disconnections', 'off')
        self.options['log_duration'] = options.get('log_duration', 'off')
        self.options['log_lock_waits'] = options.get('log_lock_waits', 'off')
        self.options['log_line_prefix'] = options.get('log_line_prefix', '%t ')        

    def install(self):
        
        first = (self.options['install'] == "yes") or self.is_first_install()
        
        if first:
            if self.options['url-bin']:
                self._install_compiled_pg()
                self.log.info('Update PG configuration')
                self._make_pg_config()
                # FIXME: users / superusers not working
                
                cmd = '%s/pgctl start' % self.buildout['buildout']['bin-directory']
                p_start = subprocess.Popen(cmd, shell=True)
                import time
                time.sleep(3.0)
                p_start.communicate(self._create_superusers())
                p_start.communicate(self._create_users())
                cmd = '%s/pgctl stop' % self.buildout['buildout']['bin-directory']
                p_stop = subprocess.Popen(cmd, shell=True)
                p_stop.communicate(p_start)

            else:
                self._install_cmmi_pg()
                self.log.info('Create database')
                self._make_db()
                self.log.info('Update PG configuration')
                self._make_pg_config()
                # FIXME: users / superusers not working
                os.system('%s/pgctl.py start' % (self.options['bin_dir']))
                self._create_superusers()        
                self._create_users()
                os.system('%s/pgctl.py stop' % (self.options['bin_dir']))       

        else:
            self._make_pg_config()      
        
        return self.options['location']
    
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
            raise zc.buildout.UserError("Unable to install source version of postgresql")
        
    def _install_compiled_pg(self):
        # Download the binaries using hexagonit.recipe.download
        
        try:
            opt = self.options.copy()
            opt['url'] = self.options['url-bin']
            opt['destination'] = self.options['location']
            hexagonit.recipe.download.Recipe(self.buildout, self.name, opt).install()
        except:
            raise zc.buildout.UserError("Unable to download binaries version of postgresql")
 
    def _create_superusers(self):
        superusers = self.options['superusers'].split()
        for superuser in superusers:
            self.log.info('create superuser %s' % superuser)
            cmd = '%s/createuser -s -d -r -h %s -U %s %s' % (self.buildout['buildout']['bin-directory'],
                                                       self.options['socket_dir'],
                                                       self.options['admin'],
                                                       superuser)
            p = subprocess.Popen(cmd, shell=True)
            
    def _create_users(self):
        users = self.options['users'].split()
        for user in users:
            self.log.info('create user %s' % user)
            cmd = '%s/createuser -S -D -R -h %s -U %s %s' % (self.buildout['buildout']['bin-directory'],
                                                 self.options['socket_dir'],
                                                 self.options['admin'],
                                                 user)
            p = subprocess.Popen(cmd, shell=True)
            
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
        createuser_tpl = Template(file=os.path.join(current_dir,'templates', 'createuser.sh.tmpl'))
        createdb_tpl = Template(file=os.path.join(current_dir,'templates', 'createdb.sh.tmpl'))


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
        pg_tpl.update_process_title = self.options['update_process_title']   
        pg_tpl.wal_writer_delay = self.options['wal_writer_delay']
        pg_tpl.bgwriter_delay = self.options['bgwriter_delay']
        pg_tpl.bgwriter_lru_maxpages = self.options['bgwriter_lru_maxpages']
        pg_tpl.bgwriter_lru_multiplier = self.options['bgwriter_lru_multiplier']
        pg_tpl.max_fsm_pages = self.options['max_fsm_pages']
        pg_tpl.max_fsm_relations = self.options['max_fsm_relations']
        pg_tpl.maintenance_work_mem = self.options['maintenance_work_mem']
        pg_tpl.max_files_per_process = self.options['max_files_per_process']
        pg_tpl.commit_delay = self.options['commit_delay']
        pg_tpl.commit_siblings = self.options['commit_siblings']
        pg_tpl.debug_print_parse = self.options['debug_print_parse']
        pg_tpl.debug_print_rewritten = self.options['debug_print_rewritten']
        pg_tpl.debug_print_plan = self.options['debug_print_plan']
        pg_tpl.debug_pretty_print = self.options['debug_pretty_print']
        pg_tpl.log_checkpoints = self.options['log_checkpoints']
        pg_tpl.log_connections = self.options['log_connections']
        pg_tpl.log_disconnections = self.options['log_disconnections']
        pg_tpl.log_duration = self.options['log_duration']
        pg_tpl.log_lock_waits = self.options['log_lock_waits']
        pg_tpl.log_line_prefix = self.options['log_line_prefix']
        
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