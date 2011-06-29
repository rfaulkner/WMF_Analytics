
"""
    The home directory of all projects.  Custom modules are imported from here.
"""
__home__ = '/home/foobar/projects/'


"""
    Project home for Fundraiser Reporting
"""
__project_home__ = '/home/foobar/projects/project_name/'

"""
   Web app and django scripts are stored here 
"""
__web_home__ = '/home/foobar/projects/project_name/web/'


"""
    URL of the squid log server
"""
__squid_log_server__ = 'log.server.org'


"""
    Remote directory for the squid udp2log parsed request logs 
"""
__squid_log_home__ = '/src/log_home/'


"""
    Local directory for the squid udp2log parsed request logs
"""
__squid_log_local_home__ = __project_home__ + 'logs/'


"""
    Database server connection info and login credentials
"""
__user__ = 'user'
__db__ = 'db'
__db_server__ = '127.0.0.1'
__db_port__ = 0001
__pass__='pass'