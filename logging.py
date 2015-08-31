# Copyright (C) 2015 Idein Inc.
# Author: koichi

import logging
import logging.handlers
import sys

logger = logging.getLogger('carnival')
logger.setLevel(logging.DEBUG)
if sys.platform == 'linux':
    address = '/dev/log'
elif sys.platform == 'darwin':
    address = '/var/run/syslog'
syslog = logging.handlers.SysLogHandler(address=address)
formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
syslog.setFormatter(formatter)
logger.addHandler(syslog)
