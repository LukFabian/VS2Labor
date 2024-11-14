import logging
import rpc
from context import lab_channel, lab_logging

# Logging-Konfiguration
lab_logging.setup(stream_level=logging.INFO)
logger = logging.getLogger('vs2lab.lab2.rpc.runsrv')

# Redis-Channel-Setup
chan = lab_channel.Channel()
chan.channel.flushall()
logger.debug('Flushed all redis keys.')

# Server-Setup und Start
srv = rpc.Server()
srv.run()
