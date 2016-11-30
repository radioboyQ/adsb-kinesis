import pyModeS as pms
import sys
import logging
import json
from kinesis_producer import KinesisProducer

config = dict(
    aws_region='us-east-1',
    buffer_size_limit=100000,
    buffer_time_limit=0.2,
    kinesis_concurrency=1,
    kinesis_max_retries=10,
    record_delimiter='\n',
    stream_name='rb-adsb',
    )

config2 = dict(
    aws_region='us-east-1',
    buffer_size_limit=100000,
    buffer_time_limit=0.2,
    kinesis_concurrency=1,
    kinesis_max_retries=10,
    record_delimiter='\n',
    stream_name='rb-adsb-raw',
    )


def myreadlines(f, newline):
  print("called myreadlines")
  buf = ""
  while True:
    while newline in buf:
      pos = buf.index(newline)
      yield buf[:pos]
      buf = buf[pos + len(newline):]
    chunk = f.read(512)
    if not chunk:
      yield buf
      break
    buf += chunk


log = logging.getLogger('kinesis_producer.client')
level = logging.getLevelName('DEBUG')
logging.basicConfig()
log.setLevel(level)

k = KinesisProducer(config=config)
k2 = KinesisProducer(config=config2)

for line in myreadlines(sys.stdin, ";"):
    line = line.strip()
    mesg = line[2:]
    k2.send(mesg)
    type = pms.adsb.typecode(mesg)
    if 1 <= type <= 4:
        type = pms.adsb.typecode(mesg)
        icao = pms.adsb.icao(mesg)
        callsign = pms.adsb.callsign(mesg)
        print('aircraft id message')
        print("Type %s message" % type)
        print('callsign: %s' % pms.adsb.callsign(mesg))
        jsonobj =  {
		"type": type,
		"icao": icao,	
		"callsign": callsign
	}
        k.send(json.dumps(jsonobj))
    elif 9 <= type <= 18:
        type = pms.adsb.typecode(mesg)
        icao = pms.adsb.icao(mesg)
        jsonobj =  {
                "type": type,
                "icao": icao,   
                "altitude": pms.adsb.altitude(mesg)
        }
        k.send(json.dumps(jsonobj))
    elif type == 19:
        print("Type 19 mesg")
    else:
        print("Type %s message" % type)

k.close()
k.join()
k2.close()
k2.join()
