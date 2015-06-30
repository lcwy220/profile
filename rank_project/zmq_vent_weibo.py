# -*- coding:utf-8 -*-

import os
import zmq
import time
import json
import struct
from datetime import datetime
from config import ZMQ_VENT_PORT, ZMQ_CTRL_VENT_PORT, ZMQ_VENT_HOST, ZMQ_CTRL_HOST, BIN_FILE_PATH
from bin2json import bin2json

def load_items_from_bin(bin_path):
    return open(bin_path, 'rb')


def send_all(f, sender):
    count = 0
    tb = time.time()
    ts = tb
    
    while 1:
        hdr = f.read(8)
        if len(hdr) != 8 or '' == hdr:
            f.close()
            break

	d1, sp_type, d3, d4, total_len = struct.unpack("!ccccI", hdr)

	data = f.read(total_len - 8)
	if len(data) != total_len - 8 or '' == data:
	    f.close()
	    break
	
        weibo_item = bin2json(data, total_len, sp_type)
        if weibo_item:
            sender.send_json(weibo_item)
            count += 1

        if count % 10000 == 0:
            te = time.time()
            print '[%s] deliver speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, 10000) 
            if count % 100000 == 0:
                print '[%s] total deliver %s, cost %s sec [avg %s per/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb)) 
            ts = te

    total_cost = time.time() - tb
    return count, total_cost

if __name__=="__main__":
    """
    push data to every work

    """
    

    context = zmq.Context()

    # use for send weibo
    sender = context.socket(zmq.PUSH)
    sender.bind('tcp://%s:%s' %(ZMQ_VENT_HOST, ZMQ_VENT_PORT))  
    
    # use for worker control
    controller = context.socket(zmq.SUB)
    controller.connect('tcp://%s:%s' % (ZMQ_CTRL_HOST, ZMQ_CTRL_VENT_PORT))
    controller.setsockopt(zmq.SUBSCRIBE, "")

    message = "RESTART"
    while True:
	type = controller.recv()
	print type
        if type == "PAUSE":
	    time.sleep(5)
	    message = "PAUSE"
            continue
	elif type == "RESTART":
	    message = "RESTART"
	else:
	    if message == "PAUSE":
		time.sleep(5)
		continue
	    else:
	        pass

	total_count = 0
	total_cost = 0

	try:
	    file_list = set(os.listdir(BIN_FILE_PATH))
	    print "total file is ", len(file_list)
	    for each in file_list:
	        if 'bin' in each and 'ok' not in each:
		    filename = each.split('.')[0]
		    if '%s.bin.ok' % filename in file_list and '%s_yes.txt' % filename not in file_list:
		        bin_input = load_items_from_bin(os.path.join(BIN_FILE_PATH, each))
		        load_origin_data_func = bin_input
		        tmp_count, tmp_cost = send_all(load_origin_data_func, sender)
		        total_count += tmp_count
		        total_cost += tmp_cost

		        with open(os.path.join(BIN_FILE_PATH, '%s_yes.txt' % filename), 'w') as fw:
			    fw.write('finish reading' + '\n')

	    print 'this scan total deliver %s, cost %s sec' % (total_count, total_cost)
        except Exception,e:
            print Exception, ":", e


