#!/usr/bin/env python3

import re

parsed = dict()

def is_audit(syslog_event):
    body, header_parts = syslog_eventparts(syslog_event)
    return (header_parts[5].upper() == 'AUDIT')

def is_activity(syslog_event):
    body, header_parts = syslog_eventparts(syslog_event)
    return (header_parts[5].upper() == 'ACTIVITY')

def syslog_eventparts(syslog_event):
    header=syslog_event.split('[',1)[0]
    body=syslog_event.split('[',1)[1]
    header_parts=header.split(' ')
    return body, header_parts

def syslog_transform(syslog_event):
    body, header_parts = syslog_eventparts(syslog_event)
    parsed['timeStamp']=header_parts[1]
    parsed['eventDate']=header_parts[1].split('T')[0]
    parsed['eventType']=header_parts[5].upper()
    body_str=body[:]
    esc=0
    start=0
    end=0
    body_parts=list()
    while True:
        try:
          esc = body_str.index('\\]', start)
        except:
          esc = 0
        try:
           start = start+1 if body_str[start:-1][0]=='[' else start
           end = body_str.index(']', start)
        except:
           end = 0
           if (esc > 0 and esc != end):
             continue
        if (end > 0):
           body_parts.append(body_str[start:end])
           start = end + 1
        if (end == 0):
            break

    if (not check(body_parts, 'guId=')):
         parsed['eventType'] = 'ERROR_' + header_parts[5].upper()
    list_attr = list()
    for item in body_parts:
        list_attributes = [attrib_dict.strip('"') for attrib in item.split('" ') for attrib_dict in attrib.split('="', 1)]
        m = re.search(r'([a-zA-Z]*)(@)([0-9]*)\s+(.+)', list_attributes[0])
        if m:
            if (not m.group(1).lower().startswith('requestcontext')):
                parsed['eventName'] = m.group(1)
            list_attributes[0] = m.group(4)
        else:
            print('Empty list_attributes : {}'.format(list_attributes[0]))
            list_attributes.pop(0)
        list_attr += list_attributes
    parsed_upd = merge(parsed, dict(zip(*[iter(list_attr)] * 2)))
    return parsed_upd

def check(body_parts, guIdStr):
    guId_fnd = False
    for item in body_parts:
        if (item.find(guIdStr) >= 0):
            guId_fnd = True
            break
    return guId_fnd

def merge(dict1, dict2):
    dict3 = dict1.copy()
    dict3.update(dict2)
    return dict3