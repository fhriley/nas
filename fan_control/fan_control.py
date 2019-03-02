#!/usr/bin/env python

from __future__ import print_function
import subprocess
import socket
import os
import signal
from sched import scheduler
import time
import argparse
import logging
import logging.handlers
import json
import re
from pprint import pprint, pformat
import requests

LOG_DIR = '/var/log'
MIN_DC = 11
MAX_DC = 100

class System:
    IPMITOOL = '/usr/bin/ipmitool '
    IPMITOOL_LAN = IPMITOOL + '-I lanplus -H {host} -U {user} -P {password} '
    DUTY_CYCLE = 'raw 0x30 0x30 0x02 0xff {val} > /dev/null'
    FAN_CONTROL_MODE = 'raw 0x30 0x30 0x01 {val} > /dev/null'
    CPUS = ('0xe', '0xf')
    CPU_TEMP = 'raw 0x04 0x2d {cpu}'
    HD_TEMP = '/usr/sbin/hddtemp {disk_regex}'

    def __init__(self, logger, ipmi_info, disk_regex):
        self._logger = logger
        self._ipmi_info = ipmi_info
        self._disk_regex = disk_regex

    def _ipmi_cmd(self, cmd):
        if self._ipmi_info:
            ipmi_info = self._ipmi_info.copy()
            pw = ipmi_info['password']
            ipmi_info['password'] = '<hidden>'
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug('%s%s', self.IPMITOOL_LAN.format(**ipmi_info), cmd)
            ipmi_info['password'] = pw
            return self.IPMITOOL_LAN.format(**ipmi_info) + cmd
        else:
            self._logger.debug('%s%s', self.IPMITOOL, cmd)
            return self.IPMITOOL + cmd

    def _convert_sdr_reading(self, mm, bb, k1, k2, val):
        return ((mm * val) + (bb * pow(10.0, k1))) * pow(10.0, k2)

    def _cpu_temp(self, val):
        return self._convert_sdr_reading(1, -128, 0, 0, val)

    def enable_manual_fan_control(self, flag=True):
        cmd = self.FAN_CONTROL_MODE.format(val=hex(0 if flag else 1))
        subprocess.check_call(self._ipmi_cmd(cmd), shell=True)

    def set_duty_cycle(self, value):
        cmd = self.DUTY_CYCLE.format(val=hex(int(round(min(max(value, args.dc_min), args.dc_max)))))
        subprocess.check_call(self._ipmi_cmd(cmd), shell=True)

    def get_cpu_temp(self):
        vals = [subprocess.check_output(self._ipmi_cmd(self.CPU_TEMP.format(cpu=cpu)).split()).strip()
                for cpu in self.CPUS]
        temps = [self._cpu_temp(int(val.split()[0], 16)) for val in vals]
        if temps:
            temps_max = max(temps)
            self._logger.debug('%s mean: %s max: %s', temps, sum(temps) / float(len(temps)), temps_max)
            return temps_max
        raise Exception('unable to get cpu temps')

    def get_disk_temp(self):
        temps = []
        cmd = self.HD_TEMP.format(disk_regex=self._disk_regex)
        self._logger.debug(cmd)
        output = subprocess.check_output(cmd, shell=True).strip()
        temps = [float(val) for val in re.findall(r'.+:\s([0-9]+)\xc2\xb0C', output)]
        if temps:
            temps_max = max(temps)
            temps_mean = sum(temps) / len(temps)
            self._logger.debug('%s mean: %s max: %s', temps, temps_mean, temps_max)
            return temps_mean
        raise Exception('unable to get disk temps')

class State:
    def __init__(self, args, logger, system, control):
        self.args = args
        self.logger = logger
        self.system = system
        self.duty_cycle = args.dc_default
        self.control = control
        self.last_error = 0
        self.interval_cnt = 0
        self.cpu_dc_min = args.cpu_dc_min

    def reset_pid(self, dc):
        self.last_error = 0
        self.interval_cnt = 0
        self.duty_cycle = dc

    def set_duty_cycle(self, dc):
        actual_dc = int(round(dc))
        actual_curr_dc = int(round(self.duty_cycle))
        if actual_dc != actual_curr_dc:
            self.system.set_duty_cycle(actual_dc)
            self.logger.debug('new duty cycle: %d', actual_dc)
        self.duty_cycle = dc

    def __str__(self):
        return os.linesep.join(['%s: %s' % (vv, getattr(self, vv))
                                for vv in vars(self)
                                if vv not in ('args', 'logger', 'system')])

def get_logger(name, filename, level):
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    handler = logging.handlers.RotatingFileHandler(os.path.join(LOG_DIR, filename),
                                                   maxBytes=10*1024*1024, backupCount=5)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s|%(name)s|%(levelname)s|%(lineno)d|%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger

def cpu_control(logger, args, cpu_dc_min, cpu_temp):
    assert cpu_temp >= args.cpu_tmin - args.cpu_tmin_hyst
    coef = float(args.cpu_dc_max - cpu_dc_min) / (float(args.cpu_tmax - args.cpu_tmin) ** 2)
    logger.debug('coef=%f', coef)
    offset = max(cpu_temp, args.cpu_tmin) - args.cpu_tmin
    logger.debug('offset=%d', offset)
    return int(round(coef * (offset ** 2) + cpu_dc_min))

def pid(state, now, pv):
    time_unit = state.args.time_unit * state.args.disk_time_intervals
    logger.debug('sp: %s pv: %s', state.args.set_point, pv)

    error = state.args.set_point - pv
    int_err = state.args.kc * (1 + time_unit / state.args.ti) * error
    last_err = state.args.kc * state.last_error
    cv = int_err - last_err
    logger.debug('Kc: %.1f Ti: %.1f err: %.3f int_err: %.2f last_err: %.2f cv:%.2f',
                 state.args.kc, state.args.ti, error, int_err, last_err, cv)

    state.last_error = error
    return min(max(state.duty_cycle + cv, args.dc_min), args.dc_max)

def control_loop(sch, now, state):
    state.logger.debug('loop start now=%s', now)
    cpu_temp = state.system.get_cpu_temp()

    cpu_temp_check = state.args.cpu_tmin
    if state.control == 'cpu':
        cpu_temp_check -= (state.args.cpu_tmin_hyst - 1)
    if cpu_temp >= cpu_temp_check:
        logger.debug('%s%s', os.linesep, state)
        state.logger.debug('using cpu control loop')
        if state.control == 'disk':
            state.cpu_dc_min = max(state.args.cpu_dc_min, state.duty_cycle)
        state.control = 'cpu'
        new_dc = cpu_control(state.logger, state.args, state.cpu_dc_min, cpu_temp)
        state.set_duty_cycle(new_dc)
        state.reset_pid(new_dc)
    else:
        state.control = 'disk'
        state.interval_cnt -= 1
        if state.interval_cnt <= 0:
            logger.debug('%s%s', os.linesep, state)
            state.logger.debug('using disk pid loop')
            state.interval_cnt = state.args.disk_time_intervals
            disk_temp = state.system.get_disk_temp()
            state.set_duty_cycle(pid(state, now, disk_temp))
        else:
            state.logger.debug('nothing to do')

    if not state.args.skip_influx:
        write_influx(state, now)

    now += state.args.time_unit
    state.logger.debug('scheduling next call at %s', now)
    sch.enterabs(now, 1, control_loop, (sch, now, state))

def write_influx(state, now):
    try:
        ts = now * 1000000000
        hostname = socket.gethostname()
        data = ['duty_cycle,host=%s value=%d %d' % (hostname, round(state.duty_cycle), ts),
                'set_point,host=%s value=%d %d' % (hostname, state.args.set_point, ts)]
        state.logger.debug(data)
        resp = requests.post('%s/write' % state.args.influx_uri, params={'db': state.args.influx_db}, data='\n'.join(data))
        resp.raise_for_status()
    except Exception as ex:
        state.logger.exception(ex)

_sighup_dc = None
def log_loop(sch, logger, args, now, out_file):
    global _sighup_dc
    logger.debug('loop start now=%s', now)
    disk_temp = get_disk_temp(logger, args)
    if _sighup_dc:
        system = System(logger, args.ipmi_info)
        system.enable_manual_fan_control()
        system.set_duty_cycle(_sighup_dc)
        _sighup_dc = None
    out_file.write('%f,%d,%f%s' % (now, _sighup_dc or 0, disk_temp, os.linesep))
    out_file.flush()
    now += args.time_unit
    logger.debug('scheduling next call at %s', now)
    sch.enterabs(now, 1, log_loop, (sch, logger, args, now, out_file))

def log(logger, args):
    with open(args.log, 'w') as out_file:
        sch = scheduler(time.time, time.sleep)
        sch.enter(0, 1, log_loop, (sch, logger, args, time.time(), out_file))
        sch.run()
    return 0

def ranged_type(val, typ, min, max):
    try:
        val = typ(val)
    except ValueError:
        raise argparse.ArgumentTypeError('must be %s' % str(typ))
    if val < min or val > max:
        raise argparse.ArgumentTypeError('val must be >= %s and <= %s' % (min, max))
    return val

def ranged_int(val, min, max): return ranged_type(val, int, min, max)
def ranged_float(val, min, max): return ranged_type(val, float, min, max)

def ipmi_info_file(val):
    try:
        ipmi_info = json.load(open(val, 'r'))
    except:
        raise argparse.ArgumentTypeError('invalid ipmi-info-file')
    for key in ('host', 'user', 'password'):
        if not ipmi_info.get(key):
            raise argparse.ArgumentTypeError('invalid ipmi-info-file')
    return ipmi_info

def handler(signum, frame, args):
    global _sighup_dc
    if signum == signal.SIGHUP and args.sighup_dc:
        _sighup_dc = args.sighup_dc
    else:
        raise SystemExit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fan control')
    parser.add_argument('--ipmi-info', type=ipmi_info_file,
                        help='the JSON file containing the ipmi information')
    parser.add_argument('--disks', default='/dev/disk/by-bay/bay[1-8]',
                        help='the bash shell regex to use to get disk devices')
    parser.add_argument('--dc-default', type=lambda xx: ranged_int(xx, MIN_DC, MAX_DC), default=20,
                        help='the default duty cycle when starting up (default: 20)')
    parser.add_argument('--dc-min', type=lambda xx: ranged_int(xx, MIN_DC, MAX_DC), default=15,
                        help='the minimum duty cycle for the disk pid loop (default: 15)')
    parser.add_argument('--dc-max', type=lambda xx: ranged_int(xx, MIN_DC, MAX_DC), default=100,
                        help='the maximum duty cycle for the disk pid loop (default: 100)')
    parser.add_argument('--time-unit', type=lambda xx: ranged_float(xx, 10, 60), metavar='SECS', default=10.0,
                        help='the number of seconds between each iteration of the main loop (default: 10)')
    parser.add_argument('--disk-time-intervals', type=lambda xx: ranged_int(xx, 1, 100), default=3,
                        help='the number of iterations of the main loop before an iteration of the disk pid loop runs (default: 6)')
    parser.add_argument('--cpu-tmin', type=float, default=50,
                        help='the temperature where the cpu loop begins (default: 50)')
    parser.add_argument('--cpu-tmax', type=float, default=80,
                        help='the temperature at which the cpu loop will be at max duty cycle (default: 80)')
    parser.add_argument('--cpu-tmin_hyst', type=float, default=5,
                        help='the hysteresis when exiting the cpu loop, exit-temp = tmin - tmin_hyst (default: 5)')
    parser.add_argument('--cpu-dc-min', type=lambda xx: ranged_int(xx, MIN_DC, MAX_DC), default=20,
                        help='the minimum cpu loop duty cycle (default: 20)')
    parser.add_argument('--cpu-dc-max', type=lambda xx: ranged_int(xx, MIN_DC, MAX_DC), default=100,
                        help='the maximum cpu loop duty cycle (default: 100)')
    parser.add_argument('--set-point', type=float, metavar='TEMP_C', default=32,
                        help='the disk temperature set point (default: 32)')
    parser.add_argument('--kc', type=float, default=-5.0,
                        help='the controller gain (default: -5.0)')
    parser.add_argument('--ti', type=float, default=300,
                        help='the integral time (default: 300)')
    parser.add_argument('--skip-influx', action='store_true')
    parser.add_argument('--use-deriv', action='store_true')
    parser.add_argument('--influx-uri', default='http://influx.lan:8086',
                        help='send information to the given influx URI')
    parser.add_argument('--influx-db', default='fans',
                        help='send information to the given influx db')
    parser.add_argument('--log-level', default='DEBUG', help='set the log level')
    parser.add_argument('--set-dc', type=lambda xx: ranged_int(xx, MIN_DC, MAX_DC),
                        help='set the duty cycle and exit (also turns on manual control)')
    parser.add_argument('--set-control', type=int,
                        help='set manual control and exit, non-zero=enabled')
    parser.add_argument('--log', help='stay in manual mode and log the disk temps to this file')
    parser.add_argument('--sighup-dc', type=float, help='set the dc to this value on a SIGHUP')
    args = parser.parse_args()

    logger = get_logger('fan_control', 'fan_control.log', args.log_level)

    for sig in ('SIGALRM', 'SIGHUP', 'SIGTERM', 'SIGUSR1', 'SIGUSR2'):
        signal.signal(getattr(signal, sig), lambda signum, frame: handler(signum, frame, args))

    if args.log:
        raise SystemExit(log(logger, args))

    if args.dc_min > args.dc_max:
         parser.error('duty cycle min must be <= duty cycle max')
    args.max_correction_low = args.dc_min - args.dc_default
    args.max_correction_high = args.dc_max - args.dc_default

    system = System(logger, args.ipmi_info, args.disks)

    if args.set_dc:
        system.enable_manual_fan_control()
        system.set_duty_cycle(args.set_dc)
        raise SystemExit(1)

    if args.set_control is not None:
        system.enable_manual_fan_control(bool(args.set_control))
        raise SystemExit(1)

    try:
        system.enable_manual_fan_control()
        system.set_duty_cycle(args.dc_default)
        time.sleep(5)

        state = State(args, logger, system, 'disk')

        sch = scheduler(time.time, time.sleep)
        sch.enter(0, 1, control_loop, (sch, time.time(), state))
        sch.run()
    except KeyboardInterrupt:
        pass
    finally:
        system.enable_manual_fan_control(False)
