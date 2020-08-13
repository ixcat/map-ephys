#! /usr/bin/env python

import os
import sys
import logging

from hashlib import sha1

import magic

from pipeline.globus import GlobusStorageManager as GSM


me, app, log, gsm = None, None, None, None


def setup():
    global me, app, log

    me = sys.argv[0]
    app = os.path.basename(me)

    datefmt = '%Y-%m-%d %H:%M:%S'
    msgfmt = '%(asctime)s:%(levelname)s:%(module)s:%(funcName)s:%(message)s'

    logging.basicConfig(format=msgfmt, datefmt=datefmt, level=logging.WARNING,
                        handlers=[logging.FileHandler('{}.log'.format(me))])

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)


def get_gsm(le_id, re_id):

    log.debug('get_gsm')

    global gsm

    def new_gsm():
        log.debug('new_gsm')
        gsm = GSM()
        gsm.wait_timeout = 3600
        gsm.activate_endpoint(le_id)
        gsm.activate_endpoint(re_id)
        return gsm

    gsm = gsm if gsm else new_gsm()

    return gsm


def lister(fname):
    with open(fname, 'r') as fh:
        for line in fh.readlines():
            yield line.rstrip().lstrip('projects/mesoscaleactivityproject/')


def checker(le_id, le_dir, re_id, re_dir, fname):

    gsm = get_gsm(le_id, re_id)

    log.info('checking: {}'.format(fname))

    re_src = '{}:{}/{}'.format(re_id, re_dir, fname)

    le_tmp = '{}.tmp'.format(app)
    le_tgt = '{}/{}'.format(le_dir, le_tmp)
    le_dst = '{}:{}'.format(le_id, le_tgt)
    le_out = os.path.expanduser(le_tgt)

    if os.path.exists(le_out):
        log.debug('clearning old transfer output from {}'.format(le_out))
        os.unlink(le_out)

    log.info('.. transfer {} => {}'.format(re_src, le_dst))

    if not gsm.cp(re_src, le_dst):
        log.warning('.. transfer network error! skipping.')
        return

    if not os.path.exists(le_out):
        log.warning('.. transfer storage error! skipping.')
        return

    log.debug('getting stat information for {}'.format(le_out))
    le_out_stat = os.stat(le_out)

    log.debug('getting file magic information for {}'.format(le_out))
    le_out_magic = magic.from_file(le_out)

    log.debug('getting sha1 information for {}'.format(le_out))
    le_out_sha = sha1(le_out.encode()).hexdigest()

    rec = "{} {} {} # {}".format(le_out_stat.st_size, le_out_sha,
                                 fname, le_out_magic)

    log.info('.. result: {}'.format(rec))
    print(rec)


def main(*args):
    setup()

    if len(args) < 4:
        print('usage: {} local:/cachedir remote:/rootdir filelist'.format(
            app))
        sys.exit(0)

    le_cfg = args[1]
    re_cfg = args[2]
    flist = args[3]

    le_id, le_dir = le_cfg.split(':')
    re_id, re_dir = re_cfg.split(':')

    log.info('#' * 40)
    log.info('{} startup'.format(app))
    log.info('flist: {}'.format(flist))
    log.info('le_id: {}, le_dir: {}'.format(le_id, le_dir))
    log.info('re_id: {}, re_dir: {}'.format(re_id, re_dir))
    log.info('#' * 40)

    for fname in lister(flist):
        checker(le_id, le_dir, re_id, re_dir, fname)

    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv))
