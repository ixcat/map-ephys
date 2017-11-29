#! /usr/bin/env python

import os
import sys

import scipy.io as spio

import datajoint as dj

import experiment


if 'legacy_data_dir' not in dj.config:
    dj.config['legacy_data_dir'] = 'x:\map\map-ephys\data'

schema = dj.schema(dj.config['ingest_database'], locals())


@schema
class LegacySession(dj.Lookup):

    definition = """
    legacy_sesion_file:         varchar(255)    # legacy session file
    """

    contents = [[os.path.join(dj.config['legacy_data_dir'], f)]
                for f in os.listdir(dj.config['legacy_data_dir'])
                if f.endswith('.mat')]


@schema
class LegacySessionIngest(dj.Computed):
    definition = """
    -> LegacySessionFile
    ---
    -> experiment.Session
    """

    @property
    def key_source(self):
        return LegacySession()

    def make(self, key):

        sfname = key['legacy_sesion_file']
        print('LegacySessionIngest.make(): Loading {f}'.format(sfname))

        mat = spio.loadmat(sfname, squeeze_me=True)
        SessionData = mat['SessionData']

        # construct session key & add session
        #
        # HACK:
        #
        # Session.session as designed was to be Nth session per animal;
        # assuming here all sessions are for 1 animal and taking
        # number-of-sessions-for-animal+1 as the key..
        # best would be to find animal & trial deterministically
        # from original ingest...
        #

        skey = {'animal': 399572}
        if not experiment.Session() & skey:
            skey['session'] = 1
        else:
            skey['session'] = len(experiment.Session() & skey) + 1

        if experiment.Session() & skey:
            # XXX: raise DataJointError?
            print("Warning! session exists for {f}".format(sfname),
                  file=sys.stderr)

        # do rest of data loading here
        # ...
        # and save a record here to prevent future loading
        self.insert1(dict(**key, **skey))


if __name__ == '__main__':
    do_ingest = False
    if do_ingest:
        LegacySessionIngest().populate()
