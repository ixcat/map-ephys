# map-ephys interative shell module

import os
import sys
import logging
from code import interact
from datetime import datetime
from textwrap import dedent
import time
import numpy as np
import pandas as pd
import re
import datajoint as dj
from pymysql.err import OperationalError


from pipeline import (lab, experiment, tracking, ephys, psth, ccf, histology, export, publication, globus, get_schema_name)

pipeline_modules = [lab, ccf, experiment, ephys, histology, tracking, psth]

log = logging.getLogger(__name__)


def usage_exit():
    print(dedent(
        '''
        usage: {} cmd args

        where 'cmd' is one of:

        {}
        ''').lstrip().rstrip().format(
            os.path.basename(sys.argv[0]),
            str().join("  - {}: {}\n".format(k, v[1])
                       for k, v in actions.items())))

    # print("usage: {p} [{c}] <args>"
    #       .format(p=os.path.basename(sys.argv[0]),
    #               c='|'.join(list(actions.keys()))))
    sys.exit(0)


def logsetup(*args):
    level_map = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET,
    }
    level = level_map[args[0]] if args else logging.INFO

    logfile = dj.config.get('custom', {'logfile': None}).get('logfile', None)

    if logfile:
        handlers = [logging.StreamHandler(), logging.FileHandler(logfile)]
    else:
        handlers = [logging.StreamHandler()]

    datefmt = '%Y-%m-%d %H:%M:%S'
    msgfmt = '%(asctime)s:%(levelname)s:%(module)s:%(funcName)s:%(message)s'

    logging.basicConfig(format=msgfmt, datefmt=datefmt, level=logging.ERROR,
                        handlers=handlers)

    log.setLevel(level)

    logging.getLogger('pipeline').setLevel(level)
    logging.getLogger('pipeline.psth').setLevel(level)
    logging.getLogger('pipeline.ccf').setLevel(level)
    logging.getLogger('pipeline.report').setLevel(level)
    logging.getLogger('pipeline.publication').setLevel(level)
    logging.getLogger('pipeline.ingest.behavior').setLevel(level)
    logging.getLogger('pipeline.ingest.ephys').setLevel(level)
    logging.getLogger('pipeline.ingest.tracking').setLevel(level)
    logging.getLogger('pipeline.ingest.histology').setLevel(level)


def ingest_behavior(*args):
    from pipeline.ingest import behavior as behavior_ingest
    behavior_ingest.BehaviorIngest().populate(display_progress=True)


def ingest_foraging_behavior(*args):
    from pipeline.ingest import behavior as behavior_ingest
    behavior_ingest.BehaviorBpodIngest().populate(display_progress=True)


def ingest_ephys(*args):
    from pipeline.ingest import ephys as ephys_ingest
    ephys_ingest.EphysIngest().populate(display_progress=True)


def ingest_tracking(*args):
    from pipeline.ingest import tracking as tracking_ingest
    tracking_ingest.TrackingIngest().populate(display_progress=True)


def ingest_histology(*args):
    from pipeline.ingest import histology as histology_ingest
    histology_ingest.HistologyIngest().populate(display_progress=True)


def ingest_all(*args):

    log.info('running auto ingest')

    ingest_behavior(args)
    ingest_ephys(args)
    ingest_tracking(args)
    ingest_histology(args)

    def_sheet = {'recording_notes_spreadsheet': None,
                 'recording_notes_sheet_name': None}

    sfile = dj.config.get('custom', def_sheet).get('recording_notes_spreadsheet')
    sname = dj.config.get('custom', def_sheet).get('recording_notes_sheet_name')

    if sfile:
        load_insertion_location(sfile, sheet_name=sname)


def load_animal(excel_fp, sheet_name='Sheet1'):
    df = pd.read_excel(excel_fp, sheet_name)
    df.columns = [cname.lower().replace(' ', '_') for cname in df.columns]

    subjects, water_restrictions, subject_ids = [], [], []
    for i, row in df.iterrows():
        if row.subject_id not in subject_ids and {'subject_id': row.subject_id} not in lab.Subject.proj():
            subject = {'subject_id': row.subject_id, 'username': row.username,
                       'cage_number': row.cage_number, 'date_of_birth': row.date_of_birth.date(),
                       'sex': row.sex, 'animal_source': row.animal_source}
            wr = {'subject_id': row.subject_id, 'water_restriction_number': row.water_restriction_number,
                  'cage_number': row.cage_number, 'wr_start_date': row.wr_start_date.date(),
                  'wr_start_weight': row.wr_start_weight}
            subject_ids.append(row.subject_id)
            subjects.append(subject)
            water_restrictions.append(wr)

    lab.Subject.insert(subjects)
    lab.WaterRestriction.insert(water_restrictions)

    log.info('Inserted {} subjects'.format(len(subjects)))
    log.info('Water restriction number: {}'.format([s['water_restriction_number'] for s in water_restrictions]))


def load_insertion_location(excel_fp, sheet_name='Sheet1'):
    from pipeline.ingest import behavior as behav_ingest
    log.info('loading probe insertions from spreadsheet {}'.format(excel_fp))

    df = pd.read_excel(excel_fp, sheet_name)
    df.columns = [cname.lower().replace(' ', '_') for cname in df.columns]

    insertion_locations = []
    recordable_brain_regions = []
    for i, row in df.iterrows():
        try:
            int(row.behaviour_time)
            valid_btime = True
        except ValueError:
            log.debug('Invalid behaviour time: {} - try single-sess per day'.format(row.behaviour_time))
            valid_btime = False

        if valid_btime:
            sess_key = experiment.Session & (
                behav_ingest.BehaviorIngest.BehaviorFile
                & {'subject_id': row.subject_id, 'session_date': row.session_date.date()}
                & 'behavior_file LIKE "%{}%{}_{:06}%"'.format(
                    row.water_restriction_number, row.session_date.date().strftime('%Y%m%d'), int(row.behaviour_time)))
        else:
            sess_key = False

        if not sess_key:
            sess_key = experiment.Session & {'subject_id': row.subject_id, 'session_date': row.session_date.date()}
            if len(sess_key) == 1:
                # case of single-session per day - ensuring session's datetime matches the filename
                # session_datetime and datetime from filename should not be more than 3 hours apart
                bf = (behav_ingest.BehaviorIngest.BehaviorFile & sess_key).fetch('behavior_file')[0]
                bf_datetime = re.search('(\d{8}_\d{6}).mat', bf).groups()[0]
                bf_datetime = datetime.strptime(bf_datetime, '%Y%m%d_%H%M%S')
                s_datetime = sess_key.proj(s_dt='cast(concat(session_date, " ", session_time) as datetime)').fetch1('s_dt')
                if abs((s_datetime - bf_datetime).total_seconds()) > 10800:  # no more than 3 hours
                    log.debug('Unmatched sess_dt ({}) and behavior_dt ({}). Skipping...'.format(s_datetime, bf_datetime))
                    continue
            else:
                continue

        pinsert_key = dict(sess_key.fetch1('KEY'), insertion_number=row.insertion_number)
        if pinsert_key in ephys.ProbeInsertion.proj():
            if not (ephys.ProbeInsertion.InsertionLocation & pinsert_key):
                insertion_locations.append(dict(pinsert_key, skull_reference=row.skull_reference,
                                                ap_location=row.ap_location, ml_location=row.ml_location,
                                                depth=row.depth, theta=row.theta, phi=row.phi, beta=row.beta))
            if not (ephys.ProbeInsertion.RecordableBrainRegion & pinsert_key):
                recordable_brain_regions.append(dict(pinsert_key, brain_area=row.brain_area,
                                                     hemisphere=row.hemisphere))

    log.debug('InsertionLocation: {}'.format(insertion_locations))
    log.debug('RecordableBrainRegion: {}'.format(recordable_brain_regions))

    ephys.ProbeInsertion.InsertionLocation.insert(insertion_locations)
    ephys.ProbeInsertion.RecordableBrainRegion.insert(recordable_brain_regions)

    log.info('load_insertion_location - Number of insertions: {}'.format(len(insertion_locations)))


def load_ccf(*args):
    ccf.CCFBrainRegion.load_regions()
    ccf.CCFAnnotation.load_ccf_annotation()


def populate_ephys(populate_settings={'reserve_jobs': True, 'display_progress': True}):

    log.info('experiment.PhotostimBrainRegion.populate()')
    experiment.PhotostimBrainRegion.populate(**populate_settings)

    log.info('ephys.UnitCoarseBrainLocation.populate()')
    ephys.UnitCoarseBrainLocation.populate(**populate_settings)

    log.info('ephys.UnitStat.populate()')
    ephys.UnitStat.populate(**populate_settings)

    log.info('ephys.UnitCellType.populate()')
    ephys.UnitCellType.populate(**populate_settings)


def populate_psth(populate_settings={'reserve_jobs': True, 'display_progress': True}):

    log.info('psth.UnitPsth.populate()')
    psth.UnitPsth.populate(**populate_settings)

    log.info('psth.PeriodSelectivity.populate()')
    psth.PeriodSelectivity.populate(**populate_settings)

    log.info('psth.UnitSelectivity.populate()')
    psth.UnitSelectivity.populate(**populate_settings)


def generate_report(populate_settings={'reserve_jobs': True, 'display_progress': True}):
    from pipeline import report
    for report_tbl in report.report_tables:
        log.info(f'Populate: {report_tbl.full_table_name}')
        report_tbl.populate(**populate_settings)


def sync_report():
    from pipeline import report
    for report_tbl in report.report_tables:
        log.info(f'Sync: {report_tbl.full_table_name} - From {report.store_location} - To {report.store_stage}')
        report_tbl.fetch()


def nuke_all():
    if 'nuclear_option' not in dj.config:
        raise RuntimeError('nuke_all() function not enabled')

    from pipeline.ingest import behavior as behavior_ingest
    from pipeline.ingest import ephys as ephys_ingest
    from pipeline.ingest import tracking as tracking_ingest
    from pipeline.ingest import histology as histology_ingest

    ingest_modules = [behavior_ingest, ephys_ingest, tracking_ingest,
                      histology_ingest]

    for m in reversed(ingest_modules):
        m.schema.drop()

    # production lab schema is not map project specific, so keep it.
    for m in reversed([m for m in pipeline_modules if m is not lab]):
        m.schema.drop()


def publication_login(*args):
    cfgname = args[0] if len(args) else 'local'

    if 'custom' in dj.config and 'globus.token' in dj.config['custom']:
        del dj.config['custom']['globus.token']

    from pipeline.globus import GlobusStorageManager

    GlobusStorageManager()

    if cfgname == 'local':
        dj.config.save_local()
    elif cfgname == 'global':
        dj.config.save_global()
    else:
        log.warning('unknown configuration {}. not saving'.format(cfgname))


def publication_publish_ephys(*args):
    publication.ArchivedRawEphys.populate()


def publication_publish_video(*args):
    publication.ArchivedTrackingVideo.populate()


def publication_discover_ephys(*args):
    publication.ArchivedRawEphys.discover()


def publication_discover_video(*args):
    publication.ArchivedTrackingVideo.discover()


def export_recording(*args):
    if not args:
        print("usage: {} export-recording \"probe key\"\n"
              "  where \"probe key\" specifies a ProbeInsertion")
        return

    ik = eval(args[0])  # "{k: v}" -> {k: v}
    fn = args[1] if len(args) > 1 else None
    export.export_recording(ik, fn)


def shell(*args):
    interact('map shell.\n\nschema modules:\n\n  - {m}\n'
             .format(m='\n  - '.join(
                 '.'.join(m.__name__.split('.')[1:])
                 for m in pipeline_modules)),
             local=globals())


def erd(*args):
    report = dj.create_virtual_module('report', get_schema_name('report'))
    mods = (ephys, lab, experiment, tracking, psth, ccf, histology,
            report, publication)
    for mod in mods:
        modname = str().join(mod.__name__.split('.')[1:])
        fname = os.path.join('images', '{}.png'.format(modname))
        print('saving', fname)
        dj.ERD(mod, context={modname: mod}).save(fname)


def automate_computation():
    from pipeline import report
    populate_settings = {'reserve_jobs': True, 'suppress_errors': True, 'display_progress': True}
    while True:
        log.info('Populate for: Ephys - PSTH - Report')
        populate_ephys(populate_settings)
        populate_psth(populate_settings)
        generate_report(populate_settings)

        log.info('report.delete_outdated_session_plots()')
        try:
            report.delete_outdated_session_plots()
        except OperationalError as e:  # in case of mysql deadlock - code: 1213
            if e.args[0] == 1213:
                pass

        log.info('report.delete_outdated_project_plots()')
        try:
            report.delete_outdated_project_plots()
        except OperationalError as e:  # in case of mysql deadlock - code: 1213
            if e.args[0] == 1213:
                pass

        log.info('Delete empty ingestion tables')
        delete_empty_ingestion_tables()

        # random sleep time between 5 to 10 minutes
        sleep_time = np.random.randint(300, 600)
        log.info('Sleep: {} minutes'.format(sleep_time / 60))
        time.sleep(sleep_time)


def delete_empty_ingestion_tables():
    from pipeline.ingest import ephys as ephys_ingest
    from pipeline.ingest import tracking as tracking_ingest
    from pipeline.ingest import histology as histology_ingest

    with dj.config(safemode=False):
        try:
            (ephys_ingest.EphysIngest & (ephys_ingest.EphysIngest
                                         - ephys.ProbeInsertion).fetch('KEY')).delete()
            (tracking_ingest.TrackingIngest & (tracking_ingest.TrackingIngest
                                               - tracking.Tracking).fetch('KEY')).delete()
            (histology_ingest.HistologyIngest & (histology_ingest.HistologyIngest
                                                 - histology.ElectrodeCCFPosition).fetch('KEY')).delete()
        except OperationalError as e:  # in case of mysql deadlock - code: 1213
            if e.args[0] == 1213:
                pass


def sync_and_external_cleanup():
    if dj.config['custom'].get('allow_external_cleanup', False):
        from pipeline import report

        while True:
            log.info('Sync report')
            sync_report()
            log.info('Report "report_store" external cleanup')
            report.schema.external['report_store'].delete(delete_external_files=True)
            log.info('Delete filepath-exists error jobs')
            # This happens when workers attempt to regenerate the plots when the corresponding external files has not yet been deleted
            (report.schema.jobs & 'error_message LIKE "DataJointError: A different version of%"').delete()
            time.sleep(1800)  # once every 30 minutes
    else:
        print("allow_external_cleanup disabled, set dj.config['custom']['allow_external_cleanup'] = True to enable")


actions = {
    'ingest-behavior': (ingest_behavior, 'ingest behavior data'),
    'ingest-foraging': (ingest_behavior, 'ingest foraging behavior data'),
    'ingest-ephys': (ingest_ephys, 'ingest ephys data'),
    'ingest-tracking': (ingest_tracking, 'ingest tracking data'),
    'ingest-histology': (ingest_histology, 'ingest histology data'),
    'ingest-all': (ingest_all, 'run auto ingest job (load all types)'),
    'populate-psth': (populate_psth, 'populate psth schema'),
    'publication-login': (publication_login, 'login to globus'),
    'publication-publish-ephys': (publication_publish_ephys,
                                  'publish raw ephys data to globus'),
    'publication-publish-video': (publication_publish_video,
                                  'publish raw video data to globus'),
    'publication-discover-ephys': (publication_discover_ephys,
                                   'discover raw ephys data on globus'),
    'publication-discover-video': (publication_discover_video,
                                   'discover raw video data on globus'),
    'export-recording': (export_recording, 'export data to .mat'),
    'generate-report': (generate_report, 'run report generation logic'),
    'sync-report': (sync_report, 'sync report data locally'),
    'shell': (shell, 'interactive shell'),
    'erd': (erd, 'write DataJoint ERDs to files'),
    'load-ccf': (load_ccf, 'load CCF reference atlas'),
    'automate-computation': (automate_computation, 'run report worker job'),
    'automate-sync-and-cleanup': (sync_and_external_cleanup,
                                  'run report cleanup job'),
    'load-insertion-location': (load_insertion_location,
                                'load ProbeInsertions from .xlsx'),
    'load-animal': (load_animal, 'load subject data from .xlsx')
}
