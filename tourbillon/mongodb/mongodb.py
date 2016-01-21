import logging
import time

import pymongo


logger = logging.getLogger(__name__)


def get_mongodb_stats(agent):
    agent.run_event.wait()
    config = agent.config['mongodb']
    db_config = config['database']
    agent.create_database(**db_config)
    conn = config['connection']
    client = pymongo.MongoClient(conn['url'])
    db = client.get_database(conn['database'])
    while agent.run_event.is_set():
        time.sleep(config['frequency'])
        try:
            status = db.command('serverStatus')
            points = [{
                'measurement': 'mongodb_stats',
                'tags': {
                    'host': status['host'],
                },
                'fields': {
                }
            }]
            fields = points[0]['fields']
            bf = status['backgroundFlushing']
            fields['backgroundFlushing_average_ms'] =\
                bf['average_ms']
            fields['backgroundFlushing_flushes'] =\
                bf['flushes']
            fields['backgroundFlushing_last_ms'] =\
                bf['last_ms']
            fields['backgroundFlushing_total_ms'] =\
                bf['total_ms']
            con = status['connections']
            fields['connections_available'] =\
                con['available']
            fields['connections_current'] =\
                con['current']
            fields['connections_totalCreated'] =\
                con['totalCreated']
            cur = status['cursors']
            fields['cursors_clientCursors_size'] =\
                cur['clientCursors_size']
            fields['cursors_timedOut'] =\
                cur['timedOut']
            fields['cursors_totalOpen'] =\
                cur['totalOpen']
            dur = status['dur']
            fields['dur_commits'] = dur['commits']
            fields['dur_commitsInWriteLock'] = dur['commitsInWriteLock']
            fields['dur_compression'] = dur['compression']
            fields['dur_earlyCommits'] = dur['earlyCommits']
            fields['dur_journaledMB'] = dur['journaledMB']
            fields['dur_writeToDataFilesMB'] = dur['writeToDataFilesMB']
            tms = dur['timeMs']
            fields['dur_timeMs_dt'] = tms['dt']
            fields['dur_timeMs_prepLogBuffer'] = tms['prepLogBuffer']
            fields['dur_timeMs_remapPrivateView'] = tms['remapPrivateView']
            fields['dur_timeMs_writeToDataFiles'] = tms['writeToDataFiles']
            fields['dur_timeMs_writeToJournal'] = tms['writeToJournal']
            ei = status['extra_info']
            fields['extra_info_heap_usage_bytes'] = ei['heap_usage_bytes']
            fields['extra_info_page_faults'] = ei['page_faults']
            gl = status['globalLock']
            fields['globalLock_activeClients_readers'] =\
                gl['activeClients']['readers']
            fields['globalLock_activeClients_writers'] =\
                gl['activeClients']['writers']
            fields['globalLock_activeClients_total'] =\
                gl['activeClients']['total']
            fields['globalLock_currentQueue_readers'] =\
                gl['currentQueue']['readers']
            fields['globalLock_currentQueue_writers'] =\
                gl['currentQueue']['writers']
            fields['globalLock_currentQueue_total'] =\
                gl['currentQueue']['total']
            fields['globalLock_lockTime'] =\
                gl['lockTime']
            fields['globalLock_totalTime'] =\
                gl['totalTime']
            ic = status['indexCounters']
            fields['indexCounters_accesses'] = ic['accesses']
            fields['indexCounters_hits'] = ic['hits']
            fields['indexCounters_missRatio'] = ic['missRatio']
            fields['indexCounters_misses'] = ic['misses']
            fields['indexCounters_resets'] = ic['resets']
            mem = status['mem']
            fields['mem_mapped'] = mem['mapped']
            fields['mem_mappedWithJournal'] = mem['mappedWithJournal']
            fields['mem_resident'] = mem['resident']
            fields['mem_virtual'] = mem['virtual']
            m = status['metrics']
            fields['metrics_document_deleted'] =\
                m['document']['deleted']
            fields['metrics_document_inserted'] =\
                m['document']['inserted']
            fields['metrics_document_returned'] =\
                m['document']['returned']
            fields['metrics_document_updated'] =\
                m['document']['updated']
            fields['metrics_operation_fastmod'] =\
                m['operation']['fastmod']
            fields['metrics_operation_idhack'] =\
                m['operation']['idhack']
            fields['metrics_operation_scanAndOrder'] =\
                m['operation']['scanAndOrder']
            fields['metrics_queryExecutor_scanned'] =\
                m['queryExecutor']['scanned']
            fields['metrics_record_moves'] =\
                m['record']['moves']
            fields['metrics_repl_apply_batches_num'] =\
                m['repl']['apply']['batches']['num']
            fields['metrics_repl_apply_batches_totalMillis'] =\
                m['repl']['apply']['batches']['totalMillis']
            fields['metrics_repl_apply_ops'] =\
                m['repl']['apply']['ops']
            fields['metrics_repl_buffer_count'] =\
                m['repl']['buffer']['count']
            fields['metrics_repl_buffer_maxSizeBytes'] =\
                m['repl']['buffer']['maxSizeBytes']
            fields['metrics_repl_buffer_sizeBytes'] =\
                m['repl']['buffer']['sizeBytes']

            fields['metrics_repl_network_bytes'] =\
                m['repl']['network']['bytes']
            fields['metrics_repl_network_getmores_num'] =\
                m['repl']['network']['getmores']['num']
            fields['metrics_repl_network_getmores_totalMillis'] =\
                m['repl']['network']['getmores']['totalMillis']
            fields['metrics_repl_network_ops'] =\
                m['repl']['network']['ops']
            fields['metrics_repl_network_readersCreated'] =\
                m['repl']['network']['readersCreated']

            fields['metrics_repl_oplog_insert_num'] =\
                m['repl']['oplog']['insert']['num']
            fields['metrics_repl_oplog_insert_totalMillis'] =\
                m['repl']['oplog']['insert']['totalMillis']
            fields['metrics_repl_oplog_insertBytes'] =\
                m['repl']['oplog']['insertBytes']

            fields['metrics_repl_preload_docs_num'] =\
                m['repl']['preload']['docs']['num']
            fields['metrics_repl_preload_docs_totalMillis'] =\
                m['repl']['preload']['docs']['totalMillis']
            fields['metrics_repl_preload_indexes_num'] =\
                m['repl']['preload']['indexes']['num']
            fields['metrics_repl_preload_indexes_totalMillis'] =\
                m['repl']['preload']['indexes']['totalMillis']

            fields['metrics_ttl_deletedDocuments'] =\
                m['ttl']['deletedDocuments']
            fields['metrics_ttl_passes'] =\
                m['ttl']['passes']

            for k, v in status['network'].items():
                field_name = 'network_{}'.format(k)
                fields[field_name] = v

            for counters in ('opcounters', 'opcountersRepl'):
                op = status[counters]
                for k, v in op.items():
                    field_name = '{}_{}'.format(counters, k)
                    fields[field_name] = v
            rs = status['recordStats']
            fields['recordStats_accessesNotInMemory'] =\
                rs['accessesNotInMemory']
            fields['recordStats_pageFaultExceptionsThrown'] =\
                rs['pageFaultExceptionsThrown']

            logger.debug('points: %s', points)
            agent.push(points, db_config['name'])
        except:
            logger.exception('cannot gather stats from redis')
