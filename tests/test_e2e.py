#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""端到端回归: amqtt broker(进程内) + PHM审计云(协议胶水) + OSD模拟源 → 告警断言。

13场景×22断言:
  v1(RC档): normal零告警 / v_burst速度ALERT / accel_seq加速度ALERT(fpv档) /
            roll_burst姿态ALERT / spike单帧只WARN不ALERT / dup+jump只出L1质量告警
  v2(行业机档): dock_normal零告警 / 电池骤降+迫降线ALERT+RTH_WARN / GPS丢星ALERT /
            风载WARN / RemoteID监管WARN / 电子围栏越界ALERT+接近WARN
  第三层: 飞行哈希链全链验证+篡改检测
  v2接口: REST(/stats /devices /alerts /chain/verify /config) + webhook推送
产出: results/e2e_result.json + sha256审计链。
"""
import asyncio
import hashlib
import json
import os
import sys
import threading
import time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

HTTP_PORT = 18081
WEBHOOK_PORT = 18099
os.environ['PHM_HTTP_PORT'] = str(HTTP_PORT)
os.environ['PHM_WEBHOOK_URL'] = f'http://127.0.0.1:{WEBHOOK_PORT}/hook'  # 须在cloud导入前设置

from sim.osd_simulator import gen_scenario  # noqa: E402
import phm_audit_cloud as cloud  # noqa: E402
import paho.mqtt.client as mqtt  # noqa: E402

BROKER_PORT = 18830
COLLECTED = []          # (topic, rec)
SEEN = set()
BROKER_REF = None
WEBHOOK_INBOX = []


def _start_webhook():
    """进程内webhook接收器: 捕获云推送的L2 ALERT。"""
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get('Content-Length') or 0)
            WEBHOOK_INBOX.append(json.loads(self.rfile.read(n) or b'{}'))
            self.send_response(200)
            self.end_headers()

    srv = HTTPServer(('127.0.0.1', WEBHOOK_PORT), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _start_broker():
    global BROKER_REF
    from amqtt.broker import Broker

    def _run():
        global BROKER_REF
        loop = asyncio.new_event_loop()   # loop必须在线程内创建并绑定
        asyncio.set_event_loop(loop)

        async def _make():                # amqtt0.11: Broker构造必须在运行中的loop内
            BROKER_REF = Broker({'listeners': {'default': {'type': 'tcp', 'bind': f'127.0.0.1:{BROKER_PORT}'}},
                                 'auth': {'allow-anonymous': True}, 'sys_interval': 10})
            await BROKER_REF.start()
        loop.run_until_complete(_make())
        loop.run_forever()
    threading.Thread(target=_run, daemon=True).start()

    import socket
    for _ in range(50):  # 端口就绪探测, 最多10s
        try:
            s = socket.create_connection(('127.0.0.1', BROKER_PORT), timeout=0.4)
            s.close()
            return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError('amqtt broker not listening')


def _mk_collector():
    clc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='collector')

    def on_msg(c, u, m):
        rec = json.loads(m.payload)
        key = (rec['sn'], rec['rule'], rec['ts'])
        if key not in SEEN:  # retain重投去重
            SEEN.add(key)
            COLLECTED.append((m.topic, rec))
    clc.on_message = on_msg
    clc.connect('127.0.0.1', BROKER_PORT, keepalive=60)
    clc.subscribe('phm/audit/+/alerts')
    clc.subscribe('phm/audit/+/quality')
    clc.loop_start()
    return clc


def _mk_cloud():
    clc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='phm-audit-cloud-test')
    clc.on_connect = cloud.on_connect
    clc.on_message = cloud.on_message
    clc.connect('127.0.0.1', BROKER_PORT, keepalive=60)
    clc.loop_start()
    time.sleep(0.5)
    return clc


def run_scenario(simc, sn, name, profile=None):
    if profile:
        cloud.ENGINE.bind_profile(sn, profile)
    simc.publish(f'sys/product/{sn}/status', json.dumps({'data': {'status': 'ok'}}), qos=1)
    seq = gen_scenario(name, sn)
    for dt, m in seq:
        simc.publish(f'thing/product/{sn}/osd', json.dumps(m), qos=1)
        time.sleep(min(dt, 0.02))  # 测试加速: 保留dt排序语义, 不真睡1s
    time.sleep(0.6)


def _clean_chains():
    import glob
    for f in glob.glob(os.path.join(BASE, 'results', 'flight_audit_*.jsonl')):
        os.remove(f)
    for f in glob.glob(os.path.join(BASE, 'results', 'chain_anchor_*.json')):
        os.remove(f)


def main():
    print('== E2E: amqtt broker start ==', flush=True)
    _start_broker()
    _start_webhook()
    cloud.start_http(HTTP_PORT)
    _clean_chains()
    collector = _mk_collector()
    cloudc = _mk_cloud()
    simc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='sim-main')
    simc.connect('127.0.0.1', BROKER_PORT, keepalive=60)
    simc.loop_start()
    time.sleep(0.3)

    runs = [
        ('SN_NORMAL_001', 'normal', None),
        ('SN_VBURST_002', 'v_burst', None),
        ('SN_ACCEL_003', 'accel_seq', 'fpv'),
        ('SN_ROLL_004', 'roll_burst', None),
        ('SN_SPIKE_005', 'spike', None),
        ('SN_DUP_006', 'dup', None),
        ('SN_JUMP_007', 'jump', None),
        ('SN_DOCK_008', 'dock_normal', 'dock'),
        ('SN_BATT_009', 'batt_drop', 'dock'),
        ('SN_GPS_010', 'gps_loss', 'dock'),
        ('SN_WIND_011', 'wind_high', 'dock'),
        ('SN_RID_012', 'rid_off', 'dock'),
    ]
    for sn, name, prof in runs:
        run_scenario(simc, sn, name, prof)

    # ---- v2: REST API配置围栏(走POST /config) → geofence场景 ----
    import urllib.request as _ur
    cfg = {'profile': 'dock', 'geofence': {'type': 'circle', 'lat': 22.8170,
                                           'lon': 108.3660, 'radius_m': 100}}
    req = _ur.Request(f'http://127.0.0.1:{HTTP_PORT}/config/SN_FENCE_013',
                      data=json.dumps(cfg).encode(), headers={'Content-Type': 'application/json'})
    cfg_resp = json.load(_ur.urlopen(req, timeout=5))
    ck_rest_cfg = cfg_resp.get('ok') is True
    run_scenario(simc, 'SN_FENCE_013', 'geofence', None)

    # ---- v2: webhook接收验证(首个L2 ALERT应已推送) ----
    time.sleep(1.0)
    webhook_hits = list(WEBHOOK_INBOX)

    time.sleep(0.8)
    by_sn = {}
    for topic, rec in COLLECTED:
        by_sn.setdefault(rec['sn'], []).append((topic.split('/')[-1], rec['rule'],
                                                rec['level'], rec['caliber']))

    checks = []
    def ck(name, cond, detail=''):
        checks.append({'check': name, 'pass': bool(cond), 'detail': detail})
        print(('PASS ' if cond else 'FAIL ') + name + (f' :: {detail}' if detail else ''), flush=True)

    a = lambda sn: [x for x in by_sn.get(sn, []) if x[0] == 'alerts']
    q = lambda sn: [x for x in by_sn.get(sn, []) if x[0] == 'quality']

    ck('normal_零告警', len(a('SN_NORMAL_001')) == 0, str(a('SN_NORMAL_001')))
    ck('v_burst_速度ALERT确认', any(x[2] == 'ALERT' and x[1] == 'V_SPEED' for x in a('SN_VBURST_002')),
       str(a('SN_VBURST_002')))
    ck('v_burst_首帧WARN', any(x[2] == 'WARN' and x[1] == 'V_SPEED' for x in a('SN_VBURST_002')))
    ck('accel_seq_加速度ALERT(fpv档)', any(x[2] == 'ALERT' and x[1] == 'ACCEL' for x in a('SN_ACCEL_003')),
       str(a('SN_ACCEL_003')))
    ck('roll_burst_姿态ALERT确认', any(x[2] == 'ALERT' and x[1] == 'ATT_ROLL' for x in a('SN_ROLL_004')),
       str(a('SN_ROLL_004')))
    ck('spike_单帧只WARN不ALERT',
       any(x[2] == 'WARN' and x[1] == 'L1_SPEED_SPIKE' for x in by_sn.get('SN_SPIKE_005', []))
       and not any(x[2] == 'ALERT' for x in a('SN_SPIKE_005')),
       str(by_sn.get('SN_SPIKE_005', [])))
    ck('dup_L1质量口径', any(x[1] == 'L1_DUP' and x[3] == 'L1质量' for x in q('SN_DUP_006')),
       str(q('SN_DUP_006')))
    ck('jump_L1质量口径(数据缺陷非物理违规)',
       any(x[1] == 'L1_JUMP' and x[3] == 'L1质量' for x in q('SN_JUMP_007')), str(q('SN_JUMP_007')))

    # ---- 第三层: 飞级行哈希链(数字黑匣子) ----
    import shutil
    chain_path = os.path.join(BASE, 'results', 'flight_audit_SN_VBURST_002.jsonl')
    ok, nch, fin = cloud.verify_chain(chain_path)
    ck('哈希链_全链验证通过', ok and nch >= 2, f'ok={ok} n={nch} final={fin}')
    tam = chain_path + '.tampered'
    shutil.copy(chain_path, tam)
    lines = open(tam).readlines()
    e2 = json.loads(lines[1]); e2['rec']['measured'] = 20.0   # 篡改第2条记录数值
    lines[1] = json.dumps(e2, ensure_ascii=False) + '\n'      # 注意补换行(丢行尾会与下一条拼接)
    open(tam, 'w').writelines(lines)
    ok2, _, _ = cloud.verify_chain(tam)
    ck('哈希链_篡改检测生效', ok2 is False, f'tampered_ok={ok2}')
    os.remove(tam)

    # ---- v3.1: 全256位 + 帧级入链 + 头部外锚 + 重启续链 ----
    _laste = json.loads(open(chain_path, encoding='utf-8').readlines()[-1])
    ck('哈希链_全256位', len(_laste['hash']) == 64, f"hash_len={len(_laste['hash'])}")
    ck('帧级入链_链记录数≥帧数', _laste['n'] >= 30, f"chain_n={_laste['n']}")
    _anchor = json.load(open(os.path.join(BASE, 'results', 'chain_anchor_SN_VBURST_002.json')))
    ck('头部外锚_锚文件与链尾一致', _anchor['head'] == _laste['hash'] and _anchor['n'] == _laste['n'],
       str(_anchor)[:120])
    # 全文件一致性重写攻击: 攻击者篡改所有数值并重算整条链(本地验证可通过) → 外部锚点戳穿
    import hashlib as _hl
    _rw = chain_path + '.rewritten'
    _rls = open(chain_path, encoding='utf-8').readlines()
    _prev = 'GENESIS'; _newlines = []
    for _ln in _rls:
        _e = json.loads(_ln)
        _e['rec']['measured'] = 0.0
        _blob = json.dumps(_e['rec'], sort_keys=True, ensure_ascii=False, default=str)
        _h = _hl.sha256((_prev + _blob).encode()).hexdigest()[:len(_e['hash'])]
        _e['prev'] = _prev; _e['hash'] = _h; _prev = _h
        _newlines.append(json.dumps(_e, ensure_ascii=False) + '\n')
    open(_rw, 'w', encoding='utf-8').writelines(_newlines)
    _ok_rw, _, _fin_rw = cloud.verify_chain(_rw)
    ck('全文件重写_本地验证被攻破(如实暴露链的局限)', _ok_rw is True, f'local_ok={_ok_rw}')
    ck('全文件重写_外部锚点戳穿', _fin_rw != _anchor['head'], 'anchor catches rewrite')
    os.remove(_rw)
    # 重启安全续链: 清空内存链状态 → 新append必须从文件尾续链不断裂
    cloud.FLIGHT_CHAINS.clear()
    _n_before = _laste['n']
    cloud.chain_append('SN_VBURST_002', {'rule': 'RESTART_TEST', 'level': 'FRAME', 'ts': 0})
    _ok_rs, _n_rs, _fin_rs = cloud.verify_chain(chain_path)
    ck('重启安全续链', _ok_rs and _n_rs == _n_before + 1, f"n={_n_rs} ok={_ok_rs}")

    ck('总OSD帧数>150', cloud.STATE['n_osd'] > 150, f"n_osd={cloud.STATE['n_osd']}")

    # ---- v2: dock/行业机新规则 ----
    ck('dock_normal_零L2告警', not any(x[2] == 'ALERT' for x in a('SN_DOCK_008')),
       str(a('SN_DOCK_008')))
    ck('batt_drop_骤降ALERT+迫降线ALERT+RTH_WARN',
       any(x[1] == 'BATT_DROP' and x[2] == 'ALERT' for x in a('SN_BATT_009'))
       and any(x[1] == 'BATT_LAND' and x[2] == 'ALERT' for x in a('SN_BATT_009'))
       and any(x[1] == 'BATT_RTH' and x[2] == 'WARN' for x in a('SN_BATT_009')),
       str(a('SN_BATT_009')))
    ck('gps_loss_丢星ALERT+弱星WARN',
       any(x[1] == 'GPS_LOSS' and x[2] == 'ALERT' for x in a('SN_GPS_010'))
       and any(x[1] == 'GPS_WEAK' for x in q('SN_GPS_010')),
       str(a('SN_GPS_010')) + str(q('SN_GPS_010')))
    ck('wind_high_风载WARN', any(x[1] == 'WIND_HIGH' for x in a('SN_WIND_011')),
       str(a('SN_WIND_011')))
    ck('rid_off_RemoteID监管WARN', any(x[1] == 'RID_OFF' for x in a('SN_RID_012')),
       str(a('SN_RID_012')))
    ck('geofence_越界ALERT+接近WARN',
       any(x[1] == 'GEOFENCE' and x[2] == 'ALERT' for x in a('SN_FENCE_013'))
       and any(x[1] == 'GEOFENCE_NEAR' for x in a('SN_FENCE_013')),
       str(a('SN_FENCE_013')))

    # ---- v2: REST API ----
    def _get(p):
        return json.load(_ur.urlopen(f'http://127.0.0.1:{HTTP_PORT}{p}', timeout=5))
    st = _get('/stats')
    ck('REST_stats_帧数与告警可见', st['state']['n_osd'] > 150 and st['state']['n_alerts'] > 0,
       f"n_osd={st['state']['n_osd']} n_alerts={st['state']['n_alerts']}")
    dv = _get('/devices')
    ck('REST_devices_在线SN列表', 'SN_DOCK_008' in dv['online'] and 'SN_FENCE_013' in dv['online'],
       str(dv['online']))
    al = _get('/alerts?sn=SN_VBURST_002')
    ck('REST_alerts_按sn查询', al['n'] >= 2 and al['rows'][-1]['rule'] == 'V_SPEED',
       f"n={al['n']}")
    cv = _get('/chain/SN_VBURST_002/verify')
    ck('REST_chain_verify_通过', cv['ok'] is True and cv['records'] >= 2, str(cv))
    ck('REST_config_围栏配置生效', ck_rest_cfg, str(cfg_resp))
    ck('webhook_L2告警已推送', len(webhook_hits) >= 1
       and any(h.get('rule') == 'V_SPEED' and h.get('level') == 'ALERT' for h in webhook_hits),
       f"hits={len(webhook_hits)} sample={webhook_hits[:1]}")

    n_pass = sum(1 for c in checks if c['pass'])
    blob = json.dumps({'checks': checks, 'n_osd': cloud.STATE['n_osd'],
                       'collected': COLLECTED}, sort_keys=True, ensure_ascii=False)
    out = {'verdict': 'PASS' if n_pass == len(checks) else 'FAIL',
           'n_pass': n_pass, 'n_total': len(checks),
           'n_osd': cloud.STATE['n_osd'], 'n_alert_records': len(COLLECTED),
           'engine': type(cloud.ENGINE).__name__,
           'checks': checks, 'ts': time.strftime('%F %T')}
    out['audit_sha256'] = hashlib.sha256(blob.encode()).hexdigest()[:16]
    path = os.path.join(BASE, 'results', 'e2e_result.json')
    json.dump(out, open(path, 'w'), ensure_ascii=False, indent=1)
    print(json.dumps({k: out[k] for k in ('verdict', 'n_pass', 'n_total', 'n_osd', 'audit_sha256')}),
          flush=True)
    simc.loop_stop(); cloudc.loop_stop(); collector.loop_stop()
    return 0 if out['verdict'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
