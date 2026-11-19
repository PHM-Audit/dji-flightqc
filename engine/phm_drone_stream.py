# -*- coding: utf-8 -*-
"""
PHM-Drone 流式校验引擎 v2 —— 【闭源模块, 不在开源包内分发】
            ↑ 本文件含PHM核心判定逻辑, GitHub开源包中替换为interface.NullEngine

v2新增(2026-10-04):
  · dock档位: 机场/行业机OSD(OsdDockDrone 37字段, M300/350/M30系列+Dock)
  · 电子围栏: per-SN圆形围栏配置, 越界单帧即ALERT(硬边界无多帧确认), 靠近50m内WARN
  · 电池:     capacityPercent <= returnHomePower→WARN(应返航) / <= landingPower→ALERT(迫降线)
              骤降: 单帧掉电>3%且连续2帧→ALERT
  · GPS:      gpsNumber<=5→WARN, 连续2帧<=3→ALERT(丢星)
  · 风载:     windSpeed>档位wind_max连续2帧→WARN(环境告警,建议返航,不判违规)
  · RemoteID: ridState=False飞行中(dock档)→WARN(监管广播缺失)
v1继承: 机型化四档(tello/mavic/agras/fpv) + L1质量层(dup/gap/jump/speed_spike,
         位置-速度一致性闸门) + L2物理层多帧确认
"""
import math

PROFILES = {
    # 档位:   vh(m/s) vv(m/s) a(m/s²) roll° pitch° jump倍率 wind_max(m/s)
    'tello':  dict(vh=8.0,  vv=3.0,  a=25.0, roll=40.0, pitch=40.0, jump=8.0,  wind=8.0),
    'mavic':  dict(vh=21.0, vv=8.0,  a=35.0, roll=45.0, pitch=45.0, jump=10.0, wind=12.0),
    'agras':  dict(vh=10.0, vv=6.0,  a=15.0, roll=35.0, pitch=35.0, jump=6.0,  wind=10.0),
    'fpv':    dict(vh=60.0, vv=20.0, a=70.0, roll=80.0, pitch=80.0, jump=12.0, wind=15.0),
    'dock':   dict(vh=23.0, vv=6.0,  a=35.0, roll=45.0, pitch=45.0, jump=10.0, wind=12.0),
    # v2.2(2026-10-06) 车辆档: 公路场景(NGSIM I-80实测校准)
    # vh=40m/s(144km/h): I-80车流物理上限之上(实测合法车流峰值<35m/s), 超限=数据缺陷检出
    # vv/roll/pitch/jump/wind=999: 地面车辆无对应量纲, 实测数据亦不喂这些字段, 设不触发
    'highway': dict(vh=40.0, vv=999.0, a=999.0, roll=999.0, pitch=999.0, jump=999.0, wind=999.0),
}

FENCE_MARGIN_M = 50.0     # 围栏接近预警距离
FENCE_WARN_ONCE = True    # 接近预警只发一次(回安全区后复位)
BATT_DROP_PCT = 3.0       # 单帧掉电百分比(骤降候选)
BATT_DROP_CONFIRM = 2     # 骤降确认帧数
GPS_WARN_N = 5            # 卫星数<=5 → WARN
GPS_ALERT_N = 3           # 连续2帧<=3 → ALERT
WIND_CONFIRM = 2
RTH_DEFAULT, LAND_DEFAULT = 30, 10   # OSD无returnHomePower/landingPower时兜底


def _haversine(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


class _SnState:
    __slots__ = ('last_ts', 'last_vh', 'last_clean_vh', 'dts', 'consec', 'fired',
                 'last_lat', 'last_lon', 'last_cap', 'fence', 'fence_warned')

    def __init__(self):
        self.last_ts = None
        self.last_vh = None
        self.last_clean_vh = None   # 最近的非脏帧速度(加速度跨脏帧不连续)
        self.dts = []               # 最近dt窗口(ms)
        self.consec = {}            # rule -> 连续命中帧数
        self.fired = set()          # 已发ALERT未复位的rule
        self.last_lat = None
        self.last_lon = None
        self.last_cap = None        # 上一帧电量%
        self.fence = None           # {'type':'circle','lat','lon','radius_m'}
        self.fence_warned = False


class PHMDroneStreamEngine:
    """流式引擎: 每帧OSD一次on_osd调用, 返回当帧触发的告警。版本v2。"""

    __version__ = 'phm-drone-stream/2.0'

    def __init__(self, profiles=None, confirm=None):
        self.profiles = dict(PROFILES)
        if profiles:
            self.profiles.update(profiles)
        self.confirm = confirm or {'V_SPEED': 2, 'V_SPEED_V': 2, 'ACCEL': 2,
                                   'ATT_ROLL': 3, 'ATT_PITCH': 3,
                                   'BATT_DROP': BATT_DROP_CONFIRM,
                                   'GPS_LOSS': 2, 'WIND_HIGH': WIND_CONFIRM}
        self._sns = {}
        self.sn_profiles = {}

    # ---- 接口实现 ----
    def profile_for(self, sn, osd):
        return self.sn_profiles.get(sn, 'mavic')

    def bind_profile(self, sn, profile):
        assert profile in self.profiles, f'unknown profile {profile}'
        self.sn_profiles[sn] = profile

    def on_status(self, sn, online):
        return None  # 接口兼容: 设备上下线通知(引擎无跨帧动作)

    def configure(self, sn, geofence=None, profile=None):
        """per-SN运行时配置: 电子围栏/档位。REST /config/{sn} 直通此接口。"""
        if profile:
            self.bind_profile(sn, profile)
        st = self._state(sn)
        if geofence:
            assert geofence.get('type') == 'circle', 'v2仅支持circle围栏'
            st.fence = geofence
            st.fence_warned = False

    def _state(self, sn):
        if sn not in self._sns:
            self._sns[sn] = _SnState()
        return self._sns[sn]

    def on_osd(self, sn, ts_ms, osd):
        out = []
        st = self._state(sn)
        prof = self.profiles[self.profile_for(sn, osd)]
        vh = abs(float(osd.get('horizontalSpeed') or 0.0))
        vv = abs(float(osd.get('verticalSpeed') or 0.0))
        roll = abs(float(osd.get('attitudeRoll') or 0.0))
        pitch = abs(float(osd.get('attitudePitch') or 0.0))
        lat, lon = osd.get('latitude'), osd.get('longitude')
        batt = osd.get('battery') or {}
        cap = batt.get('capacityPercent')
        pos = osd.get('positionState') or {}
        gps_n = pos.get('gpsNumber')
        wind = osd.get('windSpeed')

        # ---- L1 数据质量层 ----
        dt = None
        dirty = False
        if st.last_ts is not None:
            dt = ts_ms - st.last_ts
            self.dts_push(st, dt)
            med = self.median_dt(st)
            if dt < 20:
                out.append(self._rec('L1_DUP', 'L1质量', dt, med, 1, ts_ms, 'WARN'))
            elif dt > 5 * med:
                out.append(self._rec('L1_GAP', 'L1质量', dt, med, 1, ts_ms, 'WARN'))
            if lat is not None and st.last_lat is not None and dt > 0:
                d = _haversine(st.last_lat, st.last_lon, lat, lon)
                jump_lim = prof['jump'] * prof['vh'] * (dt / 1000.0) + 5.0
                if d > jump_lim:
                    out.append(self._rec('L1_JUMP', 'L1质量', round(d, 1),
                                         round(jump_lim, 1), 1, ts_ms, 'WARN'))
                pos_v = d / (dt / 1000.0)
                if vh > prof['vh'] * 1.5 and pos_v < vh * 0.5:
                    dirty = True
                    out.append(self._rec('L1_SPEED_SPIKE', 'L1质量',
                                         round(vh, 1), round(pos_v, 1), 1, ts_ms, 'WARN'))
        if lat is not None:
            st.last_lat, st.last_lon = lat, lon

        # ---- L2 物理层 (多帧确认; 脏帧重置连续性) ----
        out += self._stream_check(st, 'V_SPEED', (vh > prof['vh']) and not dirty,
                                  vh, prof['vh'], ts_ms)
        out += self._stream_check(st, 'V_SPEED_V', vv > prof['vv'], vv, prof['vv'], ts_ms)
        if dt is not None and dt > 100 and st.last_clean_vh is not None and not dirty:
            accel = abs(vh - st.last_clean_vh) / (dt / 1000.0)
            out += self._stream_check(st, 'ACCEL', accel > prof['a'], accel, prof['a'], ts_ms)
        else:
            st.consec.pop('ACCEL', None)
        out += self._stream_check(st, 'ATT_ROLL', roll > prof['roll'], roll, prof['roll'], ts_ms)
        out += self._stream_check(st, 'ATT_PITCH', pitch > prof['pitch'], pitch, prof['pitch'], ts_ms)

        # ---- 电子围栏 (硬边界, 单帧即判) ----
        if st.fence and lat is not None and lon is not None:
            d = _haversine(st.fence['lat'], st.fence['lon'], lat, lon)
            if d > st.fence['radius_m']:
                if 'GEOFENCE' not in st.fired:
                    st.fired.add('GEOFENCE')
                    out.append(self._rec('GEOFENCE', 'L2物理', round(d, 1),
                                         st.fence['radius_m'], 1, ts_ms, 'ALERT'))
            elif d > st.fence['radius_m'] - FENCE_MARGIN_M:
                if not st.fence_warned:
                    st.fence_warned = True
                    out.append(self._rec('GEOFENCE_NEAR', 'L2物理', round(d, 1),
                                         st.fence['radius_m'], 1, ts_ms, 'WARN'))
            else:
                st.fence_warned = False          # 回到安全区, 接近预警复位
                st.fired.discard('GEOFENCE')

        # ---- 电池 ----
        if cap is not None:
            rth = batt.get('returnHomePower') or RTH_DEFAULT
            land = batt.get('landingPower') or LAND_DEFAULT
            if cap <= land:
                out += self._stream_check(st, 'BATT_LAND', True, cap, land, ts_ms)
            elif cap <= rth:
                out += self._stream_check(st, 'BATT_RTH', True, cap, rth, ts_ms)
            if st.last_cap is not None and st.last_cap - cap >= BATT_DROP_PCT and dt and dt > 0:
                drop_rate = (st.last_cap - cap) / (dt / 1000.0)   # %/s
                out += self._stream_check(st, 'BATT_DROP', drop_rate >= BATT_DROP_PCT,
                                          round(drop_rate, 2), BATT_DROP_PCT, ts_ms)
            st.last_cap = cap

        # ---- GPS ----
        if gps_n is not None:
            out += self._stream_check(st, 'GPS_LOSS', gps_n <= GPS_ALERT_N,
                                      gps_n, GPS_ALERT_N, ts_ms)
            if gps_n <= GPS_WARN_N and gps_n > GPS_ALERT_N:
                if st.consec.get('GPS_WARN', 0) == 0:
                    st.consec['GPS_WARN'] = 1
                    out.append(self._rec('GPS_WEAK', 'L1质量', gps_n, GPS_WARN_N, 1, ts_ms, 'WARN'))
            else:
                st.consec['GPS_WARN'] = 0

        # ---- 风载 (环境告警, 只WARN) ----
        if wind is not None:
            out += self._stream_check(st, 'WIND_HIGH', wind > prof['wind'],
                                      wind, prof['wind'], ts_ms, warn_only=True)

        # ---- RemoteID (dock档监管广播) ----
        rid = osd.get('ridState')
        if rid is False and self.profile_for(sn, osd) == 'dock':
            if st.consec.get('RID_OFF', 0) == 0:
                st.consec['RID_OFF'] = 1
                out.append(self._rec('RID_OFF', 'L2物理', 0, 1, 1, ts_ms, 'WARN'))
        elif rid is True:
            st.consec['RID_OFF'] = 0

        st.last_ts, st.last_vh = ts_ms, vh
        if not dirty:
            st.last_clean_vh = vh
        return out

    # ---- 内部 ----
    def _stream_check(self, st, rule, hit, measured, limit, ts, warn_only=False):
        if not hit:
            st.consec.pop(rule, None)
            st.fired.discard(rule)
            return []
        n = st.consec.get(rule, 0) + 1
        st.consec[rule] = n
        need = self.confirm.get(rule, 2)
        level = 'WARN' if (warn_only or n < need) else 'ALERT'
        if n == 1 or (n >= need and rule not in st.fired):
            if n >= need:
                st.fired.add(rule)
            return [self._rec(rule, 'L2物理', round(measured, 2), limit, n, ts, level)]
        return []

    @staticmethod
    def _rec(rule, caliber, measured, limit, frames, ts, level):
        return {'rule': rule, 'level': level, 'caliber': caliber,
                'measured': measured, 'limit': limit,
                'frames_confirmed': frames, 'ts': ts}

    @staticmethod
    def dts_push(st, dt):
        st.dts.append(dt)
        if len(st.dts) > 20:
            st.dts.pop(0)

    @staticmethod
    def median_dt(st):
        if not st.dts:
            return 1000.0
        s = sorted(st.dts)
        return s[len(s) // 2]
