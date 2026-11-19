# dji-flightqc

**Deterministic physics/bounds QC for DJI Cloud API OSD telemetry streams.**

A zero-dependency Python implementation that plugs into the DJI Cloud API architecture (third-party cloud role): pilot-side devices publish OSD telemetry over MQTT, and this service issues per-frame physics/bounds verdicts with a replayable SHA-256 audit chain and a three-state exit model.

## What it does

- **Login handshake aligned with the official DJI-Cloud-API-Demo semantics** (`LoginController` / `UserServiceImpl` / `JwtUtil`): pilot → third-party cloud login returns `access_token` + `mqtt_addr` + `workspace_id`. JWT is a stdlib HS256 implementation (verified by independent recomputation against the official demo).
- **Per-frame deterministic checks on OSD telemetry** (L2 physics caliber): vertical speed burst, acceleration (FPV profile), attitude roll burst, battery drop / forced-landing / RTH lines, GPS loss / weak, wind load, Remote ID off, geofence near / breach.
- **Data-quality checks (L1 caliber)**: single-frame spikes, duplicate frames, frame jumps — defects flagged as data issues, not physics violations (caliber separation).
- **Three-state exit**: WARN → ALERT (per rule thresholds), all records appended to a **SHA-256 hash chain** with external anchor files; restart-safe chain continuation; tamper detection verified (full-file rewrite is honestly reported as out of scope for local verification — external anchor catches it).
- **REST surface**: `/stats`, `/devices`, `/alerts?sn=`, `/chain/{sn}/verify` (POST), `/config/{sn}` (live geofence/profile update).
- **Webhook fan-out** on L2 ALERTs (env `PHM_WEBHOOK_URL`).

## Verification (re-run 2026-11-19)

`tests/test_e2e.py` — **29/29 PASS** across 13 SN scenarios (normal, vertical burst, acceleration, roll, spike, duplicate, jump, dock, battery, GPS loss, wind, Remote ID, geofence), 334 OSD frames, MQTT broker loop via `amqtt`:

- hash chain: 32 records verified, tamper detection active, external anchor match, restart continuation (n=33)
- REST: stats/devices/alerts/chain-verify/config all green
- webhook: 7 L2 alerts delivered with chain head attached

Result file: `results/e2e_result.json` (audit sha256 prefix `2ba226827181d28e`).

## Run

```bash
pip install paho-mqtt amqtt
python tests/test_e2e.py
```

Credentials are read from environment (`DJI_APP_KEY`, `DJI_APP_LICENSE`, `PHM_WEBHOOK_URL`); none are hardcoded. The synthetic scenario set contains no flight data from real operators.

## Positioning

Complementary to the official DJI-Cloud-API-Demo (Java): this repo is the deterministic QC layer a third-party cloud can attach behind the OSD topic subscription. Same input, same verdict; every verdict carries a replayable audit chain.
