# C128 — Klaw: Cross-team Kafka Connect secret disclosure via getConnectorOverview + default Jasypt key

**Status:** CONFIRMED locally. **NOT submitted** (await user approval; Aiven Bugcrowd weekly submit limit).
**Platform:** Bugcrowd — Aiven (aiven-mbb-og). Prefer non-H1; H1 KYC pending N/A here.
**Target:** Aiven-Open/klaw @ f1c24b9d3c8a079dba0a54e7713a09fdd43a974b (v2.10.6-1)
**Severity (proposed):** High / Bugcrowd VRT P2 — Sensitive Data Exposure (credentials) / Broken Access Control
**CWE:** CWE-639 (IDOR), CWE-863 (Incorrect Authorization), CWE-200 (Information Exposure), CWE-321 (Use of Hard-coded Cryptographic Key)

## Summary

`GET /getConnectorOverview` (permission: `VIEW_CONNECTORS` only) returns each connector's stored `connectorConfig` JSON — including Jasypt `ENC(...)` ciphertext for passwords and similar fields — to **any** authenticated user who can view connectors, with **no team-ownership check**.

Coral UI (`ConnectorOverview.tsx`) renders that config in a read-only Monaco editor for **all** viewers; only the promotion banner is owner-gated. The product therefore treats Jasypt encryption as the barrier that keeps Connect secrets safe when non-owners view overview.

Klaw ships with a **hard-coded default** in `application.properties`:

```
klaw.jasypt.encryptor.secretkey=kw2021secretkey
```

Any attacker who retains the default (common for self-hosted / quickstart / misconfigured tenants) can offline-decrypt foreign-team Connect credentials (JDBC passwords, API tokens, etc.) obtained from overview.

By contrast, `getConnectorDetailsPerEnvToEdit` correctly refuses foreign-team connectors (`KAFKA_CONNECT_ERR_119` / "Sorry, your team does not own the connector !!") **before** decrypting via `updateJsonNode("decrypt", ...)`.

This is a different root cause from C121 (unbound Aiven SA `userName`) and from GHSA-v7m7-fr8v-hpx2 / CVE-2026-45080 (user password-hash disclosure endpoint, patched ≤2.10.3).

## Root cause

In `KafkaConnectControllerService.getConnectorOverview`:

```java
connectorInfo.setConnectorConfig(conn.getConnectorConfig());  // no team gate
connectorInfo.setConnectorOwner(Objects.equals(connectorInfo.getTeamId(), loggedInUserTeam));
// owner flag only gates UI actions; config still returned
```

Controller:

```java
@PermissionAllowed(permissionAllowed = {PermissionType.VIEW_CONNECTORS})
@RequestMapping(value = "/getConnectorOverview", method = RequestMethod.GET, ...)
```

## Impact

Cross-team disclosure of Kafka Connect configuration secrets → credential reuse against databases, cloud APIs, or Connect workers depending on what operators stored in connector configs. Default Jasypt key makes ciphertext trivially recoverable on default installs.

## Preconditions

1. Authenticated Klaw user with `VIEW_CONNECTORS`.
2. Knowledge of (or ability to enumerate) a foreign team's connector name (list endpoints / UI).
3. Victim connector config fields encrypted with Jasypt (as Klaw does on save for sensitive keys).
4. For full plaintext recovery: tenant still using shipped default `kw2021secretkey` (or any key the attacker learns). Ciphertext leak alone is still unauthorized secret material.

## Local PoC (read-only synthetic)

JUnit in `KafkaConnectControllerServiceTest`:

- **EXP** `C128_getConnectorOverview_foreignTeam_leaksJasyptEncryptedSecrets`: attacker teamId=99, victim teamId=8; overview returns `ENC(...)`; decrypt with `kw2021secretkey` recovers plaintext.
- **NEG** `C128_getConnectorDetailsPerEnvToEdit_foreignTeam_stillBlocked`: edit path still throws team ownership error.

```bash
./poc_e2e.sh
# surefire: Tests run: 2, Failures: 0, Errors: 0
```

## Suggested fix

1. In `getConnectorOverview`, omit `connectorConfig` (or strip/redact sensitive keys) unless `connectorOwner` / team match; or require the same team check as `getConnectorDetailsPerEnvToEdit` before returning config.
2. Do **not** ship a default Jasypt secretkey; require operators to set a strong unique key at install (fail closed if unset).
3. Rotate any secrets previously encrypted under `kw2021secretkey`.

## Novelty

- No GHSA/CVE for connector-overview / default-jasypt class (checked 2026-11-02).
- Existing advisories: GHSA-rp26-qv9w-xr5q resetMemoryCache; GHSA-v7m7-fr8v-hpx2 pwd-hash; GHSA-75rx-m9vh-mm9p case-lockout — unrelated.
- Public issues #1404 / #1522 are frontend/OpenAPI shape, not authz.
- Default key is visible in the public repo (documentation of CWE-321), but cross-team overview leak of encrypted Connect secrets is not documented as accepted risk.

## Non-goals / out of scope for this package

No production probing, no Bugcrowd submit this week without explicit user approval, no public issue/PR.
