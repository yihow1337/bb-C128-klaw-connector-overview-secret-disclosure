## Title
[Klaw] Cross-team Kafka Connect credential disclosure via GET /getConnectorOverview + default Jasypt key

## VRT
P2 Sensitive Data Exposure → Credentials (or Broken Access Control → Privilege Escalation / IDOR leading to credential disclosure)

## Description
`GET /getConnectorOverview` is gated only by `VIEW_CONNECTORS` and returns `connectorConfig` (including Jasypt `ENC(...)` ciphertext for connector passwords) for connectors owned by **other teams**. Coral shows this config to all viewers; only promotion UI is owner-gated. Shipped `application.properties` sets `klaw.jasypt.encryptor.secretkey=kw2021secretkey`, so default installs allow offline decryption of foreign-team Connect secrets. The edit endpoint correctly team-gates before decrypt — overview does not.

Affected: Aiven-Open/klaw @ f1c24b9 (v2.10.6-1) and current main as of review.
Distinct from CVE-2026-45080 (user password hash endpoint) and from prior SA IDOR / updateUser reports.

## Steps to reproduce (local / synthetic)
1. Two teams in one tenant; Team A owns a connector whose password fields were saved via Klaw (Jasypt ENC).
2. User on Team B with VIEW_CONNECTORS calls `GET /getConnectorOverview?connectornamesearch=<A's connector>`.
3. Response includes `connectorConfig` with `ENC(...)`.
4. Decrypt with BasicTextEncryptor password `kw2021secretkey` (default) → plaintext Connect secrets.
5. Same user calling edit/details-per-env for that connector is refused (team ownership).

Automated: see attached JUnit EXP/NEG (surefire 2/2 PASS).

## Impact
Any VIEW_CONNECTORS user can obtain other teams' Kafka Connect credentials on default-key deployments → access to backends those connectors use.

## Fix
Gate or redact `connectorConfig` on overview by team ownership; remove hard-coded default Jasypt key; rotate secrets encrypted under the default.
