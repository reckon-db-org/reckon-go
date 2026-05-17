# Changelog

All notable changes to `reckon-go` will be documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning: [SemVer](https://semver.org/) at the Go-API level.

## [Unreleased]

## [0.1.0] - 2026-05-17

### Added — Scaffold

- Top-level `reckon.Client` facade with `Connect()` / `Close()` / `Conn()`
- Module path `codeberg.org/reckon-db-org/reckon-go`, package name `reckon`
- Insecure-transport default; TLS + capability-token auth are follow-ups
- Service wrappers are stubs — they land incrementally starting with `Stores`
- `genproto/gatewayv1/` reserved for generated bindings (committed, not built)

Compatible with `reckon-proto 0.1.x` / `reckon-gateway 0.4.x` / `reckon-db 2.2.x`.
