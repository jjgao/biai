# CLAUDE.md - Project Instructions for Claude Code

This file extends `CORE.md`. Read `CORE.md` first for shared project instructions. Use this file only for Claude-specific additions or overrides.

## Claude-Specific Guidance

- For research/exploration requests, use the Explore agent.

## Additional Known Issues & Technical Debt

1. **SQL Injection Risk** (#88) - `aggregationService.ts` uses string concatenation

## Additional Open GitHub Issues (Key)

- #88 - SQL injection fix (security, high priority)
