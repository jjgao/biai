# CLAUDE.md - Project Instructions for Claude Code

## Project Overview

**BIAI (Business Intelligence AI)** is a web-based data exploration and visualization tool built for interactive analysis of multi-table datasets. It's designed primarily for biomedical/clinical data but supports any CSV/TSV datasets.

### Tech Stack
- **Frontend**: React 18 + Vite + TypeScript (port 3000)
- **Backend**: Node.js + Express + TypeScript (port 5001)
- **Database**: ClickHouse (port 8123)
- **Visualization**: Plotly.js, Recharts
- **Testing**: Vitest

## Project Structure

```
biai/
├── client/                  # React frontend
│   └── src/
│       ├── pages/          # Main views (DatasetExplorer, DatasetManage, etc.)
│       ├── components/     # Reusable UI components
│       ├── services/       # API client (api.ts)
│       ├── utils/          # Filter helpers, state management
│       └── types/          # TypeScript definitions
├── server/                  # Node.js backend
│   └── src/
│       ├── routes/         # API endpoints
│       ├── services/       # Business logic
│       │   ├── aggregationService.ts  # SQL generation, filtering
│       │   ├── datasetService.ts      # Dataset CRUD
│       │   ├── fileParser.ts          # CSV parsing
│       │   └── columnAnalyzer.ts      # Type inference
│       ├── utils/          # Helpers (listParser, etc.)
│       └── config/         # ClickHouse connection
├── clickhouse/             # Database setup
│   ├── init/              # Schema initialization
│   └── migrations/        # Schema updates
├── docs/                   # User documentation
└── example_data/           # Sample datasets with .meta files
```

## Development Commands

```bash
# Start everything (frontend + backend)
npm run dev

# Start individual services
npm run dev:client    # Frontend only (port 3000)
npm run dev:server    # Backend only (port 5001)

# Start ClickHouse
docker-compose up -d clickhouse

# Run tests
cd server && npm test           # Backend tests
cd client && npm test           # Frontend tests
npm test                        # All tests from root

# Build
npm run build
```

## Key Concepts

### 1. Datasets & Tables
- A **dataset** contains multiple related **tables**
- Tables have **relationships** (foreign keys) enabling cross-table queries
- Metadata stored in ClickHouse: `datasets_metadata`, `dataset_tables`, `dataset_columns`, `table_relationships`

### 2. Filtering System
- Filters support `eq`, `in`, `gt`, `lt`, `between`, `contains`, `not` operators
- Filters propagate across related tables automatically
- URL hash encodes filters for sharing
- Filter presets can be saved/loaded/exported

### 3. List/Array Columns
- CSV columns can contain Python lists `['a', 'b']` or JSON arrays `["a", "b"]`
- Stored as ClickHouse `Array(String)` type
- Individual items shown in pie charts
- Filter by individual items with OR logic

### 4. Aggregation & Visualization
- `aggregationService.ts` builds SQL queries dynamically
- Supports ARRAY JOIN for list columns
- Parent table counting (count distinct parents)
- Chart types: pie, bar, histogram, survival curves, geographic maps

## Code Conventions

### TypeScript
- Use strict typing; avoid `any` when possible
- Interfaces in `/types/` or co-located with usage
- Use `type` for unions/intersections, `interface` for object shapes

### React Components
- Functional components with hooks
- State managed via useState/useEffect
- Large files need refactoring (DatasetExplorer.tsx is too big)

### API Routes
- RESTful patterns: `/api/datasets`, `/api/datasets/:id/tables`
- Express async handlers with try/catch
- Return JSON responses with `success` or `error` fields

### SQL Generation
- **IMPORTANT**: Use parameterized queries to prevent SQL injection
- ClickHouse parameters: `{paramName:Type}` syntax
- Validate column/table names against schema

### Testing
- Vitest for unit tests
- Test files: `__tests__/*.test.ts` or `*.test.ts`
- Test utilities in `server/src/__tests__/utils/`

## Common Patterns

### Adding a New API Endpoint
```typescript
// server/src/routes/datasets.ts
router.get('/datasets/:id/newEndpoint', async (req, res) => {
  try {
    const result = await datasetService.someMethod(req.params.id)
    res.json({ success: true, data: result })
  } catch (error) {
    console.error('Error:', error)
    res.status(500).json({ error: 'Something went wrong' })
  }
})
```

### Adding a New Service Method
```typescript
// server/src/services/datasetService.ts
async newMethod(datasetId: string): Promise<SomeType> {
  const result = await clickhouseClient.query({
    query: `SELECT ... FROM ... WHERE dataset_id = {datasetId:String}`,
    query_params: { datasetId },
    format: 'JSONEachRow'
  })
  return await result.json<SomeType>()
}
```

### Frontend API Call
```typescript
// client/src/services/api.ts
export const fetchSomething = async (id: string) => {
  const response = await api.get(`/datasets/${id}/something`)
  return response.data
}
```

## Important Files

| File | Purpose | Notes |
|------|---------|-------|
| `server/src/services/aggregationService.ts` | SQL query generation | Complex, handles all filtering logic |
| `server/src/services/datasetService.ts` | Dataset CRUD operations | Table creation, data insertion |
| `server/src/services/fileParser.ts` | CSV parsing | Delimiter detection, list parsing |
| `client/src/pages/DatasetExplorer.tsx` | Main exploration UI | 6K lines, needs refactoring |
| `client/src/pages/DatasetManage.tsx` | Upload/config UI | Dataset and table management |
| `clickhouse/init/01-init.sql` | Database schema | 4 metadata tables |

## Known Issues & Technical Debt

1. **SQL Injection Risk** (#88) - `aggregationService.ts` uses string concatenation
2. **Large Component** (#92) - `DatasetExplorer.tsx` is 6,274 lines
3. **No CI/CD** (#89) - Tests don't run automatically
4. **Type Duplication** (#94) - Same types defined in client and server

## Debugging Tips

### ClickHouse Queries
```bash
# Connect to ClickHouse CLI
docker exec -it biai-clickhouse clickhouse-client

# View recent queries
SELECT query, exception FROM system.query_log ORDER BY event_time DESC LIMIT 10
```

### Server Logs
- Check terminal output for `console.log` statements
- Errors logged with `console.error`

### Frontend Debugging
- React DevTools for component state
- Network tab for API calls
- Console for errors

## Environment Variables

### Server (.env)
```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=biai
PORT=5001
```

### Client (Vite)
```bash
VITE_API_URL=http://localhost:5001/api
```

## Git Workflow

- Feature branches: `feature/feature-name`
- Bug fixes: `bugfix/issue-description`
- Commit messages: `type: description` (feat, fix, docs, refactor, test)
- PRs should reference issue numbers

## Development Workflow Guidelines

**IMPORTANT**: Follow this workflow for all code changes.

### Before Starting Any Work

1. **Create a GitHub Issue First**
   - Before fixing a bug or developing a feature, always create a GitHub issue
   - Include clear description, acceptance criteria, and relevant context
   - Use labels: `bug`, `enhancement`, `documentation`, `security`, etc.
   - Reference related issues if applicable
   ```bash
   gh issue create --title "Brief description" --body "Detailed description..." --label "enhancement"
   ```

2. **Create a Feature Branch**
   - Branch from `main` with descriptive name
   - Use naming convention: `feature/issue-number-brief-description` or `bugfix/issue-number-brief-description`
   ```bash
   git checkout main
   git pull origin main
   git checkout -b feature/123-add-new-filter-type
   ```

3. **Create a Pull Request Early**
   - Create PR as draft if work is in progress
   - Reference the issue in PR description (e.g., "Closes #123" or "Fixes #123")
   - Fill out PR template with summary and test plan
   ```bash
   gh pr create --title "feat: add new filter type" --body "Closes #123" --draft
   ```

### During Development

4. **Make Incremental Commits**
   - Commit frequently with meaningful messages
   - Follow commit message format: `type: description`
   - Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

5. **Keep PR Updated**
   - Push changes regularly
   - Update PR description if scope changes
   - Mark ready for review when complete

### Before Merging

6. **Verify Changes**
   - Run tests: `npm test`
   - Run build: `npm run build`
   - Test manually in browser

7. **Request Review** (if applicable)
   - Mark PR as ready for review
   - Address feedback promptly

### Workflow Commands Summary

```bash
# 1. Create issue
gh issue create --title "Add feature X" --body "Description..." --label "enhancement"

# 2. Create branch (after issue #123 created)
git checkout -b feature/123-add-feature-x

# 3. Create draft PR
gh pr create --title "feat: add feature X" --body "Closes #123" --draft

# 4. Make changes and commit
git add .
git commit -m "feat: implement feature X"
git push origin feature/123-add-feature-x

# 5. Mark ready and merge
gh pr ready
gh pr merge --squash
```

### Exception Cases

- **Trivial fixes** (typos, formatting): Can skip issue creation, but still use a branch and PR
- **Urgent security fixes**: Create issue and PR simultaneously, label as `security`
- **Documentation-only changes**: Issue optional, but PR still required

## Quality Checklist (Before Completing Work)

### After Implementing Features

1. **Add Tests**
   - Write unit tests for new functions/services
   - Test files go in `__tests__/` directories or use `.test.ts` suffix
   - Run `npm test` to verify all tests pass

2. **Update Documentation**
   - Update README.md if adding user-facing features
   - Update CLAUDE.md if adding patterns or conventions
   - Update user docs in `docs/` if UI changes

3. **Clean Up Code**
   - Remove console.log statements (except intentional logging)
   - Remove commented-out code
   - Ensure consistent formatting

4. **Run Build**
   - `npm run build` must succeed
   - Check for TypeScript errors

5. **Manual Testing**
   - Start servers: `npm run dev`
   - Test the feature in browser
   - Verify no console errors

### Periodic Project Reviews

Periodically review the project holistically:
- **Code quality**: Look for technical debt, large files, security issues
- **Documentation**: Ensure docs match current functionality
- **GitHub issues**: Review open issues, close stale ones
- **Dependencies**: Check for outdated packages

Create GitHub issues for identified improvements with appropriate labels.

## Prompting Patterns

### When Asked to Fix a Bug
```
1. Create issue: gh issue create --title "Fix: [bug description]" --label "bug"
2. Create branch: git checkout -b bugfix/[issue-number]-[brief-name]
3. Investigate and fix
4. Add test to prevent regression
5. Create PR referencing the issue
```

### When Asked to Add a Feature
```
1. Create issue: gh issue create --title "Feature: [feature name]" --label "enhancement"
2. Create branch: git checkout -b feature/[issue-number]-[brief-name]
3. Implement feature
4. Add tests
5. Update documentation
6. Create PR referencing the issue
```

### When Asked to Review Project
```
1. Explore codebase structure and patterns
2. Check documentation completeness
3. Review open GitHub issues
4. Identify technical debt and security concerns
5. Create issues for recommendations (with labels)
```

### When Asked to Do Research/Exploration
```
- No issue/branch needed
- Use Explore agent for codebase questions
- Provide summary of findings
```

## GitHub Issue Templates

### Bug Report
```markdown
## Description
Brief description of the bug

## Steps to Reproduce
1. Step one
2. Step two

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Additional Context
Screenshots, error messages, etc.
```

### Feature Request
```markdown
## Summary
Brief description of the feature

## Motivation
Why this feature is needed

## Proposed Solution
How it could be implemented

## Alternatives Considered
Other approaches that were considered

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2
```

### Epic (Large Feature)
```markdown
## Overview
High-level description

## Goals
- Goal 1
- Goal 2

## Phases
### Phase 1: [Name]
- Task 1
- Task 2

### Phase 2: [Name]
- Task 3
- Task 4

## Success Metrics
How to measure completion
```

## Related Documentation

- [User Guide](docs/USER_GUIDE.md) - End-user documentation
- [Quick Reference](docs/QUICK_REFERENCE.md) - Common tasks
- [FAQ](docs/FAQ.md) - Frequently asked questions
- [List Values Feature](docs/list-values-feature.md) - Array column support
- [Testing Checklist](docs/TESTING_CHECKLIST.md) - QA scenarios

## Open GitHub Issues (Key)

- #88 - SQL injection fix (security, high priority)
- #89 - CI/CD pipeline
- #92 - DatasetExplorer refactoring (epic)
- #95 - E2E testing
- #76 - Temporal filtering epic (7 phases)
