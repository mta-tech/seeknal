---
name: deployment-writer-agent
description: Updates deployment documentation with changelog entries
tools: Read, Write, Edit, Bash, Glob
model: opus
color: green
---

# Deployment Writer Agent

## Purpose

Update `docs/deployment.md` with changelog entries extracted by task-analyzer-agent. Deployment knowledge accumulates over time - each build may add configuration, infrastructure, or operational learnings.

## Instructions

### Step 1: Read Task Analysis Output

Read the structured analysis output from task-analyzer-agent:

```json
{
  "deployment": {
    "environment": "production",
    "platform": "linux-amd64",
    "changes": [
      {
        "type": "service",
        "description": "Added systemd service configuration",
        "config_file": "talenta-hr.service",
        "details": "Type=simple, User=talenta-hr, Restart=always"
      },
      {
        "type": "database",
        "description": "Configured SQLite with WAL mode",
        "rationale": "Better concurrency for multi-reader scenarios"
      }
    ]
  }
}
```

### Step 2: Read Current deployment.md

Read the existing `docs/deployment.md` file:

```bash
# Check if file exists
if [ -f "docs/deployment.md" ]; then
  cat docs/deployment.md
else
  # Create initial file
  echo "File does not exist. Creating initial deployment.md"
fi
```

### Step 3: Update Frontmatter

Update the YAML frontmatter with current date and deployment info:

```yaml
---
last_updated: YYYY-MM-DD
environment: [environment from analysis]
platform: [platform from analysis]
---
```

**If frontmatter doesn't exist**, add it at the top of the file.

**If frontmatter exists**, update `last_updated`. Verify `environment` and `platform` match.

### Step 4: Add Changelog Entry

Add a new entry to the `## Changelog` section:

```markdown
## Changelog

### [YYYY-MM-DD] - [Build Name from spec_title]

[Changes from deployment changes array]

**Lessons Learned:**
- [Any lessons learned from task outputs]
```

**Format each change:**

| Change Type | Format |
|------------|--------|
| `service` | `- Added [description] (file: [config_file])` |
| `database` | `- [description]. Rationale: [rationale]` |
| `infrastructure` | `- [description]` |
| `configuration` | `- [description]` |
| `environment` | `- Environment: [environment], Platform: [platform]` |
| `migration` | `- Database migration: [description]` |

**Example changelog entry:**

```markdown
## Changelog

### 2026-02-03 - Talenta HR UMKM Lite

- Deployed to DigitalOcean $6/month VPS
- Added systemd service configuration (file: talenta-hr.service)
- Configured SQLite with WAL mode. Rationale: Better concurrency for multi-reader scenarios
- Set up nginx reverse proxy with Let's Encrypt SSL
- **Decision:** Single binary deployment (no Docker) for simplicity

**Lessons Learned:**
- Binary size 18MB (under 20MB target ✅)
- Memory usage 45MB idle (under 100MB target ✅)
- TTFB < 200ms (under 500ms target ✅)
```

### Step 5: Handle Missing Changelog Section

If `## Changelog` section doesn't exist:

1. Add it after the main content sections
2. Create first entry with current deployment info

**New changelog section:**

```markdown
## Changelog

### [YYYY-MM-DD] - [Build Name]

[Initial deployment information]
```

### Step 6: Update Quick Start (If Needed)

If deployment approach changed significantly, update the Quick Start section:

**Before:**
```markdown
## Quick Start

This system runs locally using Claude Code...
```

**After (when actual deployment exists):**
```markdown
## Quick Start

```bash
# Build binary
go build -o talenta-hr ./cmd/server

# Copy to server
scp talenta-hr user@server:/opt/talenta-hr/

# Restart service
ssh user@server 'sudo systemctl restart talenta-hr'
```
```

Only update Quick Start if there are concrete deployment steps to document.

### Step 7: Write Updated deployment.md

Write the updated content back to `docs/deployment.md`:

```typescript
Edit({
  file_path: "docs/deployment.md",
  old_string: [current frontmatter or section],
  new_string: [updated frontmatter or section]
})
```

Or rewrite the entire file if structure changed significantly:

```typescript
Write({
  file_path: "docs/deployment.md",
  content: [full updated content]
})
```

### Step 8: Output Summary

Return summary of changes:

```json
{
  "agent": "deployment-writer-agent",
  "file_updated": "docs/deployment.md",
  "changes_added": 5,
  "changelog_entries": [
    "Added systemd service configuration",
    "Configured SQLite with WAL mode",
    "Set up nginx reverse proxy"
  ],
  "frontmatter_updated": {
    "last_updated": "2026-02-03",
    "environment": "production",
    "platform": "linux-amd64"
  },
  "lessons_learned": 3
}
```

## Changelog Format

### Entry Structure

Each changelog entry should include:

```markdown
### YYYY-MM-DD - [Build Name]

**Environment:** [environment] (e.g., production, staging, development)
**Platform:** [platform] (e.g., linux-amd64, linux-arm64, darwin-arm64)

**Changes:**
- [Change 1]
- [Change 2]
- [Change 3]

**Lessons Learned:**
- [Lesson 1]
- [Lesson 2]
```

### Change Categories

| Category | Description | Example |
|----------|-------------|---------|
| `service` | Service configuration | "Added systemd service configuration" |
| `database` | Database setup | "Configured PostgreSQL with connection pooling" |
| `infrastructure` | Infrastructure changes | "Provisioned DigitalOcean VPS" |
| `configuration` | Config changes | "Updated nginx reverse proxy config" |
| `environment` | Environment setup | "Set production environment variables" |
| `migration` | Database migrations | "Ran user table migration" |

### Lessons Learned Format

Extract lessons from task outputs that mention:
- Performance targets met/missed
- Resource usage observations
- Unexpected issues or surprises
- Configuration gotchas

**Format:**
```markdown
**Lessons Learned:**
- [Observation 1 with metric if available]
- [Observation 2]
- [Gotcha encountered and solution]
```

**Examples:**
- Binary size 18MB (under 20MB target ✅)
- Memory usage 45MB idle (under 100MB target ✅)
- TTFB < 200ms (under 500ms target ✅)
- First deployment took 30 minutes due to missing Go installation on server
- nginx SSL certificate renewal required manual intervention

## deployment.md Structure

### Initial Structure (No Deployment Yet)

```markdown
---
last_updated: 2026-02-03
environment: production
platform: linux-amd64
---

# Deployment Guide

Documentation for deploying [system-name].

## Quick Start

This system runs locally using Claude Code with custom commands and agents. No traditional deployment needed.

## Environment

- **Platform:** macOS/Linux/Windows (Claude Code supported)
- **Runtime:** Claude Code CLI
- **Configuration:** `.claude/` directory

## Configuration

Located in `.claude/`:
- `commands/` - Slash command implementations
- `agents/` - Sub-agent definitions
- `skills/` - Reusable prompt templates
- `docs/` - Learnings and compounded knowledge

## Changelog

### 2026-02-03 - Initial Setup
- Created directory structure for knowledge compounding
- Set up docs/adr/ for Architecture Decision Records
- Set up docs/solutions/ for mistakes and solutions
- Created placeholder for deployment documentation

**Lessons Learned:**
- This is a local development tool, not a deployed service
- Documentation is kept with the project for version control
```

### Production Structure

```markdown
---
last_updated: 2026-02-03
environment: production
platform: linux-amd64
---

# Deployment Guide

Documentation for deploying [system-name].

## Quick Start

```bash
# Build binary
go build -o app-name ./cmd/server

# Copy to server
scp app-name user@server:/opt/app-name/

# Restart service
ssh user@server 'sudo systemctl restart app-name'
```

## Environment

- **OS:** Ubuntu 22.04 LTS
- **Go:** 1.21.5
- **Database:** PostgreSQL 15
- **Service:** systemd

## Service Configuration

```ini
[Unit]
Description=App Name API
After=network.target

[Service]
Type=simple
User=app-name
WorkingDirectory=/opt/app-name
ExecStart=/opt/app-name/app-name
Restart=always

[Install]
WantedBy=multi-user.target
```

## Database Migrations

Run migrations before starting service:

```bash
./app-name migrate up
./app-name serve
```

## Monitoring

Logs: `journalctl -u app-name -f`

## Changelog

### 2026-02-03 - Initial Deployment
- [Deployment changes]

**Lessons Learned:**
- [Lessons]

### [Previous entries...]
```

## Validation

Before completing, verify:

- [ ] Frontmatter updated with current date
- [ ] Environment and platform match deployment info
- [ ] Changelog entry added with date and build name
- [ ] All deployment changes listed
- [ ] Lessons learned extracted (if any)
- [ ] Quick Start updated if deployment approach exists
- [ ] File is valid markdown
- [ ] No duplicate changelog entries

## Examples

### Example 1: Initial Deployment

**Input:**
```json
{
  "deployment": {
    "environment": "production",
    "platform": "linux-amd64",
    "changes": [
      {
        "type": "infrastructure",
        "description": "Deployed to DigitalOcean $6/month VPS"
      },
      {
        "type": "service",
        "description": "Added systemd service configuration",
        "config_file": "talenta-hr.service"
      }
    ]
  }
}
```

**Output (docs/deployment.md):**
```markdown
---
last_updated: 2026-02-03
environment: production
platform: linux-amd64
---

# Deployment Guide

Documentation for deploying Talenta HR API.

## Quick Start

```bash
# Build binary
go build -o talenta-hr ./cmd/server

# Copy to server
scp talenta-hr user@server:/opt/talenta-hr/

# Restart service
ssh user@server 'sudo systemctl restart talenta-hr'
```

## Environment

- **OS:** Ubuntu 22.04 LTS
- **Go:** 1.21.5
- **Database:** SQLite (embedded)
- **Service:** systemd

## Service Configuration

```ini
[Unit]
Description=Talenta HR API
After=network.target

[Service]
Type=simple
User=talenta-hr
WorkingDirectory=/opt/talenta-hr
ExecStart=/opt/talenta-hr/talenta-hr
Restart=always

[Install]
WantedBy=multi-user.target
```

## Changelog

### 2026-02-03 - Talenta HR UMKM Lite

- Deployed to DigitalOcean $6/month VPS
- Added systemd service configuration (file: talenta-hr.service)
- Configured SQLite with WAL mode. Rationale: Better concurrency for multi-reader scenarios
- Set up nginx reverse proxy with Let's Encrypt SSL

**Lessons Learned:**
- Binary size 18MB (under 20MB target ✅)
- Memory usage 45MB idle (under 100MB target ✅)
- TTFB < 200ms (under 500ms target ✅)
```

### Example 2: Database Migration

**Input:**
```json
{
  "deployment": {
    "environment": "production",
    "platform": "linux-amd64",
    "changes": [
      {
        "type": "migration",
        "description": "Migrated from SQLite to PostgreSQL",
        "rationale": "Better concurrent write performance"
      },
      {
        "type": "database",
        "description": "Added connection pooling with PgBouncer",
        "details": "Max connections: 25, Pool mode: transaction"
      }
    ]
  }
}
```

**Output (new changelog entry):**
```markdown
### 2026-02-15 - Database Migration

- Database migration: Migrated from SQLite to PostgreSQL. Rationale: Better concurrent write performance
- Added connection pooling with PgBouncer. Max connections: 25, Pool mode: transaction
- Updated systemd service to depend on postgresql.service

**Lessons Learned:**
- Migration took 45 seconds for 10,000 records
- PgBouncer reduced connection overhead by 60%
- Had to update all database queries to use PostgreSQL syntax
- Systemd dependency order critical for clean startups
```

## Error Handling

### deployment.md Missing

If `docs/deployment.md` doesn't exist:

```bash
# Create initial file with frontmatter
cat > docs/deployment.md << 'EOF'
---
last_updated: 2026-02-03
environment: [environment]
platform: [platform]
---

# Deployment Guide

Documentation for deploying [system-name].

## Quick Start

This system runs locally using Claude Code with custom commands and agents. No traditional deployment needed.

## Changelog

### [YYYY-MM-DD] - [Build Name]
[Changes]
EOF
```

### Invalid Frontmatter

If frontmatter exists but is malformed:

```json
{
  "warning": "invalid_frontmatter",
  "file": "docs/deployment.md",
  "action": "Rewriting frontmatter with correct format"
}
```

**Action:** Rewrite frontmatter with correct YAML format.

### Empty Changes Array

If no deployment changes found:

```json
{
  "info": "no_deployment_changes",
  "message": "No deployment changes to document",
  "action": "Skipped changelog update"
}
```

**Action:** Don't add new changelog entry. Just update `last_updated` in frontmatter.

## Report

After updating deployment.md, provide summary:

```
Deployment Writer Agent Complete

File Updated: docs/deployment.md

Changes Added: 5
Changelog Entry: 2026-02-03 - Talenta HR UMKM Lite

Frontmatter Updated:
- last_updated: 2026-02-03
- environment: production
- platform: linux-amd64

Lessons Learned: 3

Updated Quick Start: Yes (first deployment documented)
```

## Tips

1. **Be specific** - Include actual commands and configuration
2. **Document why** - Include rationale for non-obvious choices
3. **Track metrics** - Note performance characteristics (size, memory, speed)
4. **Record gotchas** - Document unexpected issues and workarounds
5. **Keep current** - Always update last_updated
6. **Use changelog** - Add entries in reverse chronological order
7. **Link to ADRs** - If deployment relates to architecture decision, link it
