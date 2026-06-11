# Data Requirements for CELL, NERVE & IRIS

> Mapped against `MCP AI Engineer Schemas.md` (V2.0) + actual codebase analysis

---

## Architecture Recap (Data Flow)

```
ERP Backend ──trigger──► IRIS ──webhook──► NERVE ──fan-out──► CELL (+ CORTEX)
                            │                                     │
                            ▼                                     ▼
                     R2 Storage (YAML)                    ERP API (write-back)
                                                          Slack (notifications)
                                                          Postgres (internal state)
```

---

## 1. IRIS — Meeting Insight Extractor

### 1A. Input Data (What IRIS Needs to Receive)

#### Trigger Payload (from ERP Backend → `POST /iris/trigger`)

| Field | Type | Source Schema | Notes |
|-------|------|--------------|-------|
| `meeting_id` | `str` | Custom (not in MCP schemas) | Unique meeting identifier |
| `project_id` | `str` | `ProjectWorkspace.Code` | Maps to project |
| `r2_path` | `str` | — | R2 folder path containing artifacts |
| `transcript_status` | `Literal["completed","processing"]` | — | Must be `"completed"` |
| `triggered_at` | `datetime` | — | When the trigger was fired |

#### R2 Storage Artifacts (3 files per meeting)

IRIS reads these from R2/mock filesystem at the `r2_path`:

**1. `metadata.json`** — Meeting metadata:

| Field | Type | Maps to MCP Schema |
|-------|------|--------------------|
| `meeting_id` | `str` | — |
| `project_id` | `str` | `ProjectWorkspace.Code` |
| `date` | `str` (YYYY-MM-DD) | — |
| `meeting_type` | `str` | One of: `standup`, `sprint-planning`, `client-call`, `milestone-review`, `cross-dept`, `design-review`, `sales-bd`, `hr`, `company-allhands`, `vendor` |
| `duration_minutes` | `int` | — |
| `security_level` | `str` | — |
| `organiser_id` | `str` | `EmployeeProfile.EmployeeCode` (intranet_id) |
| `attendee_count_internal` | `int` | — |
| `attendee_count_external` | `int` | — |
| `language_mix` | `str` | — |
| `series_id` | `str?` | Optional recurring series ID |
| `client_id` | `str?` | Optional client identifier |

**2. `attendees.json`** — List of attendees:

| Field | Type | Maps to MCP Schema |
|-------|------|--------------------|
| `intranet_id` | `str?` | `EmployeeProfile.EmployeeCode` |
| `name` | `str?` | `EmployeeProfile.DisplayName` |
| `name_hash` | `str?` | Anonymised hash for external attendees |
| `department` | `str?` | `Department.Name` |
| `role` | `str?` | `Position.Title` |
| `type` | `Literal["internal","external"]` | — |

**3. `transcript.txt`** — Raw meeting transcript (plain text)

#### Rerun Payload (PM correction → `POST /iris/rerun`)

| Field | Type | Notes |
|-------|------|-------|
| `meeting_id` | `str` | Meeting to re-extract |
| `pm_notes` | `str` | PM correction instructions |

### 1B. Environment / Secrets

| Variable | Required | Purpose |
|----------|----------|---------|
| `LLM_PROVIDER` | ✅ | `"anthropic"` or `"openai"` |
| `ANTHROPIC_API_KEY` | If anthropic | LLM API key |
| `OPENAI_API_KEY` | If openai | LLM API key |
| `ANTHROPIC_MODEL` | Optional | Default: `claude-haiku-4-5` |
| `OPENAI_MODEL` | Optional | Default: `gpt-4o-mini` |
| `R2_MOCK_BASE_PATH` | ✅ | Local filesystem path for mock R2 |
| `INTRANET_API_BASE_URL` | ✅ | ERP/Intranet API base URL |
| `NERVE_WEBHOOK_URL` | ✅ | NERVE event endpoint (e.g. `http://localhost:8001/nerve/event`) |
| `NERVE_API_KEY` | ✅ | Auth key for NERVE webhook |
| `IRIS_HOST` / `IRIS_PORT` | ✅ | Service binding (`0.0.0.0:8000`) |
| `LOG_LEVEL` | Optional | Default: `INFO` |

### 1C. Output Data (What IRIS Produces)

#### `insights.yaml` (written to R2)

The YAML structure varies by `meeting_type`. All types include a BASE section:

| Field | Source |
|-------|--------|
| `meeting_id`, `project_id`, `date`, `meeting_type`, `duration_minutes` | Passthrough from metadata |
| `security_level` | Passthrough (never changed) |
| `organiser_id` | Passthrough |
| `extraction_confidence` | LLM self-assessment (0.0–1.0) |
| `follow_up_owner` | LLM-extracted `intranet_id` |
| `tags`, `consumer_level` | LLM-derived |

Type-specific sections contain **action items, commitments, blockers, decisions** etc. — all referencing `intranet_id` (= `EmployeeCode`) for person identification.

#### NERVE Event (emitted via `POST /nerve/event`)

| Field | Type | Notes |
|-------|------|-------|
| `event` | `str` | Always `"iris.extraction.complete"` |
| `meeting_id` | `str` | — |
| `project_id` | `str` | — |
| `confidence_score` | `float` | 0.0–1.0 |
| `flagged` | `bool` | `true` if confidence < 0.6 |
| `insights_path` | `str` | R2 path to `insights.yaml` |
| `timestamp` | `datetime` | UTC |
| `provider` | `str` | `"anthropic"` or `"openai"` |

### 1D. ERP Schema Data IRIS Depends On

| MCP Schema Entity | Fields Used | How |
|-------------------|-------------|-----|
| `EmployeeProfile` | `EmployeeCode` (as `intranet_id`), `DisplayName`, `Department`, `Position` | Referenced in attendees.json & extracted insights |
| `ProjectWorkspace` | `Code` (as `project_id`) | Meeting → project mapping |
| `Department` | `Name` | Attendee department context |
| `Position` | `Title` | Attendee role context |

> [!IMPORTANT]
> IRIS does **not** query the ERP database directly. It relies on pre-populated R2 artifacts (`metadata.json`, `attendees.json`) that must be seeded with correct `EmployeeCode` and `ProjectWorkspace.Code` values.

---

## 2. NERVE — Event Orchestrator / Router

### 2A. Input Data (What NERVE Receives)

#### Inbound Event (from IRIS or ERP → `POST /nerve/event`)

| Field | Type | Notes |
|-------|------|-------|
| `event` | `str` | Event type, e.g. `iris.extraction.complete` |
| `meeting_id` | `str?` | Optional |
| `project_id` | `str?` | Optional |
| _+ extra fields_ | `Any` | `Config.extra = "allow"` — all IRIS event fields pass through |

Auth: `X-API-Key` header must match `NERVE_API_KEY`.

### 2B. Internal Database (NERVE's own Postgres)

NERVE maintains **4 tables** for orchestration state:

**`nerve_event_log`** — Event audit trail:

| Column | Type | Notes |
|--------|------|-------|
| `id` | `SERIAL PK` | — |
| `timestamp` | `TIMESTAMPTZ` | Auto |
| `event_type` | `VARCHAR` | e.g. `iris.extraction.complete` |
| `source` | `VARCHAR` | e.g. `external` |
| `project_id` | `VARCHAR?` | — |
| `meeting_id` | `VARCHAR?` | — |
| `details` | `JSONB` | Full event payload |
| `status` | `VARCHAR` | `received` → `completed` / `failed` / `ignored` / `error` |
| `error_message` | `TEXT?` | On failure |

**`nerve_job_log`** — Agent call audit:

| Column | Type | Notes |
|--------|------|-------|
| `id` | `SERIAL PK` | — |
| `trigger_id` | `VARCHAR` | UUID per call |
| `job_id` | `VARCHAR` | — |
| `target_agent` | `VARCHAR` | `cell`, `cortex`, etc. |
| `target_endpoint` | `VARCHAR` | e.g. `/cell/ingest-nerve` |
| `success` | `BOOLEAN` | — |
| `duration_ms` | `INT` | — |
| `error_type` | `VARCHAR?` | `agent_down`, `timeout`, `quota_exceeded` |
| `error_message` | `TEXT?` | — |
| `response_payload` | `JSONB?` | Agent response |
| `completed_at` | `TIMESTAMPTZ` | — |

**`nerve_agent_status`** — Agent health tracking:

| Column | Type | Notes |
|--------|------|-------|
| `agent` | `VARCHAR PK` | `iris`, `cell`, `cortex`, `stroma` |
| `base_url` | `VARCHAR` | — |
| `status` | `VARCHAR` | `unknown` / `healthy` / `degraded` |
| `last_success` | `TIMESTAMPTZ?` | — |
| `last_failure` | `TIMESTAMPTZ?` | — |
| `consecutive_failures` | `INT` | Reset on success |
| `updated_at` | `TIMESTAMPTZ` | — |

**`nerve_provider_status`** — LLM provider circuit breaker:

| Column | Type | Notes |
|--------|------|-------|
| `provider` | `VARCHAR PK` | `anthropic`, `openai` |
| `status` | `VARCHAR` | `ok` / `quota_exceeded` / `credits_exhausted` |
| `last_error` | `TEXT?` | — |
| `updated_at` | `TIMESTAMPTZ` | — |

### 2C. Environment / Secrets

| Variable | Required | Purpose |
|----------|----------|---------|
| `DATABASE_URL` | ✅ | Postgres connection string |
| `NERVE_API_KEY` | ✅ | Auth for inbound webhooks |
| `IRIS_BASE_URL` | ✅ | IRIS agent URL (default `http://localhost:8000`) |
| `CELL_BASE_URL` | ✅ | CELL agent URL (default `http://localhost:8002`) |
| `CORTEX_BASE_URL` | ✅ | CORTEX agent URL (default `http://localhost:8004`) |
| `STROMA_BASE_URL` | ✅ | STROMA agent URL (default `http://localhost:8005`) |
| `SLACK_BOT_TOKEN` | Optional | For alerting on failures |
| `SLACK_ADMIN_CHANNEL` | Optional | Channel for admin alerts |
| `NERVE_HOST` / `NERVE_PORT` | ✅ | Service binding (`0.0.0.0:8001`) |
| `MOCK_MODE` | Optional | Bypass DB with mocks |
| `TZ` | Optional | Default: `Asia/Kolkata` |

### 2D. Routing Logic (What NERVE Forwards)

On `iris.extraction.complete`, NERVE fans out to **two** agents simultaneously:

| Target | Endpoint | Payload |
|--------|----------|---------|
| **CELL** | `POST /cell/ingest-nerve` | Full IRIS event payload |
| **CORTEX** | `POST /cortex/ingest-nerve` | Full IRIS event payload |

### 2E. ERP Schema Data NERVE Depends On

| MCP Schema Entity | Fields Used | How |
|-------------------|-------------|-----|
| `ProjectWorkspace` | `Code` (as `project_id`) | Passed through in events, logged |
| — | — | NERVE does **not** query ERP directly |

> [!NOTE]
> NERVE is schema-agnostic — it routes events without interpreting ERP data. It only needs its own 4 Postgres tables and network access to downstream agents.

---

## 3. CELL — Task Lifecycle Manager

### 3A. Input Data (What CELL Receives)

#### From NERVE (`POST /cell/ingest-nerve`) — NerveEvent:

| Field | Type | Maps to MCP Schema |
|-------|------|--------------------|
| `event` | `str` | — |
| `meeting_id` | `str` | — |
| `project_id` | `str` | `ProjectWorkspace.Code` |
| `confidence_score` | `float` | — |
| `flagged` | `bool` | — |
| `insights_path` | `str` | R2 path to `insights.yaml` |
| `timestamp` | `datetime` | — |

#### From Agent 3 (`POST /cell/ingest-tasks`) — Agent3IngestRequest:

| Field | Type | Maps to MCP Schema |
|-------|------|--------------------|
| `source` | `str` | Always `"agent3"` |
| `project_id` | `str` | `ProjectWorkspace.Code` |
| `week_ref` | `str` | e.g. `"2026-W20"` |
| `tasks[].title` | `str` | → `WorkItem.Title` |
| `tasks[].assignee_id` | `str` | `EmployeeProfile.EmployeeCode` |
| `tasks[].estimated_hours` | `float` | — |
| `tasks[].priority` | `str` | `urgent/high/normal/low` |
| `tasks[].due_date` | `date` | → `WorkItem.DueAt` |
| `tasks[].notes` | `str?` | — |

### 3B. R2 Data CELL Fetches

CELL fetches `insights.yaml` from R2 using the `insights_path` from the NERVE event. The YAML contains the structured meeting insights produced by IRIS — the exact fields CELL extracts from depend on `meeting_type`:

| Meeting Type | YAML Section | Fields Extracted into Tasks |
|-------------|-------------|---------------------------|
| `standup` | `standup.blocked_today[]` | `person_id`, `desc`, `days_carried`, `cross_team`, `blocking_dept` |
| `standup` | `standup.unplanned_work_mentioned[]` | `desc`, `raised_by` |
| `standup` | `standup.silent_members[]` | `person_id`, `days_silent_in_row` |
| `hr` | `hr.action_items[]` | `text`, `owner`, `due` |
| `hr` | `hr.decisions[]` | `text`, `owner`, `due` |
| `client-call` | `client.commitments[]` | `text`, `owner`, `due`, `made_by`, `status`, `critical_path` |
| `vendor` | `vendor.commitments[]` | `text`, `owner`, `due`, `status` |
| `sales-bd` | `sales_bd.next_action` | `next_action`, `next_action_owner`, `next_action_due` |
| `sprint-planning` | `internal.decisions[]` | `text`, `owner`, `due` |
| `sprint-planning` | `internal.deferred_items[]` | `text`, `owner`, `deferred_count` |
| `sprint-planning` | `internal.cross_dept_dependencies[]` | `item`, `owner`, `due`, `blocking_dept`, `blocked_dept` |
| `design-review` | `design_review.feedback_items[]` | `item`, `severity`, `owner`, `due` |
| `milestone-review` | `milestone_review.sign_offs[]` | `deliverable`, `status`, `rework_requested`, `rework_owner`, `rework_due` |
| `cross-dept` | `cross_dept.unresolved_items[]` | `text`, `owner`, `due` |

All `owner`/`person_id` values map to **`EmployeeProfile.EmployeeCode`** (`intranet_id`).

### 3C. Internal Database (CELL's own Postgres + pgvector)

CELL maintains **5 tables**:

**`tasks`** — Internal task state:

| Column | Type | Maps to MCP Schema |
|--------|------|--------------------|
| `id` | `SERIAL PK` | — |
| `erp_task_id` | `VARCHAR?` | `WorkItem.Id` (after ERP write) |
| `project_id` | `VARCHAR` | `ProjectWorkspace.Code` |
| `assignee_id` | `VARCHAR` | `EmployeeProfile.EmployeeCode` |
| `title` | `VARCHAR` | `WorkItem.Title` |
| `title_embedding` | `VECTOR` | pgvector for dedup |
| `priority` | `VARCHAR` | `urgent/high/normal/low` → `WorkItem.Priority` |
| `estimated_hours` | `FLOAT` | — |
| `due_date` | `DATE?` | `WorkItem.DueAt` |
| `status` | `VARCHAR` | `open/in_progress/pending_pm_approval/approved/rejected/deadline_missed/blocked/closed` |
| `source` | `VARCHAR` | `iris/agent3/manual` |
| `source_meeting_id` | `VARCHAR?` | — |
| `pm_notes` | `TEXT?` | — |
| `bounty_value` | `DECIMAL` | Bounty units (×₹100 = payout) |
| `created_at` / `updated_at` | `TIMESTAMPTZ` | — |

**`bounty_ledger`** — Bounty tracking:

| Column | Type | Notes |
|--------|------|-------|
| `id` | `SERIAL PK` | — |
| `task_erp_id` | `VARCHAR` | → `WorkItem.Id` |
| `intern_id` | `VARCHAR` | → `EmployeeProfile.EmployeeCode` |
| `project_id` | `VARCHAR` | → `ProjectWorkspace.Code` |
| `estimated_hours` | `FLOAT` | — |
| `priority` | `VARCHAR` | — |
| `bounty_value` | `DECIMAL` | — |
| `status` | `VARCHAR` | `pending/approved/rejected` |
| `approved_by` | `VARCHAR?` | PM's `EmployeeCode` |
| `approved_at` | `TIMESTAMPTZ?` | — |

**`eod_submissions`** — EOD report tracking:

| Column | Type | Maps to MCP Schema |
|--------|------|--------------------|
| `id` | `SERIAL PK` | — |
| `intern_id` | `VARCHAR` | `EmployeeProfile.EmployeeCode` |
| `task_id` | `INT` | FK → tasks.id |
| `submission_date` | `DATE` | `DailyStatusEntry.StatusDate` |
| `status` | `VARCHAR` | `done/blocked/carry/missed` |
| `block_reason` | `TEXT?` | `DailyStatusEntry.Blockers` |
| `raw_message` | `TEXT` | — |
| `parse_success` | `BOOLEAN` | — |

**`accountability_log`** — Escalation tracking:

| Column | Type | Notes |
|--------|------|-------|
| `id` | `SERIAL PK` | — |
| `intern_id` | `VARCHAR` | `EmployeeProfile.EmployeeCode` |
| `date` | `DATE` | UNIQUE with intern_id |
| `eod_submitted` | `BOOLEAN` | — |
| `tasks_missed` | `INT` | — |
| `consecutive_miss_count` | `INT` | Triggers escalation at 3 (→APM) and 5 (→Dept Head) |
| `warning_sent` | `BOOLEAN` | — |
| `escalated_to` | `VARCHAR?` | APM or Dept Head `EmployeeCode` |

**`erp_write_queue`** — Retry/dead-letter queue:

| Column | Type | Notes |
|--------|------|-------|
| `id` | `SERIAL PK` | — |
| `task_id` | `INT` | FK → tasks.id |
| `payload` | `JSONB` | ERP API payload |
| `attempt_count` | `INT` | Max 3 retries |
| `last_error` | `TEXT?` | — |
| `status` | `VARCHAR` | `pending/success/dead_letter` |
| `next_retry_at` | `TIMESTAMPTZ?` | Exponential backoff |

### 3D. Seed Data CELL Needs in its `employees` Table

CELL queries a local `employees` table for Slack routing and escalation:

| Column | Type | Maps to MCP Schema |
|--------|------|--------------------|
| `employee_id` | `VARCHAR PK` | `EmployeeProfile.EmployeeCode` |
| `slack_user_id` | `VARCHAR` | `EmployeeProfile.SlackUsername` |
| `name` | `VARCHAR` | `EmployeeProfile.DisplayName` |
| `role` | `VARCHAR` | `"intern"`, `"pm"`, etc. |
| `active` | `BOOLEAN` | `EmployeeProfile.Status == 'Active'` |
| `apm_id` | `VARCHAR?` | `ProjectWorkspace.AssociateProjectManager` → `EmployeeCode` |
| `dept_head_id` | `VARCHAR?` | Manager chain / Department head |

CELL also queries a `project_members` table:

| Column | Type | Maps to MCP Schema |
|--------|------|--------------------|
| `project_id` | `VARCHAR` | `ProjectWorkspace.Code` |
| `employee_id` | `VARCHAR` | `EmployeeProfile.EmployeeCode` |
| `role` | `VARCHAR` | `"pm"` for PM lookup |

### 3E. ERP API (Write-back)

CELL writes tasks to ERP via `POST /api/tasks`:

| Field | Type | Maps to MCP Schema |
|-------|------|--------------------|
| `title` | `str` | → `WorkItem.Title` |
| `project_id` | `str` | → `WorkItem.Project` (via `ProjectWorkspace.Code`) |
| `assignee_id` | `str` | → `WorkItem.Owner` (via `EmployeeProfile.EmployeeCode`) |
| `priority` | `str` | → `WorkItem.Priority` |
| `due_date` | `str?` (ISO) | → `WorkItem.DueAt` |
| `estimated_hours` | `float` | — |
| `bounty_value` | `float` | → `WorkItem.Bounty` |
| `status` | `str` | → `WorkItem.Status` |
| `source_meeting_id` | `str?` | — |
| `notes` | `str?` | — |
| `subtasks` | `list` | → `WorkItem.Parent` (subtask relationship) |

Auth: `X-API-Key` header with `ERP_API_KEY`.

### 3F. Slack Integration

CELL sends messages via Slack Bot to:

| Purpose | Recipient | Data Needed |
|---------|-----------|-------------|
| Morning task digest | Each intern | `SlackUsername`, open tasks list |
| EOD reminder | Each intern | `SlackUsername` |
| PM approval digest | Project PM | `SlackUsername`, pending tasks |
| Escalation warnings | Intern → APM → Dept Head | `SlackUsername` chain |

Required Slack scopes: `chat:write`, `im:history`, `im:write`, `users:read`

### 3G. Environment / Secrets

| Variable | Required | Purpose |
|----------|----------|---------|
| `OPENAI_API_KEY` | ✅ | Task title normalisation & hours estimation |
| `DATABASE_URL` | ✅ | Postgres+pgvector connection |
| `R2_ENDPOINT_URL` | ✅ | Cloudflare R2 endpoint |
| `R2_ACCESS_KEY_ID` | ✅ | R2 auth |
| `R2_SECRET_ACCESS_KEY` | ✅ | R2 auth |
| `R2_BUCKET_NAME` | ✅ | Default: `erp-agents` |
| `SLACK_BOT_TOKEN` | ✅ | Slack API (`xoxb-...`) |
| `ERP_BASE_URL` | ✅ | ERP API for task write-back |
| `ERP_API_KEY` | ✅ | ERP auth |
| `CELL_HOST` / `CELL_PORT` | ✅ | Service binding (`0.0.0.0:8002`) |
| `MOCK_MODE` | Optional | Use mock ERP + Slack |
| `TZ` | Optional | Hardcoded to `Asia/Kolkata` |

---

## 4. Summary: MCP Schema Entities Used Across All Agents

| MCP Schema Entity | IRIS | NERVE | CELL | Usage |
|-------------------|------|-------|------|-------|
| **EmployeeProfile.EmployeeCode** | ✅ (as `intranet_id` in attendees & insights) | — | ✅ (as `assignee_id`, `intern_id`, `pm_id`) | Person identification everywhere |
| **EmployeeProfile.DisplayName** | ✅ (attendee name) | — | ✅ (employee name for Slack) | Display/notification |
| **EmployeeProfile.SlackUsername** | — | — | ✅ (DM routing) | Slack notifications |
| **EmployeeProfile.Status** | — | — | ✅ (`active` flag) | Filter active interns |
| **EmployeeProfile.Department** | ✅ (attendee context) | — | ✅ (escalation chain) | Org context |
| **EmployeeProfile.Position** | ✅ (attendee role) | — | — | Meeting context |
| **EmployeeProfile.Manager** | — | — | ✅ (escalation to dept head) | Accountability chain |
| **ProjectWorkspace.Code** | ✅ (as `project_id`) | ✅ (passthrough) | ✅ (task project assignment) | Project identification |
| **ProjectWorkspace.ProjectManager** | — | — | ✅ (PM lookup for approval) | PM digest routing |
| **ProjectWorkspace.AssociateProjectManager** | — | — | ✅ (APM escalation) | Escalation at 3 misses |
| **WorkItem** (all fields) | — | — | ✅ (creates via ERP API) | Task creation/update |
| **WorkItem.Bounty** | — | — | ✅ (calculated & written) | Bounty system |
| **DailyStatusEntry** | — | — | ✅ (EOD tracking) | EOD submissions |
| **SlackDeliveryThread/Message** | — | — | ✅ (Slack posting) | EOD Slack delivery |

---

## 5. Minimum Seed Data Checklist

For the full pipeline to work end-to-end, you need:

### In ERP/Intranet Database
- [ ] **Employees** with: `EmployeeCode`, `DisplayName`, `SlackUsername`, `Status`, `Department`, `Position`, `Manager`
- [ ] **Projects** with: `Code`, `Name`, `ProjectManager` (FK→Employee), `AssociateProjectManager` (FK→Employee), `Status`
- [ ] **TeamAssignments** linking employees to projects with roles

### In R2 Storage (per meeting)
- [ ] `metadata.json` — with valid `project_id` matching `ProjectWorkspace.Code` and `organiser_id` matching `EmployeeProfile.EmployeeCode`
- [ ] `attendees.json` — with `intranet_id` values matching `EmployeeProfile.EmployeeCode`
- [ ] `transcript.txt` — meeting transcript text

### In CELL's Local Postgres
- [ ] `employees` table seeded with employee data (synced from ERP)
- [ ] `project_members` table seeded with PM assignments (synced from ERP)

### In NERVE's Local Postgres
- [ ] 4 tables created via migration (`nerve_event_log`, `nerve_job_log`, `nerve_agent_status`, `nerve_provider_status`)

### External Services
- [ ] **LLM API Key** — Anthropic or OpenAI (for IRIS extraction + CELL enrichment)
- [ ] **Slack Bot Token** — with required scopes (for CELL notifications)
- [ ] **R2/S3 credentials** — for storage access (IRIS writes, CELL reads)
- [ ] **ERP API** — running and accessible (CELL writes tasks back)
