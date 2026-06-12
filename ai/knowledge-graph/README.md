# AI Knowledge Graph

This directory contains the initial project knowledge graph for AI-agent-led development.

The goal is practical, not academic:

- give agents a compact semantic scaffold before they touch code
- make impact analysis explicit
- bind code changes to invariants, integration tests, and future E2E scenarios
- keep project memory stable across long sessions

## Files

- `project-graph.xml`: canonical project graph seed
- `agent-prime.xml`: compact session primer for agents
- `e2e-scenarios.yaml`: scenario catalog linked to modules, invariants, and test evidence

## How To Feed This To Agents

Use a three-layer context strategy.

### 1. Session Primer

Always start coding or planning sessions with `agent-prime.xml`.

Use it when the agent needs to:

- understand the project quickly
- align on domain language
- avoid drifting across unrelated modules
- preserve critical invariants

Recommended order:

1. task statement from the human
2. `agent-prime.xml`
3. task-relevant code files
4. task-relevant scenario entries from `e2e-scenarios.yaml`
5. only then broader repo search or edits

### 2. Graph Neighborhood

After priming, give the agent only the graph neighborhood relevant to the task.

Examples:

- subscription feature work:
  - `Subscription`
  - `SubscriptionChannel`
  - `BotFlow.subscription_management`
  - linked invariants
  - linked tests and scenarios
- scraping bug:
  - `Channel`
  - `Post`
  - `Flow.scraping_job`
  - linked repository tests

Do not dump the full repository graph into every session.

### 3. Verification Pack

Before implementation completes, attach the impacted scenario entries and their test commands.

Agents should answer:

- what entities changed
- which invariants may be affected
- which tests were selected and why
- whether any scenario still lacks true E2E automation

## Recommended Agent Prompt Pattern

Use this structure for implementation tasks:

```text
Task: <what needs to change>

Load and obey the attached project primer first.
Then inspect only the graph neighborhood relevant to the task.

Requirements:
- preserve linked invariants
- minimize code changes
- identify impacted tests before editing
- run the smallest sufficient verification set first
- if an impacted E2E scenario lacks full automation, say so explicitly
```

Use this structure for planning tasks:

```text
Task: <feature or bug>

Read the project primer and relevant graph neighborhood.
Return:
1. impacted entities
2. impacted flows
3. invariants at risk
4. tests to run
5. missing scenario coverage
```

## How This Connects To E2E

`e2e-scenarios.yaml` is the bridge between architecture knowledge and verification.

Each scenario links:

- user journey
- touched modules
- domain entities
- invariants
- existing integration evidence
- target future E2E runner

Current state:

- the repository has strong unit and integration coverage
- it does not yet have full external black-box E2E automation
- the scenario catalog is the source of truth for building that layer

## Update Rules

When features change, update all three files together if the change affects behavior:

1. `project-graph.xml`
2. `agent-prime.xml`
3. `e2e-scenarios.yaml`

At minimum, update:

- changed entities and flows
- added or removed invariants
- test links
- scenario status if automation improved

## Design Notes

This MVP intentionally keeps the graph in files instead of introducing a graph database immediately.

That keeps it:

- reviewable in git
- easy to hand to agents
- easy to diff after feature work
- compatible with future ingestion into PostgreSQL, Neo4j, or another graph store
