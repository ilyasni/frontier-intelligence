# Frontier Intelligence for AI/Tech PMs
<!-- audit-status:2026-08-04 -->
> **📐 ЗАМЫСЕЛ, НЕ РЕАЛИЗОВАНО · сверено 2026-08-04.**
> Замысел, а не описание системы: на дату сверки не реализован. Не читать как отчёт о готовом.
> Конкретных расхождений найдено: **3** — перечислены в разборе.
> Разбор: [AUDIT-2026-08-04.md](../AUDIT-2026-08-04.md).

### Onboarding Guide — 3–5 minutes to your first insight

---

## What you're setting up

A Claude Project wired to Frontier Intelligence — a live corpus of AI/Tech signals, competitive moves, and emerging product patterns. Your assistant doesn't answer from memory. It queries the corpus and gives you signal-backed briefs.

Four modes, no configuration needed:
- **Signal Scan** — what's happening in a product area right now
- **Feature Intelligence** — what competitors are shipping and what's gaining traction
- **Decision Brief** — structured brief for a specific product decision
- **Weekly Digest** — your workspace, summarized for the week

---

## Step 1 — Create the Claude Project

1. Open [claude.ai](https://claude.ai) and go to **Projects** in the left sidebar.
2. Click **New project**.
3. Name it something like `Frontier PM Intelligence` or whatever makes sense for your workflow.
4. Open **Project instructions** (the gear icon or settings tab inside the project).
5. Paste the entire contents of `ai-pm-project-template.md` into the instructions field.
6. Save.

The project is now set up. Before the MCP is connected, Claude will work from its training knowledge. After Step 2, it will pull live signals.

---

## Step 2 — Connect the Frontier MCP

1. In Claude desktop app, go to **Settings → Integrations** (or **MCP Servers**, depending on your version).
2. Add a new MCP server with the following details:

```
Name:    Frontier Intelligence
URL:     [YOUR_FRONTIER_MCP_ENDPOINT]
Auth:    Bearer [YOUR_API_KEY]
```

> Replace `[YOUR_FRONTIER_MCP_ENDPOINT]` and `[YOUR_API_KEY]` with the credentials provided by your Frontier Intelligence admin.

3. Save and restart Claude if prompted.
4. Open your project. You should see the Frontier tools available in the conversation (the assistant will be able to call `search_frontier`, `get_frontier_brief`, etc.).

**Verify the connection** by sending: `Check if Frontier MCP is connected and list available workspaces.`

You should get a list of workspaces — typically including `ai_tools`, `disruption`, and any domain-specific workspaces your organization has set up.

---

## Step 3 — Configure your workspace

By default the assistant uses `ai_tools` for AI product signals and `disruption` for broader market shifts.

If your product area has a dedicated workspace, tell the assistant once at the start of a conversation:

> "My primary domain is [your area — e.g., developer tools, AI agents, data platforms]. Use the `[workspace_name]` workspace by default."

You can also pin this as a note in Project instructions alongside the system prompt.

---

## Your first three commands

Try these to calibrate the assistant to your workflow:

**1. Signal Scan — get oriented in your area**
```
Signal scan: what's happening in [your product category] in the last 7 days.
```
Expected output: a structured scan with confirmed patterns, early signals, noise, and a product implication.

**2. Feature Intelligence — check competitive landscape**
```
Feature intelligence: what are the major AI assistant players shipping in [specific feature area]?
```
Expected output: a market map by player, what's gaining traction vs. announced-but-unproven, and a differentiation read.

**3. Decision Brief — get support on a real decision**
```
Decision brief: should we prioritize building [feature X] now or wait 6 months?
Context: [one paragraph on your product, stage, and what you know so far]
```
Expected output: a structured brief with evidence layers, options, a directional recommendation, and what's missing to be more confident.

---

## What the assistant won't do

- Give you a confident answer when the signal is weak — it will name the gap.
- Treat a product announcement as a confirmed market shift.
- Fill space with generic "AI is transforming X" commentary.
- Say "watch this space" without telling you what to watch and why.

If an output sounds hedged, it's usually because the corpus is genuinely sparse for that topic. Ask: `What would make you more confident here?` to surface what's missing.

---

## Workflow tips

**Start a session by anchoring domain and horizon.** "I'm working on [product]. Looking at the next 30 days." This prevents the assistant from defaulting to generic AI industry framing.

**Use Decision Brief for real decisions, not just research.** The mode is designed to produce something you can share — a brief a stakeholder can read in 2 minutes.

**Run Weekly Digest on Monday mornings.** One prompt gives you the week's signal picture for your domain. Use it to prep for planning or stakeholder calls.

**Don't skip the `What would change this read` line.** It's where the real intelligence lives — knowing what signal would flip a conclusion is as valuable as the conclusion itself.

---

## Getting help

If the Frontier MCP isn't returning results, ask the assistant: `Run a connection check on Frontier MCP and report which tools are responding.`

For workspace setup and API credentials, contact your Frontier Intelligence administrator.
