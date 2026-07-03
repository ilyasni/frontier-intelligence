# Платежи (YooKassa), НПД-чеки, подписки и юридический контур

> **Статус:** проект / план (НЕ реализовано). Документ описывает целевую SaaS-трансформацию Frontier Intelligence.
> **Дата:** 2026-06-26 · Сгенерировано многоагентным анализом текущей сборки. Индекс: [README](./README.md)

# Payments, Subscriptions, НПД Receipts & Legal Envelope (RU-first launch)

> Scope: monetization layer for a **solo-operated, самозанятый (НПД)** SaaS selling curated intelligence feeds. Covers YooKassa checkout + recurrent autopayments, НПД receipt auto-issuance, the 2.4 M RUB revenue-cap guardrail, 152-FZ launch gates, and a provider-abstraction seam so Stripe/Paddle slot in later. Grounded in the real schema (`storage/postgres/init.sql`) and existing FinOps assets.
>
> **Hard dependency:** every flow here assumes the *Identity & Auth* domain has shipped `accounts` + `api_keys` + auth on all three surfaces. Payments cannot launch on top of the current "workspace is client-controlled free text" model. This section owns `plans / subscriptions / payments / receipts / entitlements / revenue_ledger`; Identity owns `accounts / users / api_keys`.

## 0. Legal frame — what самозанятый (НПД) can and cannot do

Confirmed owner decision: operate as **самозанятый (налог на профессиональный доход, НПД)**. This shapes the whole billing design and must be stated up front in the doc.

| Constraint | Consequence for this product | Verdict |
|---|---|---|
| Annual gross revenue ceiling **2 400 000 RUB/calendar year** | If exceeded, НПД status is lost from the date of breach; remaining income that year is taxed under ОСНО/УСН. This is the #1 monetization risk. | Build a **hard capacity guardrail** (§3). |
| **Cannot hire employees** | The whole platform must stay automation-first and solo-operable. No "ops team" can be assumed in any runbook. | Honored: all flows below are webhook/cron-driven, zero manual receipt issuance. |
| **Service income is allowed**; **reselling goods is NOT** (перепродажа товаров запрещена под НПД) | Selling *access to an information/analytics service* (subscription to feeds) is a **service** (услуга/информационные услуги) — permitted. We are **not** reselling LLM tokens as goods; we sell our analytical service. | OK, but **flag for accountant** (§7) — phrase the offer/receipt as "информационно-аналитические услуги", not "продажа доступа к API третьих лиц". |
| Can sell to **физлица and юрлица/ИП** | B2B feed subscriptions to companies are allowed under НПД, **but** income from a current/recent (within 2 years) employer is excluded. The НПД cap and a separate юрлицо-receipt requirement apply. | OK — НПД cap math (§3) treats all gross identically. Receipts to юрлица need INN (§2). |
| **No 54-FZ ККТ / online cash register required** | НПД receipts ("чек") are issued via FNS, *not* via a fiscal cash register. This removes the entire ОФД/ККТ integration burden — **do not** build 54-FZ fiscalization now. | Use НПД receipt flow (§2). |
| Receipt obligation | A **чек** must be issued to the buyer for **every** payment (электронно — ссылка/QR). Non-issuance is a fineable violation. | Auto-issue on every `payment.succeeded` (§2). |

**Migration trigger (documented now, built later):** when projected calendar-year gross approaches the cap, migrate to **ИП на УСН** (or ООО). That migration *adds*: 54-FZ ККТ fiscalization (YooKassa can host an online касса / `receipt` object in the payment), possibly VAT (НДС) at higher turnover, страховые взносы, and lets you hire. The payment-provider abstraction (§5) and the `receipts` table's `provider` column are designed so this is a config/strategy swap, not a rewrite.

> ⚖️ **Accountant/lawyer verification required** (collected in §7): exact wording of the offer as "услуга", treatment of юрлицо sales under НПД, and whether любой specific source channel counts as "перепродажа".

---

## 1. Data model — plans, subscriptions, payments, entitlements

### 1.1 Design principles grounded in the codebase

- **Tenancy root is `workspaces` today** (`storage/postgres/init.sql:4`). Every core table FKs to it. But a *paying customer* is **not** a workspace — a customer subscribes to one or more **curated feeds** (the 5 existing workspaces: `disruption, ai_trends, ai_research, ai_products_media, design` per `config/workspaces.yml`). So billing hangs off a new **`accounts`** entity (owned by Identity domain) and **`entitlements`** map `account → workspace/feed` plus quota.
- **Money, not tokens.** The current FinOps stack (`worker/provider_budget_manager.py`) stores *billable-token counts* in Redis with ~3-day TTL — unusable as a billing ledger. We add a **durable Postgres `revenue_ledger`** for money in (customer payments) and reuse the existing `_normalize_pricing()` table (`admin/backend/services/wormsoft_limits.py:62`) only for **cost** attribution (money out), kept separate from revenue.
- **Idempotency everywhere.** YooKassa requires an `Idempotence-Key` on every create call and re-delivers webhooks; we model both with unique constraints.
- **All amounts in integer minor units (kopecks)** to avoid float drift. Currency column for future international.

### 1.2 DDL sketch — new migration `storage/postgres/migrations/20260701_billing_core.sql`

```sql
-- =====================================================================
-- Billing core. Depends on identity migration providing accounts(id).
-- Money is stored in integer kopecks. Currency ISO-4217.
-- =====================================================================

-- ---- PLANS: catalog of purchasable tiers (config-as-data) ----------
CREATE TABLE IF NOT EXISTS plans (
    id              TEXT PRIMARY KEY,                 -- 'feed_solo', 'feed_pro', 'feed_studio'
    name            TEXT NOT NULL,
    description     TEXT,
    price_minor     BIGINT NOT NULL,                  -- monthly price in kopecks
    currency        TEXT NOT NULL DEFAULT 'RUB',
    interval        TEXT NOT NULL DEFAULT 'month'
                      CHECK (interval IN ('month','year')),
    -- entitlement template applied on subscribe:
    included_feeds  JSONB NOT NULL DEFAULT '[]',      -- ['disruption','ai_trends'] or ['*'] for all curated
    max_custom_sources INTEGER NOT NULL DEFAULT 0,    -- self-serve RSS/web/api cap (0 = none)
    quota_limits    JSONB NOT NULL DEFAULT '{}',      -- {'mcp_calls_day':2000,'llm_tokens_month':...,'searches_day':...}
    is_public       BOOLEAN NOT NULL DEFAULT TRUE,    -- hide from signup when revenue cap nears
    sort_order      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---- SUBSCRIPTIONS: account's recurring commitment to a plan -------
CREATE TABLE IF NOT EXISTS subscriptions (
    id                  TEXT PRIMARY KEY,             -- uuid hex
    account_id          TEXT NOT NULL REFERENCES accounts(id),
    plan_id             TEXT NOT NULL REFERENCES plans(id),
    status              TEXT NOT NULL
                          CHECK (status IN ('incomplete','trialing','active',
                                            'past_due','canceled','paused')),
    -- recurrent autopayment binding (YooKassa saved-method):
    payment_provider    TEXT NOT NULL DEFAULT 'yookassa',
    provider_payment_method_id TEXT,                  -- YooKassa payment_method.id (saved card token)
    autopay_enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    current_period_start TIMESTAMPTZ,
    current_period_end   TIMESTAMPTZ,                 -- next charge due at this boundary
    cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
    canceled_at         TIMESTAMPTZ,
    -- buyer type drives receipt fields (физлицо vs юрлицо):
    buyer_kind          TEXT NOT NULL DEFAULT 'individual'
                          CHECK (buyer_kind IN ('individual','legal_entity')),
    buyer_inn           TEXT,                          -- required when legal_entity
    buyer_email         TEXT NOT NULL,                 -- receipt delivery target
    metadata            JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_account ON subscriptions(account_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_due
    ON subscriptions(current_period_end)
    WHERE status = 'active' AND autopay_enabled = TRUE;

-- ---- PAYMENTS: one row per YooKassa payment object ------------------
CREATE TABLE IF NOT EXISTS payments (
    id                  TEXT PRIMARY KEY,             -- internal uuid hex
    account_id          TEXT NOT NULL REFERENCES accounts(id),
    subscription_id     TEXT REFERENCES subscriptions(id),
    provider            TEXT NOT NULL DEFAULT 'yookassa',
    provider_payment_id TEXT UNIQUE,                  -- YooKassa payment.id (idempotent across webhook redelivery)
    idempotence_key     TEXT NOT NULL,                -- key WE sent on create (uuid)
    kind                TEXT NOT NULL
                          CHECK (kind IN ('initial','recurrent','manual_topup')),
    amount_minor        BIGINT NOT NULL,
    currency            TEXT NOT NULL DEFAULT 'RUB',
    status              TEXT NOT NULL
                          CHECK (status IN ('pending','waiting_for_capture',
                                            'succeeded','canceled')),
    is_recurrent_parent BOOLEAN NOT NULL DEFAULT FALSE, -- the payment that captured save_payment_method=true
    paid_at             TIMESTAMPTZ,
    cancellation_reason TEXT,
    raw_event           JSONB,                          -- last webhook body for audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider, idempotence_key)
);
CREATE INDEX IF NOT EXISTS idx_payments_account ON payments(account_id);
CREATE INDEX IF NOT EXISTS idx_payments_subscription ON payments(subscription_id);
CREATE INDEX IF NOT EXISTS idx_payments_succeeded
    ON payments(paid_at) WHERE status = 'succeeded';

-- ---- WEBHOOK INBOX: dedupe + replay safety -------------------------
CREATE TABLE IF NOT EXISTS payment_webhook_events (
    id              TEXT PRIMARY KEY,                  -- hash of (provider, provider_event_id|payment_id+status)
    provider        TEXT NOT NULL DEFAULT 'yookassa',
    event_type      TEXT NOT NULL,                     -- 'payment.succeeded' etc.
    provider_object_id TEXT,                           -- payment.id
    payload         JSONB NOT NULL,
    processed_at    TIMESTAMPTZ,
    process_status  TEXT NOT NULL DEFAULT 'received'
                      CHECK (process_status IN ('received','processed','error','ignored')),
    error_text      TEXT,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---- NPD RECEIPTS: один чек самозанятого на каждый успешный платёж --
CREATE TABLE IF NOT EXISTS receipts (
    id              TEXT PRIMARY KEY,
    payment_id      TEXT NOT NULL REFERENCES payments(id),
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    -- issuance channel: FNS "Мой налог" API, or YooKassa самозанятый receipt flow:
    provider        TEXT NOT NULL DEFAULT 'fns_lknpd'  -- 'fns_lknpd' | 'yookassa_selfemployed'
                      CHECK (provider IN ('fns_lknpd','yookassa_selfemployed')),
    receipt_status  TEXT NOT NULL DEFAULT 'pending'
                      CHECK (receipt_status IN ('pending','issued','canceled','error')),
    npd_receipt_id  TEXT,                              -- ФНС receiptId / approvedReceiptUuid
    receipt_url     TEXT,                              -- public чек link (given to buyer + stored)
    amount_minor    BIGINT NOT NULL,
    service_name    TEXT NOT NULL                      -- 'Информационно-аналитические услуги (подписка ...)'
                      DEFAULT 'Информационно-аналитические услуги',
    buyer_kind      TEXT NOT NULL DEFAULT 'individual',
    buyer_inn       TEXT,                              -- юрлицо INN if legal_entity
    issued_at       TIMESTAMPTZ,
    canceled_at     TIMESTAMPTZ,
    cancel_reason   TEXT,                              -- 'Возврат средств' | 'Чек сформирован ошибочно'
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (payment_id)                                -- exactly one чек per payment
);
CREATE INDEX IF NOT EXISTS idx_receipts_status
    ON receipts(receipt_status) WHERE receipt_status IN ('pending','error');

-- ---- ENTITLEMENTS: account -> feed access + per-feed quota ----------
-- This is what auth/MCP checks on every request. Derived from plan on
-- subscribe, but stored explicitly so grants survive plan edits & comps.
CREATE TABLE IF NOT EXISTS entitlements (
    id              TEXT PRIMARY KEY,
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),  -- the curated feed
    subscription_id TEXT REFERENCES subscriptions(id),         -- NULL = comp/manual grant
    access_level    TEXT NOT NULL DEFAULT 'read'
                      CHECK (access_level IN ('read','read_custom_sources')),
    quota_limits    JSONB NOT NULL DEFAULT '{}',               -- snapshot of plan quota at grant time
    status          TEXT NOT NULL DEFAULT 'active'
                      CHECK (status IN ('active','suspended','revoked')),
    granted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,                                -- = subscription period_end + grace
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, workspace_id)
);
CREATE INDEX IF NOT EXISTS idx_entitlements_account ON entitlements(account_id);
CREATE INDEX IF NOT EXISTS idx_entitlements_active
    ON entitlements(account_id, workspace_id) WHERE status = 'active';

-- ---- REVENUE LEDGER: durable money-IN, basis for НПД cap (§3) -------
CREATE TABLE IF NOT EXISTS revenue_ledger (
    id              TEXT PRIMARY KEY,
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    payment_id      TEXT REFERENCES payments(id),
    receipt_id      TEXT REFERENCES receipts(id),
    entry_type      TEXT NOT NULL
                      CHECK (entry_type IN ('charge','refund')),
    amount_minor    BIGINT NOT NULL,                  -- positive charge, negative refund
    currency        TEXT NOT NULL DEFAULT 'RUB',
    recognized_at   TIMESTAMPTZ NOT NULL,             -- = paid_at; the date that counts toward НПД year
    tax_year        INTEGER NOT NULL,                 -- EXTRACT(YEAR FROM recognized_at AT TIME ZONE 'Europe/Moscow')
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_revenue_ledger_year ON revenue_ledger(tax_year);
```

Quotas (`plan.quota_limits` → `entitlement.quota_limits`) are enforced by the **Metering/Quota** domain. This domain only *defines* and *snapshots* them; it does not implement the leaky-bucket. The natural enforcement point is the same Redis machinery in `worker/provider_budget_manager.py` once `workspace_id`/`account_id` is finally propagated (it already has a `cost_workspace` scope, see §6).

---

## 2. YooKassa integration + НПД receipt auto-issuance

### 2.1 Why a payment-provider service module (new code)

There is **no payment code anywhere** today (verified — zero YooKassa/Stripe references). Build a new package mirroring the existing service-module convention (`admin/backend/services/*.py`, async `httpx`, settings from `shared.config`):

```
admin/backend/services/payments/
    base.py            # PaymentProvider Protocol (§5) + dataclasses
    yookassa.py        # YooKassaProvider — the only impl now
    receipts_npd.py    # NpdReceiptIssuer Protocol + FNS/YooKassa-self-employed impls
admin/backend/routers/
    billing.py         # customer-facing: create-checkout, list plans, subscription self-service
    payment_webhooks.py# POST /api/payments/webhook/yookassa  (signature/IP-allowlisted)
```

YooKassa's REST API: create payment = `POST /v3/payments` with `Idempotence-Key` header and Basic auth (`shopId:secretKey`); webhooks deliver `payment.succeeded`, `payment.canceled`, `refund.succeeded`. Confirm exact field shapes against current YooKassa docs at build time (use Context7/their SDK) — do not hardcode from memory.

### 2.2 Initial checkout + save card for autopayments

YooKassa recurrent payments work by: (1) first payment created with `save_payment_method: true` and `confirmation.type: redirect`; (2) on success the webhook returns `payment_method.id` + `payment_method.saved: true`; (3) subsequent charges are server-initiated `POST /v3/payments` with `payment_method_id` + `capture: true` (no user redirect).

```
SEQUENCE — initial subscribe
────────────────────────────
Customer            Billing API (admin)         YooKassa            FNS/НПД
   │  POST /api/billing/checkout {plan_id}       │                    │
   ├───────────────►│                            │                    │
   │                │ create subscription(status=incomplete)          │
   │                │ idem=uuid; INSERT payments(kind=initial,pending)│
   │                │ POST /v3/payments  (Idempotence-Key: idem,      │
   │                │   amount, save_payment_method=true,             │
   │                │   confirmation.redirect, metadata.payment_id)   │
   │                ├───────────────────────────►│                    │
   │                │   {id, confirmation_url}    │                    │
   │  confirmation_url (302)                      │                    │
   │◄───────────────┤                            │                    │
   │  ── pays on YooKassa hosted page ──►         │                    │
   │                │   webhook payment.succeeded │                    │
   │                │◄───────────────────────────┤                    │
   │                │ dedupe in payment_webhook_events                 │
   │                │ payments.status=succeeded; store payment_method_id
   │                │ subscriptions.status=active; provider_payment_method_id=...
   │                │ current_period_end = now()+1 month              │
   │                │ INSERT revenue_ledger(charge, tax_year)         │
   │                │ INSERT entitlements from plan.included_feeds    │
   │                │ enqueue receipt issuance ──────────────────────►│
   │                │                            │  issue НПД чек      │
   │                │                            │   receipts.issued, receipt_url
   │  receipt link emailed / shown               │                    │
```

### 2.3 Recurrent charge (monthly, cron-driven — solo-operable)

A scheduler job (extend the existing APScheduler in `admin/backend/scheduler.py`, which already runs locked async jobs) selects subscriptions due via `idx_subscriptions_due` and server-charges:

```
SEQUENCE — recurrent monthly charge (cron: every 15 min, idempotent)
────────────────────────────────────────────────────────────────────
for sub in subscriptions where status='active' and autopay
          and current_period_end <= now() and not already-charged-this-period:
   idem = sha256(sub.id + period_start)          # deterministic → safe retries
   INSERT payments(kind=recurrent, pending, idempotence_key=idem)
   POST /v3/payments { amount, payment_method_id=sub.provider_payment_method_id,
                       capture:true, Idempotence-Key: idem }
   # response is sync 'succeeded' OR webhook confirms; both paths converge on
   # the same handler (status=succeeded → ledger + receipt + extend period)
   on succeeded: current_period_end += 1 month; entitlements.expires_at extended
   on canceled (insufficient funds / card expired):
        subscriptions.status='past_due'; dunning email; retry schedule (T+1,T+3,T+5d)
        after final retry → status='canceled', entitlements.status='suspended'
```

> ⚠️ **CBR autopayment rules / СБП:** RU recurring-card rules and YooKassa autopayment availability depend on the merchant's YooKassa contract. **Flag for owner verification** (§7): confirm recurrent (автоплатёж) is enabled on the самозанятый/ИП YooKassa account type — some самозанятый YooKassa products are payout-only and may not support saved-card autopay, which would force "renew = new checkout link emailed monthly" as the fallback. Design supports both: if `autopay_enabled=false`, the cron emails a fresh `confirmation_url` instead of charging.

### 2.4 НПД receipt issuance — the самозанятый чек

**Two viable channels; pick by what YooKassa actually offers on the самозанятый account:**

1. **`yookassa_selfemployed`** — YooKassa's самозанятый product can auto-register the НПД чек with ФНС for each payment when the account is bound to "Мой налог". Preferred because it is one integration and no separate ФНС creds. The `receipt_url` comes back on/after the payment object.
2. **`fns_lknpd`** — direct ФНС "Мой налог"/ЛКНП API (`POST .../income` → `receiptId` → public чек URL; `POST .../cancel` to аннулировать). Used if YooKassa does not auto-issue. Requires a ФНС partner/ЛКНП token.

Both are modeled identically behind `NpdReceiptIssuer` so the `receipts.provider` column is the only thing that differs:

```python
class NpdReceiptIssuer(Protocol):
    provider_name: str
    async def issue(self, *, payment: PaymentRecord, account: AccountRecord,
                    service_name: str) -> NpdReceipt: ...      # -> npd_receipt_id, receipt_url
    async def cancel(self, *, receipt: ReceiptRecord,
                     reason: str) -> None: ...                  # 'Возврат средств' | 'Чек ошибочный'
```

**Issuance rules baked into the design:**
- Auto-issue on **every** `payments.status=succeeded` (legal obligation), idempotent via `receipts.UNIQUE(payment_id)`. A background retrier (cron, mirrors `trend_alerts` retry pattern in `admin/backend/services/trend_alerts.py`) drains `receipts.receipt_status IN ('pending','error')`.
- `service_name` = "Информационно-аналитические услуги (подписка <plan.name>)" — a **service**, never "перепродажа доступа к API". (Legal framing, §0.)
- **Refund → cancel чек.** On `refund.succeeded`, issue `revenue_ledger(refund, negative)` **and** call `receipts.cancel(reason='Возврат средств')`; the canceled amount must be subtracted from the НПД-cap running total (§3).
- **юрлицо buyer:** when `subscriptions.buyer_kind='legal_entity'`, the чек must carry the buyer INN (`buyer_inn`) — ФНС distinguishes income from физлица vs юрлица. Capture INN at checkout for B2B.

> ⚖️ **Accountant verification:** (a) whether YooKassa's самозанятый product auto-files the чек with ФНС or whether you must also confirm it in "Мой налог"; (b) the exact OKVED/service wording; (c) handling of the YooKassa commission (the чек amount is the gross the buyer paid, not net of YooKassa fee — НПД is taxed on gross income received).

---

## 3. Revenue-cap guardrail (2.4 M RUB) — never breach involuntarily

This is the single most important control for an НПД business and is **fully automatable** (no employee needed).

### 3.1 Source of truth
`revenue_ledger` (§1.2): `charge` rows are positive at `recognized_at = paid_at`; `refund` rows negative. `tax_year` is computed in **Europe/Moscow** (the tax calendar is Russian local time, not UTC — important near year boundaries).

```sql
-- Year-to-date gross recognized (the number that counts toward 2.4M)
SELECT COALESCE(SUM(amount_minor),0) AS ytd_minor
FROM revenue_ledger
WHERE tax_year = EXTRACT(YEAR FROM (now() AT TIME ZONE 'Europe/Moscow'))::int;
```

### 3.2 Three-stage capacity model (config-driven thresholds)

| Stage | Trigger (YTD gross) | Automated action |
|---|---|---|
| **Green** | < 80 % (1 920 000 RUB) | Normal operation. |
| **Amber (soft alert)** | ≥ 80 % | Prometheus alert via existing Alertmanager→Telegram path (`admin/backend/services/telegram_alerts.py`); set `plans.is_public=false` on the lowest-tier/highest-volume plan to slow new low-ASP signups; owner reviews. |
| **Red (hard stop)** | ≥ ~95 % (configurable `NPD_CAP_HARD_STOP_MINOR`) **OR** a single new charge would cross 2.4 M | **Block new subscription creation** (`POST /api/billing/checkout` returns 409 `revenue_cap_reached`); **disable autopay charges that would cross the line** (skip/queue the recurrent charge, or pro-rate — see below); existing read access continues so paying customers are not cut off. |

### 3.3 Pre-charge admission check (the involuntary-breach prevention)

Every payment-creating call (initial checkout *and* recurrent cron) runs an **admission check inside the same DB transaction** that inserts the `payments` row:

```
ytd = sum(revenue_ledger this tax_year)
if ytd + this_charge_amount > NPD_CAP_HARD_STOP_MINOR:
    refuse: do NOT call YooKassa. Mark subscription 'paused', alert owner.
```

Because the check precedes the YooKassa call, **the cap can never be crossed by an automated charge.** A near-cap recurrent renewal is paused (customer keeps access through grace, gets notified) rather than charged — protecting НПД status is prioritized over one month's revenue.

### 3.4 Capacity planning (so Red is rarely hit)
With the НПД cap at 2.4 M RUB/yr = **200 000 RUB/mo MRR ceiling**, the business is intentionally a **small number of higher-ASP B2B feed subscriptions**, not high-volume self-serve (honors the strategic implication). E.g. 200 000/mo ÷ ~5 000–20 000 RUB ASP ⇒ ~10–40 active subscriptions. The admin dashboard surfaces `ytd_minor`, projected EOY gross (linear + trailing-3-mo run-rate), and "months of runway before cap" so the owner sees the wall early.

### 3.5 Migration trigger to ИП/ООО
When projected EOY gross ≥ 2.0 M **for two consecutive months**, emit a `migration_recommended` alert. The doc records what changes on migration: register ИП → choose УСН (6% доходы) → **add 54-FZ ККТ** (YooKassa online-касса: payments gain a `receipt` object with item VAT tags) → possibly VAT above УСН limits → страховые взносы → ability to hire. The `payments.provider` + `receipts.provider` columns and §5 abstraction absorb the ККТ addition as a new receipt strategy (`yookassa_kkt`) without schema change.

---

## 4. 152-FZ data-protection launch gates

These **block launch** and are independent of payment code but must ship with it.

| Gate | What's needed | Codebase touchpoint |
|---|---|---|
| **Consent + Privacy Policy + Оферта** | Public "Политика обработки ПДн", "Согласие на обработку ПДн", and the public offer (договор-оферта) that frames the product as информационно-аналитические услуги. Checkout records consent (timestamp, policy version) — add `accounts.consent_version` / `consent_at` (Identity domain) and gate `POST /api/billing/checkout` on it. | New static pages + checkbox in `billing.py` checkout. |
| **РКН operator notification** | Уведомление в Роскомнадзор about processing ПДн (email, payment metadata, buyer INN). Must be filed before/at launch. | Process step — **flag for owner** (§7). |
| **Data residency / cross-border PDn** | RU personal data (Telegram authors, buyer email/INN) must be **stored in RU** (already true: Cloud.ru S3 ru-central-1, Postgres on RU server) and **not shipped to non-RU LLM providers** (OpenRouter/Polza are non-RU) without pseudonymization. Today the router (`worker/llm_router_client.py`) can route Telegram-derived content abroad with no gating. | **Add a PDn gate** to the routing policy: content from Telegram sources must route only to RU providers (GigaChat) **or** be pseudonymized (strip @handles/names) before non-RU egress. Embeddings are already GigaChat-only (RU) — good. |
| **Buyer PII minimization** | Store only email + (for юрлица) INN; never store card PAN — YooKassa is PCI scope, we hold only `payment_method_id` token. Card data never touches our DB. | Enforced by design — `subscriptions.provider_payment_method_id` is a token, not a card. |
| **Erasure / retention** | On account deletion, erase `accounts`/`subscriptions` PII; but **`receipts` and `revenue_ledger` must be retained** for tax/НПД records (financial documents have a statutory retention). Soft-delete account PII, keep financial rows with `account_id` anonymized to a pseudonym. | New erasure job; flag retention period for lawyer (§7). |

> ⚖️ **Lawyer verification:** РКН notification scope, statutory retention period for НПД financial records, and whether buyer email alone triggers full operator obligations.

---

## 5. International-phase abstraction (build the seam, implement only YooKassa)

Define provider interfaces **now** so Stripe/Paddle (Paddle = merchant-of-record, handles foreign VAT) slot in without rework. Mirror the existing `ProviderAdapter` Protocol pattern already used for LLM providers in `shared/llm_control_plane.py:555` — the team is fluent in this shape.

```python
# admin/backend/services/payments/base.py
class PaymentProvider(Protocol):
    provider_name: str   # 'yookassa' | 'stripe' | 'paddle'

    async def create_checkout(self, *, amount_minor: int, currency: str,
                              idempotence_key: str, save_method: bool,
                              return_url: str, metadata: dict) -> CheckoutResult: ...
        # -> {provider_payment_id, confirmation_url}

    async def charge_saved_method(self, *, amount_minor: int, currency: str,
                                  payment_method_id: str,
                                  idempotence_key: str, metadata: dict) -> ChargeResult: ...

    async def parse_webhook(self, *, headers: dict, body: bytes) -> WebhookEvent: ...
        # verifies signature/IP; returns normalized {event_type, provider_payment_id, status, amount}

    async def refund(self, *, provider_payment_id: str,
                     amount_minor: int, idempotence_key: str) -> RefundResult: ...
```

Receipts are a **separate** Protocol (`NpdReceiptIssuer`, §2.4) because tax compliance is jurisdiction-bound: НПД чек is RU-only; for international, Paddle-as-MoR issues invoices itself and the receipt strategy becomes `paddle_mor` (no FNS call). The `payments.provider` and `receipts.provider` columns are the runtime switch. **Do not** build Stripe/Paddle now — only ensure `currency` is carried everywhere (it is) and no RUB/kopeck assumption is hardcoded in business logic.

---

## 6. Reuse of existing FinOps assets (cost side — money OUT)

Revenue (above) and **cost** must be reconciled to know per-customer margin, but they are different ledgers:

- **Per-tenant cost attribution is dead code today.** `ProviderExecutionRequest.workspace_id` exists (`shared/llm_control_plane.py:545`) but is **never set** — every `ProviderExecutionRequest(...)` construction in `worker/llm_router_client.py` (lines 459, 915, 2315) omits it, so `receipt.workspace_id` is always `""` and the `cost_workspace` Redis scope in `provider_budget_manager.py:244` never populates. The **Metering** domain must thread `workspace_id` (and ideally `account_id`) into these constructions; once done, `snapshot_costs(workspace_ids=[...])` (`provider_budget_manager.py:824`) yields per-feed cost for free.
- **Token→money conversion already has its price table but it's unused.** `_normalize_pricing()` (`admin/backend/services/wormsoft_limits.py:62`) is the only real per-model input/output/cache RUB price table and is fetched but never applied. Wire it into `estimate_cost`/`actual_cost` so `revenue_ledger` (money in) can be set against an LLM **cost** figure (money out) for margin. This is the **only** place token→RUB conversion should live; keep it out of the НПД revenue path (the cap is about gross *income*, not cost).
- **Durable cost ledger gap:** Redis cost keys have ~3-day TTL (`provider_budget_manager.py` `ttl = 3*24*3600`). For monthly margin reporting, a small cron should roll daily `snapshot_costs` into a durable `cost_ledger` Postgres table (out of scope here, owned by Metering; flagged as a dependency).

---

## 7. Sequenced change list (effort tags)

| # | Change | Effort | Depends on |
|---|---|---|---|
| 1 | Migration `20260701_billing_core.sql`: `plans, subscriptions, payments, payment_webhook_events, receipts, entitlements, revenue_ledger` (§1.2). Seed 2–3 plans. | **M** | Identity `accounts` table |
| 2 | `services/payments/base.py` + `yookassa.py`: `PaymentProvider` impl (create_checkout, charge_saved_method, parse_webhook+signature/IP verify, refund). | **L** | YooKassa account + keys in `.env` |
| 3 | `routers/billing.py`: list plans, `POST /checkout`, subscription self-service (cancel-at-period-end, change plan), consent capture. Auth-gated. | **M** | #1, #2, auth middleware |
| 4 | `routers/payment_webhooks.py`: idempotent inbox (`payment_webhook_events`), state machine → `payments`/`subscriptions`/`entitlements`/`revenue_ledger`. | **L** | #2 |
| 5 | Recurrent-charge cron in `admin/backend/scheduler.py` (deterministic idempotence key, dunning, past_due→canceled). | **M** | #2, #4 |
| 6 | `services/payments/receipts_npd.py` + receipt retrier cron: auto-issue НПД чек on succeeded, cancel on refund, store `receipt_url`. | **L** | #4; YooKassa-selfemployed *or* FNS creds |
| 7 | Revenue-cap guardrail: pre-charge admission check, `is_public` toggling, 80%/95% Prometheus alerts via existing Alertmanager path, EOY projection + runway on admin dashboard. | **M** | #1, #4 |
| 8 | 152-FZ gates: privacy/consent pages + `consent_version` checkout gate; **PDn routing gate** in `worker/llm_router_client.py` (Telegram-derived content → RU providers or pseudonymize before non-RU egress). | **L** | routing policy |
| 9 | Thread `workspace_id`/`account_id` into `ProviderExecutionRequest(...)` to revive per-tenant cost; wire `_normalize_pricing()` into `estimate_cost`; durable `cost_ledger` rollup for margin. | **M** | Metering domain coordination |
| 10 | International seam: ensure `currency` carried end-to-end, no kopeck hardcodes in logic (interface already defined in #2). Stripe/Paddle impls **deferred**. | **S** | #2 |

**Net effort:** ~3 L + 4 M + 1 S for a launchable RU billing layer, excluding the Identity/auth prerequisite (separate domain) which is a hard blocker.

---

## Открытые решения по этому разделу

- YooKassa autopay capability on a самозанятый/ИП account: confirm whether saved-card recurrent (автоплатёж) is contractually enabled. RECOMMENDATION: design ships both paths; default to true but fall back to monthly emailed checkout link if the YooKassa самозанятый product is payout-only. Owner must verify with YooKassa before committing the recurrent cron (change #5).
- НПД receipt channel: yookassa_selfemployed (one integration, YooKassa auto-files the чек with ФНС) vs fns_lknpd (direct 'Мой налог'/ЛКНП API). RECOMMENDATION: prefer yookassa_selfemployed to stay solo-operable; keep fns_lknpd behind the same NpdReceiptIssuer Protocol as fallback. Verify with accountant which actually files with ФНС.
- Revenue-cap hard-stop threshold: recommend NPD_CAP_HARD_STOP at ~95% (2.28M RUB) with the pre-charge admission check refusing any charge that would cross 2.4M. Owner to confirm the buffer; lower it if conservative.
- Whether a recurrent charge that would cross the cap should be PAUSED (recommended — protects НПД status, customer keeps grace access) vs pro-rated to the remaining headroom (more revenue, more complexity). RECOMMENDATION: pause + notify.
- Plan/ASP shape: given the 200k RUB/mo MRR ceiling, recommend a small number of higher-priced B2B feed tiers (~5–20k RUB) over high-volume low-ASP self-serve. Owner to set actual prices in the plans seed.
- Accountant/lawyer items that gate launch: (a) exact 'информационно-аналитические услуги' wording so no channel is deemed 'перепродажа товаров'; (b) юрлицо sales under НПД and INN capture; (c) РКН operator notification scope; (d) statutory retention for receipts/revenue_ledger vs 152-FZ erasure; (e) НПД taxed on gross received incl. YooKassa commission. RECOMMENDATION: book one consult covering all five before charging the first ruble.

## Зависимости от других разделов

- Identity & Auth — MUST ship first: accounts/users/api_keys tables, auth middleware on all 3 surfaces (MCP REST :8100, gateway :8102, Admin :8101), and replace client-controlled free-text workspace with server-resolved entitlement checks. Payments cannot launch without this. This domain depends on accounts(id) as the FK target for subscriptions/payments/entitlements/revenue_ledger.
- Metering & Quota — owns enforcement of plan.quota_limits/entitlement.quota_limits via the Redis leaky-bucket; must thread workspace_id/account_id into ProviderExecutionRequest (currently dead) and wire _normalize_pricing() into cost estimation so per-customer margin (revenue vs cost) is computable. Owns the durable cost_ledger rollup.
- Tenancy isolation — entitlements gate read access to curated feeds (workspaces); relies on the workspace_id Qdrant payload filter and Neo4j root scoping already present, plus the missing Postgres RLS, to ensure a paying account only reads feeds it is entitled to.
- Ops/Reliability — recurrent-charge cron and receipt-retrier run on the single-process APScheduler in admin/backend/scheduler.py; if that scheduler is down, charges/receipts stall. Needs the HA/restart-resilience work so billing crons are not a single point of failure for revenue and legal receipt obligations.
- Observability — revenue-cap alerts (80%/95%) and dunning failures reuse the Alertmanager→Telegram path in admin/backend/services/telegram_alerts.py; needs per-account/per-tenant metric dimensions added (currently absent) to surface MRR, churn, and НПД runway.

