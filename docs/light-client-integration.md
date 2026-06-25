# Making CAS Trustless — An Onboarding Guide

> Goal of this doc: explain, from the ground up, **what we're trying to do and why**, so
> that someone new to Espresso/rollups/light clients can follow the design discussion.
> No prior knowledge assumed.

---

## 1. The cast of characters

Before anything else, let's name the players.

```
   ┌──────────────┐         ┌──────────────────────┐         ┌──────────────────┐
   │   ROLLUP     │         │         CAS          │         │     ESPRESSO     │
   │  (Arbitrum   │ <─────> │  (Chain Adjacent     │ <─────> │     NETWORK      │
   │   Nitro)     │         │      Service)        │         │  HotShot consensus│
   └──────────────┘         └──────────────────────┘         └──────────────────┘
     the customer            the middleman (us)               the data source
```

- **Espresso Network** — a network running the **HotShot consensus protocol** (a
  Byzantine-fault-tolerant protocol derived from the **HotStuff** paper). A set of
  validators run HotShot to agree on finalized blocks of data and make them available. The
  key property for us: a block is "finalized" once a **supermajority of validators have
  signed off on it** (a quorum certificate). Those signatures are exactly what we'll later
  verify — that's where trustlessness comes from.

- **Rollup (Arbitrum Nitro)** — a blockchain that posts its transaction data to Espresso so
  that HotShot consensus finalizes it and makes it available, but doesn't want to learn
  Espresso's internals to do so.

- **CAS (us)** — the **middleman**. We sit between the rollup and Espresso and translate
  between them, in both directions.

---

## 2. What CAS actually does

CAS has two jobs — think of it like a translator standing at a border crossing handling
traffic in both directions:

```
                          ┌─────────────────────────────────────┐
                          │                CAS                  │
                          │                                     │
   ROLLUP   ──messages──> │  OUTBOUND: batch them, submit to    │ ──> ESPRESSO
            (feed)        │           Espresso, track finality  │
                          │                                     │
   ROLLUP   <──txns────── │  INBOUND:  pull our txns back out,  │ <── ESPRESSO
                          │           filter, hand to rollup    │
                          └─────────────────────────────────────┘
```

- **OUTBOUND** (submission): the rollup produces messages → CAS bundles them into Espresso
  transactions → submits them → waits until they're finalized.
- **INBOUND** (consumption): CAS reads finalized transactions back out of Espresso, keeps
  only the ones belonging to our rollup, and feeds them to the rollup.

**This whole project is about the INBOUND path.** That's the one we're changing.

> CAS runs inside a secure hardware enclave (AWS Nitro Enclave) — a locked box that even
> the operator can't peek inside. That detail matters later for *where secrets/config come from*.

---

## 3. How CAS reads data today, and the problem with it

To read transactions out of Espresso, CAS talks to something called the **Query Service**.

```
                         "give me my rollup's txns
                          for blocks 100 to 200"
   ┌───────────┐  ───────────────────────────────────>  ┌───────────────┐
   │    CAS    │                                          │ QUERY SERVICE │
   │           │  <───────────────────────────────────   │  (one server) │
   └───────────┘     here are the transactions             └───────────────┘
                     (CAS just believes them ✓)
```

The Query Service is a convenient HTTP API that hands back transactions. **The problem:
CAS just trusts whatever it says.**

That's a real risk. The Query Service is *one server*. If it's buggy, hacked, or malicious,
it could hand CAS:
- transactions that were never actually finalized,
- fake transactions that no one submitted,
- a doctored version of what really happened.

CAS would forward all of that to the rollup as if it were gospel. **We don't want to have
to trust a single server like that.** That's the entire motivation:

> **Goal: stop trusting the Query Service for the data we consume.**

A telling detail we discovered: the Query Service *does* already send along a "proof"
alongside the data — but **CAS currently throws that proof away without checking it.** So
right now the trust is 100% blind.

---

## 4. The idea: verify instead of trust (the "light client")

Here's the key concept. Espresso is run by many validators who **cryptographically sign**
what they agree is final. Those signatures are like a wax seal that can't be forged — but
only if you know who the legitimate validators are.

A **light client** is a small piece of software that **checks those seals itself** instead
of trusting a server's word. It's called "light" because it doesn't re-run the whole
network — it just verifies the cryptographic proofs.

```
   WITHOUT light client (today):          WITH light client (goal):

   Server: "trust me, here's            Server: "here's the data + proofs"
            the data"                       │
       │                                    ▼
       ▼                              ┌──────────────┐
   CAS believes it                    │ light client │  checks the seals:
   blindly  ✗                         │  inside CAS  │  - are these signed by the
                                       └──────────────┘    real validators?
                                              │            - do the proofs add up?
                                              ▼          if anything is off → REJECT ✓
                                       CAS only accepts
                                       verified data
```

Now even if the server lies, CAS catches it, because the math won't check out.

### Where does trust come from then? The "root of trust"

There's a catch. To verify "are these the *real* validators?", the light client needs a
starting point it already trusts — like knowing the official guest list before checking
signatures at the door. That starting point is called **Genesis**: the initial, known-good
list of validators (the "stake table").

```
   Genesis (trusted starting list)
        │
        ▼
   verify the next set of validators ──> verify the next ──> ... (chains forward over time)
```

We hand the light client a trusted **Genesis** once, and from there it can verify
everything that follows — including how the validator set changes over time. **Genesis is
the one thing we still have to get right and trust.** If Genesis is wrong, everything built
on it is wrong. So *where CAS gets its Genesis* is a critical open question (see §7).

---

## 5. The important subtlety (this is where the original ticket was wrong)

The Espresso team's `light-client` crate has a few layers. It's easy to grab the wrong one.

```
   ┌─────────────────────────────────────────────────────────────┐
   │  LightClient   ← the SMART layer. Verifies everything         │  ✅ what we want
   │  ──────────────────────────────────────────────────────────  │
   │  FallbackClient  ← just tries several servers for reliability │  ⚠️ no verification!
   │  ──────────────────────────────────────────────────────────  │
   │  QueryServiceClient  ← raw HTTP call to one server            │
   └─────────────────────────────────────────────────────────────┘
```

The original task suggested using **`FallbackClient`** directly. But `FallbackClient` is
*not* the verifier — it just calls several servers in turn so that if one is down, it tries
another. It improves **reliability**, not **trust**. Using it alone would *not* fix our
problem.

The proposed code also had a logic flaw: it verified transactions against a "header" that
it fetched *from the same untrusted server*. That's circular — like checking a signature
against a guest list that the suspect handed you.

```
   ✗ THE TRAP:                              ✓ THE FIX (LightClient):

   header  ← from untrusted server          header  ← VERIFIED against validators first
   proof   ← from untrusted server          proof   ← then checked against the
   "verify proof against header"                       already-verified header
   → proves nothing (both could be fake)    → genuinely trustless
```

**`LightClient` is the layer that does it right.** It verifies the headers against the
validator signatures *first*, then checks the transaction proofs against those verified
headers. We use `FallbackClient` *underneath* `LightClient` to get reliability too:

```
   LightClient                         ← verifies (trust)
     wraps  FallbackClient             ← failover across servers (reliability)
              wraps  [ QueryServiceClient(serverA), QueryServiceClient(serverB) ]
```

Best of both worlds.

---

## 6. What changes inside CAS (the concrete swap)

The good news: the part of CAS that reads data is **tiny and self-contained**. Only one
file (`src/streamer/streamer.rs`) and basically two spots do the reading. Here's the
before/after at a high level:

```
   BEFORE                                    AFTER
   ──────                                    ─────
   EspressoClient                            LightClient (for reading)
     .fetch_latest_hotshot_block_height()      .block_height()
     .fetch_namespace_transactions_in_range()  .fetch_namespaces_in_range()
        → returns UNVERIFIED txns                 → returns VERIFIED txns
```

The shape of the returned data lines up nicely with what CAS already expects, so the rest
of the pipeline (parsing the transactions into rollup messages, queueing them, handing them
to the rollup) **barely needs to change**.

**What stays exactly as it is** (these don't need verification):

| Method | Why it stays |
|---|---|
| `submit_transaction` | It's a *write* (outbound). Nothing to verify. |
| `fetch_transaction_by_hash` | Only used to confirm *our own* just-submitted tx landed. We're reading back our own write, not consuming someone else's data — so trust isn't the concern. |
| `fetch_limits` | Just told us the max range size; we can manage that ourselves. |

So after this, CAS has **two clients side by side**: the old one for sending/confirming our
own transactions, and the new `LightClient` for trustlessly reading data back.

```
                    ┌──────────────────────── CAS ────────────────────────┐
   ESPRESSO  <────  │  EspressoClient   →  submit + confirm-our-own-tx     │
                    │                                                       │
   ESPRESSO  ────>  │  LightClient      →  read txns (VERIFIED) ✅          │
                    └───────────────────────────────────────────────────────┘
```

---

## 7. The open questions before we can build it

These are the things we still need answers to. They're not blockers to *understanding* the
plan, but they decide *how* we build it.

1. **Where does CAS get its `Genesis` (the trusted validator list)?**
   This is the root of trust — the one thing we must get right. Likely it needs to be
   delivered as trusted configuration into the enclave, the same way the server URL is.
   *Most important open question.*

2. **Do the software versions line up?**
   The light-client code must speak the exact same "dialect" (same dependency versions) as
   the rest of CAS, or the types won't fit together. Needs a compatibility check.

3. **Startup cost.**
   To verify how the validator set evolved, the light client may have to replay history
   from Genesis the first time it starts. We'll want to save its progress to disk (inside
   the enclave's storage) so it doesn't redo that work on every restart.

4. **Multiple servers.**
   The reliable `FallbackClient` wants a *list* of server URLs; CAS config currently holds
   just one. Minor config change.

---

## 8. One-paragraph summary (for when someone asks you in the hallway)

> CAS reads our rollup's transactions out of Espresso by trusting a single Query Service
> server — which is risky. We're swapping the read path to use Espresso's **light client**,
> which cryptographically verifies that the data was really finalized by Espresso's
> validators (anchored to a trusted starting "Genesis" validator list). We keep the old
> client only for *sending* our own transactions. Net effect: a malicious or buggy server
> can no longer feed our rollup fake data.
