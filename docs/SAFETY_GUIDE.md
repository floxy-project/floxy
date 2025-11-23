## Floxy Safety Guide

*(Engineering Safety Guide for Early-Stage Workflow Engine)*

Floxy is a lightweight workflow engine built on top of PostgreSQL. It is powerful, but it is **not battle-tested**, may contain undiscovered failure modes, and is still evolving. This guide explains how to use Floxy responsibly and reduce operational risk.

Use this document as a checklist before integrating Floxy into any environment beyond local development.

---

## 1. Do Not Treat Floxy as a Black Box

Floxy is not a mature enterprise orchestrator. If you use it, you must understand:

* how steps are scheduled
* how workers process tasks
* how retry and compensation work
* how race conditions in fork/join are handled
* how rollback/compensation semantics work
* how state persistence is implemented in PostgreSQL

If you don’t have time to read the internals — **do not deploy Floxy in anything important**.

---

## 2. Run Floxy Only on Fully Reliable PostgreSQL Instances

Floxy inherits all guarantees — and all failure modes — of your PostgreSQL environment.

You *must* ensure:

* WAL archiving is enabled
* Durability is not compromised
* Failover logic is correct
* Monitoring for replication lag is in place
* Storage is not ephemeral
* Backups exist and are tested

If your Postgres cluster is fragile, **Floxy will amplify every weakness**.

---

## 3. Do Not Use Floxy Without Observability

You need at minimum:

* Prometheus metrics
* Structured logs from engine + workers
* Dashboards for workflow health
* Alerts for stuck workflows, high retries, compensation loops, queue starvation

Blind usage = unsafe usage.

---

## 4. Validate All Step Handlers Thoroughly

Floxy does not protect you from:

* non-idempotent handlers
* hidden side effects
* unsafe retry semantics
* blocking handlers
* panics

You must enforce idempotency, bounded execution time, retry safety, and compensation correctness.

---

## 5. Always Provide Compensation for External Side Effects

Examples:

* inventory reservations
* billing operations
* sending emails
* pushing to external APIs

If a step mutates external state and you don’t provide compensation, you’re choosing inconsistency by design.

---

## 6. Test Workflows Under Chaos Before Using Them Anywhere Near Production

Floxy specifically requires testing around:

* step crashes
* worker restarts
* partial rollbacks
* fork/join races
* compensation overlap
* database failover
* latency spikes
* message loss simulations

If you haven't run chaos tests — you don't know how Floxy behaves.

---

## 7. Avoid Complex Fork/Join Patterns Until You Understand Dynamic Join

Floxy supports parallel execution, but conditional branching creates uncertainty in join points.

Start with linear flows. Add fork/join only after deep testing.

---

## 8. Avoid Using Floxy in Regulated or Mission-Critical Systems

Floxy has no certifications, audit guarantees, or compliance story. Avoid using it in:

* banking
* medical systems
* safety-critical environments
* anything requiring strict consistency

---

## 9. Expect API Changes and Schema Migrations

Floxy is evolving. You should expect:

* breaking API updates
* schema changes
* shifts in compensation semantics
* new failure modes uncovered

If you need stability, freeze Floxy at a specific version.

---

## 10. Review Workflow State Manually After Critical Failures

Floxy cannot infer business intent. After severe failures:

* inspect logs
* check compensation order
* validate external side effects
* confirm terminal states

Workflow engines automate coordination — **not judgement**.

---

## 11. Own Your Deployment and Operational Risk

Floxy’s maintainers provide the tool **as-is**, under MIT. There are no warranties.

You accept operational responsibility, testing responsibility, correctness responsibility, and data safety responsibility.

---

## Summary

Floxy can be used safely, but only if:

* you understand it deeply
* you test it aggressively
* your handlers are idempotent
* your database is reliable
* your monitoring is real
* your workflow logic is disciplined

If you want a workflow engine you can deploy blindly — use Temporal.

If you want a workflow engine you fully understand and fully control — use Floxy, but use it **responsibly**.
