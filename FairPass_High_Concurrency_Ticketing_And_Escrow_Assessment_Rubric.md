# FairPass SA: High-Concurrency Ticketing, Anti-Scalper Queue & Split-Pay Escrow Engine
## Capstone Project Brief, Engineering Roadmap & Assessment Rubric

**Project Title:** FairPass SA (Provably Fair Event Ticketing & Distributed Split-Pay Escrow Switch)  
**Focus Areas:** Apache Kafka, High-Concurrency Backend Engineering, Distributed Transactions (Sagas), Double-Entry Financial Ledgers  
**Target Level:** Capstone / Senior-Ready Backend Portfolio Project  
**Date:** October 28, 2026  

---

## 1. Executive Summary & Problem Statement

Traditional ticketing platforms (Ticketmaster, Computicket, Webtickets) suffer from four catastrophic failures during high-demand event drops (e.g., Springboks at Ellis Park or global stadium tours):

1. **Thundering Herd Server Crashes:** When 100,000 fans click "Buy" at `09:00:00`, direct database queries cause severe lock contention and cascading outages.
2. **Scalper Bot Dominance:** Automated scripts bypass frontends, lock inventory in milliseconds, and resell tickets on secondary markets at 300%–500% markups.
3. **Cart Hoarding & Deadlocks:** Bots and indecisive buyers hold seats in carts for 10 minutes without paying, making events appear "Sold Out" to genuine fans.
4. **Group Buying Friction ("The Fronting Problem"):** If 4 friends want 4 adjacent seats, one person is forced to pay the entire R8,000 bill upfront—or if they attempt to buy separately on 4 devices, strangers snatch the seats between them mid-checkout.

### Your Mission
Design and build **FairPass SA**, an event-driven, high-concurrency backend engine that solves all four problems using **Apache Kafka**, **multi-tiered concurrency control**, and a **PayShap-inspired "Squad Split-Pay" Double-Entry Escrow Ledger**.

---

## 2. Core Learning Objectives

By completing this project, you will demonstrate mastery over:

* **Apache Kafka Fundamentals & Advanced Patterns:**
  * Topic design, partition key selection, and strict ordering guarantees.
  * Consumer groups, parallel partition assignment, and consumer lag management.
  * Offset management (manual commits vs. auto-commits) and crash replayability.
  * The **Transactional Outbox Pattern**, **Saga Pattern** (choreography/orchestration), and **Dead Letter Queues (DLQs)**.
* **Concurrency & Synchronization:**
  * Preventing race conditions under extreme contention (thousands of threads competing for the same seat or wallet balance).
  * Pessimistic locking, Optimistic Concurrency Control (versioning), and atomic Compare-And-Swap (CAS) operations.
  * Deadlock prevention via deterministic lock ordering.
  * Worker pools, bounded queues, backpressure, and fan-out/fan-in parallel processing.
* **Fintech & Ledger Integrity:**
  * Immutable **Double-Entry Bookkeeping** (every debit has an equal and opposite credit; balances are derived, never overwritten).
  * Escrow holds, multi-party split-payment state machines, and atomic rollbacks/refunds on timeout.
  * End-to-end **Idempotency** (ensuring network retries or duplicate Kafka messages never charge a user twice).

---

## 3. High-Level System Architecture

```
+-----------------------------------------------------------------------------------+
|                        1. INGESTION & VIRTUAL WAITING ROOM                        |
|  - Admits 100k+ concurrent check-ins without touching the primary database        |
|  - Publishes to Kafka `queue.checkins` (Kafka Offset = Provably Fair Queue #)     |
|  - Concurrent Bot-Scoring Worker Pool evaluates telemetry in parallel threads     |
+-----------------------------------------------------------------------------------+
                                         |
                                         v (Backpressure-Controlled Admission JWT)
+-----------------------------------------------------------------------------------+
|                      2. SEAT RESERVATION & CONCURRENCY ENGINE                     |
|  - Sub-millisecond atomic seat hold (`AVAILABLE` -> `HELD`)                       |
|  - Two-tier locking: In-Memory/Redis CAS + PostgreSQL Optimistic Locking          |
|  - Emits `seats.state.changed` -> WebSocket Broadcaster for live stadium map      |
+-----------------------------------------------------------------------------------+
                                         |
                                         v (Single Checkout OR Squad Split-Pay)
+-----------------------------------------------------------------------------------+
|                   3. SQUAD SPLIT-PAY ESCROW & SAGA COORDINATOR                    |
|  - Locks N adjacent seats and generates N individual payment links (5-min timer)  |
|  - Double-Entry Escrow Ledger tracks each friend's contribution                   |
|  - Concurrent Expiration Sweeper races safely against last-millisecond payments   |
|  - Commits tickets on 100% funding OR triggers Saga Rollback/Refund on timeout    |
+-----------------------------------------------------------------------------------+
                                         |
                                         v (Post-Purchase Lifecycle)
+-----------------------------------------------------------------------------------+
|                  4. FACE-VALUE AUTO-MATCH RESALE EXCHANGE                         |
|  - Ticket holders can return tickets to `resale.offers` Kafka topic               |
|  - Concurrent Matching Engine pairs offer with `waitlist.orders` at face value    |
|  - Executes atomic three-way escrow swap (Buyer -> Seller -> New Ticket Issued)   |
+-----------------------------------------------------------------------------------+
```

---

## 4. What You Need to Build (The 5 Core Modules)

### Module 1: Provably Fair Kafka Waiting Room & Parallel Bot Filter
* **What it does:** Absorbs massive traffic spikes at ticket drop time without hitting the relational database.
* **What to achieve:**
  1. When a user joins the drop for an event, publish their check-in directly to a Kafka topic partitioned by `EventID`.
  2. Use the assigned **Kafka Partition + Offset** as their immutable, tamper-proof place in line.
  3. Build a **Concurrent Bot-Evaluation Worker Pool** that consumes check-in events and runs multiple heuristics in parallel (e.g., IP request velocity, fingerprint entropy, behavioral timing) within a strict time budget (e.g., 20ms).
  4. Build an **Admission Controller** that monitors active checkout capacity and promotes verified users to an `admitted` topic at a controlled rate (backpressure), issuing short-lived cryptographic admission tokens.

### Module 2: Sub-Millisecond Seat Locking Engine
* **What it does:** Guarantees that if 5,000 admitted fans attempt to reserve `Section A, Row 1, Seat 12` at the exact same millisecond, **exactly 1 succeeds** and **4,999 receive an immediate rejection** without crashing the database.
* **What to achieve:**
  1. Design a strict seat state machine: `AVAILABLE` $\rightarrow$ `HELD` $\rightarrow$ `COMMITTED` (or back to `AVAILABLE` if expired/cancelled).
  2. Implement a **fast-path concurrency lock** (e.g., Redis atomic operation or in-memory sharded lock) paired with **database-level Optimistic Concurrency Control** (version numbers) so the database remains the ultimate source of truth.
  3. Support **all-or-nothing multi-seat locking**: if a user requests 4 adjacent seats (`A1, A2, A3, A4`) and `A3` was just taken by someone else, none of the 4 seats should remain locked, and deadlocks must be impossible even if two users request overlapping seat ranges simultaneously (enforce deterministic lock ordering by Seat ID).
  4. Publish every seat state transition to Kafka so connected clients see seats change color on the stadium map in real time.

### Module 3: "Squad Split-Pay" Double-Entry Escrow Ledger & Timeout Saga
* **What it does:** Allows a lead buyer to lock up to 6 adjacent seats for their group and split the cost across individual payment links with a strict countdown timer (e.g., 5 minutes).
* **What to achieve:**
  1. **Double-Entry Ledger Design:** Never store a mutable `balance` column that you overwrite with `UPDATE`. Instead, record immutable debit and credit journal entries across accounts (`User_Wallet`, `Event_Escrow_Account`, `Organizer_Payable_Account`, `Platform_Fee_Account`).
  2. **Idempotent Payment Processing:** Every payment attempt must carry an `Idempotency-Key`. If a user clicks "Pay" 10 times or a Kafka consumer retries a message after a network blip, the ledger must only record the debit once.
  3. **The Millisecond Expiration Race Condition:** A background concurrent expiration engine monitors active Split-Pay holds. You must handle the exact race condition where Friend #4 submits the final payment at `04:59.999` at the exact same instant the expiration worker attempts to mark the hold as `EXPIRED`.
  4. **Distributed Saga Rollback:** If the timer expires and only 3 out of 4 friends paid, orchestrate a Kafka Saga that either:
     * **Mode A (Strict Group):** Automatically refunds the 3 friends from Escrow back to their wallets and releases all 4 seats.
     * **Mode B (Partial Keep):** Commits the 3 paid seats for the friends who paid and releases only the 1 unpaid seat to the waitlist.

### Module 4: Anti-Scalper Face-Value Instant Resale Engine
* **What it does:** Eliminates secondary-market scalping by allowing fans who can no longer attend to click "Instant Return", automatically matching their seat to the next fan on the official waitlist at capped face value.
* **What to achieve:**
  1. Fans join an event's **Auto-Buy Waitlist** by pre-authorizing an escrow hold, published to a `waitlist.orders` Kafka topic (strictly FIFO via Kafka offsets).
  2. When an existing ticket holder lists their ticket for return (`resale.offers`), a **Concurrent Matching Engine** pairs the returned seat with the oldest compatible waitlist order.
  3. Execute an atomic **Three-Way Settlement**: debit the waitlist buyer's escrow, credit the original seller's wallet, invalidate the old cryptographic ticket token, and issue a new ticket token to the buyer.

### Module 5: Chaos & Flash-Sale Simulator (Verification Harness)
* **What it does:** Proves your system works under extreme stress during your Capstone presentation and in your GitHub README.
* **What to achieve:**
  1. Build a concurrent load-generator tool/script that spawns thousands of simulated concurrent workers:
     * **Legitimate Solo Fans** (trying to buy 1–2 seats).
     * **Squad Groups** (locking 4 seats; some where all 4 pay on time, some where 1 friend times out at the last second).
     * **Scalper Bots** (firing rapid requests and attempting to hoard seats).
     * **Double-Spend Attackers** (firing 20 concurrent payment requests using the same wallet funds).
  2. Build an **Automated Audit Check** that runs after the simulation finishes and verifies the 4 Golden Invariants (listed in Section 6).

---

## 5. Recommended Tech Stack & Figma UI Blueprint

### 5.1 Backend Stack (Java 21+ & Spring Boot 3)
* **Core Framework:** Spring Boot 3.x with **Java 21+ Virtual Threads** enabled (`spring.threads.virtual.enabled=true`) for high-throughput concurrent request handling alongside explicit thread pools (`CompletableFuture` / `ExecutorService`) for parallel bot-scoring and chaos generation.
* **Event Streaming:** **Spring for Apache Kafka (`spring-kafka`)** configured with manual acknowledgment (`AckMode.MANUAL_IMMEDIATE`), idempotent producers, and Dead Letter Publishing Recoverers.
* **Persistence & Concurrency Locking:**
  * **PostgreSQL + Spring Data JPA / JDBC:** Uses `@Version` for Optimistic Concurrency Control on Seats and `PESSIMISTIC_WRITE` (`SELECT ... FOR UPDATE` ordered by ID) for Double-Entry Ledger account locks.
  * **Flyway / Liquibase:** Version-controlled database migrations.
  * **Redis (`Spring Data Redis`):** Atomic fast-path seat holds, idempotency key deduplication with TTL, and admission token rate-limiting.
* **Real-Time Push to Frontend:** **Spring WebSocket (STOMP)** or **Server-Sent Events (`SseEmitter`)** to broadcast live seat color changes and queue progress.

### 5.2 Frontend Stack (React + Vite + TypeScript)
Keep the frontend lean (~20% of total effort) so 80% of your engineering focus stays on Kafka, Concurrency, and the Escrow Ledger:
* **Framework:** **React 18/19 + Vite + TypeScript** (lightweight SPA—avoids unnecessary SSR overhead since Spring Boot is already your backend).
* **Styling & Design System:** **Tailwind CSS + shadcn/ui** (maps 1-to-1 with Figma Auto-Layout and design tokens).
* **Real-Time Client:** `@stomp/stompjs` (for WebSockets) or native `EventSource` (for SSE) to update the stadium seat map at 60fps without polling.
* **Telemetry Charts:** `Recharts` for live Kafka throughput and seat contention graphs in the Chaos Dashboard.

### 5.3 The 3 Core Screens to Design in Figma
1. **Screen 1: The Provably Fair Virtual Waiting Room**
   * Displays the user's immutable **Kafka Partition & Offset badge** (e.g., `Partition 2 • Queue Ticket #001,402`), live progress bar, estimated wait time, and "Bot-Shield Verified" status indicator.
2. **Screen 2: Interactive Stadium Seat Map & "Squad Split-Pay" Checkout**
   * **Left Panel:** Interactive SVG Stadium Grid where seats dynamically transition between **Green (`AVAILABLE`)**, **Amber (`HELD` with countdown)**, and **Red (`SOLD`)** in real time as WebSocket events arrive.
   * **Right Panel:** The **Squad Split-Pay Escrow Card** showing the 4 selected adjacent seats, the 4 friends' payment statuses (`Paid` vs. `Awaiting Payment`), shareable PayShap/link badges, the live `04:59` countdown timer, and the fallback mode toggle (*"Strict Group Refund"* vs. *"Keep Paid Seats"*).
3. **Screen 3: The "Chaos & Flash-Sale Simulator" Mission Control (For Your Capstone Demo)**
   * **Control Bar:** Buttons to trigger *"Simulate 5,000 Fans + 2,000 Scalper Bots"*, *"Trigger 50x Double-Spend Attack"*, and *"Simulate Consumer Crash"*.
   * **Live Telemetry:** Real-time charts showing requests/sec, Kafka consumer lag, and active escrow holds.
   * **The 5 Golden Invariants Scorecard:** Live green checkmarks proving `Overbooked Seats: 0`, `Orphaned Holds: 0`, `Ledger Balance Drift: R 0.00`, and `Negative Wallets: 0`.

### 5.4 Required Dependencies Checklist

#### A. Spring Boot 3 Backend (`pom.xml` / `build.gradle`)
* **Core Web & Real-Time:**
  * `spring-boot-starter-web` (REST APIs + Java 21 Virtual Threads support)
  * `spring-boot-starter-websocket` (Live STOMP/WebSocket updates to the React stadium map)
  * `spring-boot-starter-validation` (Request payload validation)
* **Kafka & Messaging:**
  * `spring-kafka` (Apache Kafka producers, consumer groups, manual offset acks, and Dead Letter Topic routing)
  * `jackson-databind` & `jackson-datatype-jsr310` (JSON event serialization with Java 8+ timestamps)
* **Database, Locking & Caching:**
  * `spring-boot-starter-data-jpa` (ORM, `@Version` optimistic locking, and `@Lock(LockModeType.PESSIMISTIC_WRITE)` for ledger accounts)
  * `postgresql` (PostgreSQL JDBC driver)
  * `flyway-core` & `flyway-database-postgresql` (Automated SQL schema migrations)
  * `spring-boot-starter-data-redis` (Atomic Redis seat holds, Lua scripts, and idempotency keys)
* **Observability & Resilience:**
  * `spring-boot-starter-actuator` & `micrometer-registry-prometheus` (Live throughput, JVM thread, and Kafka metrics)
  * `lombok` (Reduces boilerplate)
* **Testing & Chaos Verification:**
  * `spring-boot-starter-test` & `spring-kafka-test`
  * `testcontainers` (`postgresql`, `kafka`, `junit-jupiter` — spins up real disposable Kafka & Postgres containers during integration tests)

#### B. React + Vite + TypeScript Frontend (`package.json`)
* **Core & Styling:** `react`, `react-dom`, `tailwindcss`, `lucide-react` (icons), `clsx`, `tailwind-merge`
* **Real-Time WebSocket Client:** `@stomp/stompjs` (connects to Spring Boot WebSocket broker)
* **Data Fetching & State:** `@tanstack/react-query` or `zustand` (lightweight state management for selected seats and split-pay session)
* **Telemetry Charts:** `recharts` (for the Chaos Simulator throughput and consumer lag charts)

#### C. Local Docker Compose Infrastructure (`docker-compose.yml`)
* `apache/kafka:latest` (KRaft mode — single container, **no ZooKeeper needed**) or `redpandadata/redpanda:latest` (lightweight, Kafka-compatible broker)
* `provectuslabs/kafka-ui:latest` (Web dashboard to visually inspect topics, partitions, messages, and consumer lag)
* `postgres:16-alpine` (Primary relational & ledger store)
* `redis:7-alpine` (Fast-path atomic locks & idempotency cache)

### 5.5 Free Cloud Services for Deployment (R0.00 Budget)
Using your `@student.wethinkcode.co.za` email, activate the **GitHub Student Developer Pack** (`education.github.com/pack`) alongside these free tiers:

| Component | Recommended Free Cloud Service | Free Tier Details |
| :--- | :--- | :--- |
| **1. Full-Stack All-in-One VM (Best Option)** | **Oracle Cloud Free Tier (Always Free ARM)** or **DigitalOcean ($200 Student Credit)** or **Azure ($100 Student Credit)** | Oracle gives **4 ARM CPUs + 24 GB RAM forever for free**. You can run your entire `docker-compose.yml` (Spring Boot + Kafka + Postgres + Redis) on a single VM without hitting memory limits! |
| **2. Managed Apache Kafka** | **Aiven for Apache Kafka (Free Plan)** or **Redpanda Cloud (Serverless Free Tier)** | Free managed Kafka cluster with SASL/SSL credentials you plug directly into Spring Boot's `application.yml`. |
| **3. Managed PostgreSQL** | **Neon.tech** or **Supabase** or **Aiven PostgreSQL** | Generous free forever serverless PostgreSQL tiers with connection pooling. |
| **4. Managed Redis** | **Upstash Redis** or **Redis Cloud Free** | Free serverless Redis with native TLS for atomic seat locks and idempotency keys. |
| **5. Spring Boot Hosting (if not using a VM)** | **Render.com** or **Koyeb** or **Fly.io** | Free Docker container hosting for Spring Boot microservices. |
| **6. React Frontend Hosting** | **Vercel** or **Cloudflare Pages** or **Netlify** | 100% free global CDN hosting with automatic GitHub PR deployments. |

---

## 6. Phased Implementation Roadmap (What To Do Step-by-Step)

### Phase 1: Foundation, Schema & Local Infrastructure
* [ ] Set up a single-command environment (`docker compose up`) with **Apache Kafka**, **PostgreSQL**, and **Redis** (plus a Kafka UI tool so you can visually inspect topics, partitions, and offsets).
* [ ] Design your relational schema: Events, Venue Sections, Seats (with `version` column for optimistic locking), Split-Pay Sessions, and Double-Entry Ledger tables (`accounts`, `journal_entries`, `postings`).
* [ ] Define your Kafka Topic topology (topic names, partition counts, partition keys, retention policies, and Dead Letter Queues).

### Phase 2: Concurrency Core (Seat Locking & Deadlock Prevention)
* [ ] Implement single-seat and multi-seat reservation endpoints.
* [ ] Write a concurrency stress test that spawns 500 threads fighting for the exact same seat and verify that 499 fail gracefully and 1 succeeds.
* [ ] Write a deadlock stress test where Thread A requests seats `[1, 2]` while Thread B simultaneously requests seats `[2, 1]`, proving your deterministic lock ordering prevents deadlocks.

### Phase 3: Kafka Waiting Room & Bot Defense
* [ ] Implement the Kafka producer for queue check-ins and consumer groups for parallel bot telemetry scoring.
* [ ] Build the backpressure-aware Admission Controller that reads check-ins and issues signed admission tokens based on available checkout slots.

### Phase 4: Double-Entry Escrow Ledger & Split-Pay Saga
* [ ] Implement the double-entry ledger service with strict idempotency keys and deterministic account locking.
* [ ] Build the Squad Split-Pay coordinator and the concurrent expiration worker.
* [ ] Implement the Transactional Outbox pattern so database state changes and Kafka event publications are guaranteed to be atomic (no dual-write bugs).
* [ ] Test the race condition between a last-millisecond payment and the expiration worker.

### Phase 5: Waitlist Auto-Matcher, Chaos Dashboard & Figma UI
* [ ] Implement the FIFO waitlist and instant resale matching engine.
* [ ] Connect your backend to a clean visual interface (designed in Figma) featuring:
  * Live interactive Stadium Seat Map (seats turning Yellow for Held, Red for Sold, Green for Available via WebSockets).
  * Squad Split-Pay Checkout view showing the 4 friends' payment avatars and live countdown timer.
  * "Chaos Simulator Control Panel" to trigger 5,000 concurrent requests and display the live audit scorecard.

---

## 7. Hard Engineering Targets (What You Are Aiming to Achieve)

Your system is considered **complete and production-grade** when you can run a 60-second Chaos Simulation against a **1,000-seat venue** with **10,000 concurrent requests** and pass all **5 Golden Invariants**:

1. **The Zero Overbooking Invariant:**
   $$\text{Count(Issued Tickets for Seat } X) \le 1 \quad \forall X$$
   Never, under any thread timing or Kafka retry scenario, can two users hold an active ticket for the same seat at the same time.
2. **The Zero Orphaned Lock Invariant:**
   After all expiration timers have elapsed, **0 seats** remain stuck in the `HELD` state—every seat is either `COMMITTED` (paid in full) or returned to `AVAILABLE`.
3. **The Double-Entry Zero-Sum Invariant:**
   $$\sum \text{All Ledger Debits} - \sum \text{All Ledger Credits} = \text{R } 0.00$$
   Every single cent across all user wallets, escrow holds, refunds, and organizer payouts balances to exactly zero.
4. **The Non-Negative Wallet Invariant:**
   No user wallet or escrow sub-account ever drops below `R 0.00`, even if a user fires 50 concurrent transfer requests at the exact same millisecond.
5. **The Crash-Recovery Invariant:**
   If you forcefully kill (`docker kill`) a backend consumer mid-simulation and restart it 5 seconds later, Kafka consumer rebalancing and idempotency keys ensure zero lost payments and zero duplicate charges.

---

## 8. Capstone Assessment Rubric (100 Points Total)

Use this rubric to self-grade your project or present it for academic/technical evaluation.

| Category | Weight | Needs Improvement (0–59%) | Competent / Pass (60–74%) | Proficient (75–89%) | Distinction / Senior Level (90–100%) |
| :--- | :---: | :--- | :--- | :--- | :--- |
| **1. Apache Kafka Architecture & Event Design** | **25 pts** | Uses Kafka as a single generic queue with 1 partition; relies on auto-commit offsets; messages are lost or duplicated on consumer restart. | Uses multiple topics and consumer groups; partitions events by a logical key; handles basic producer/consumer serialization cleanly. | Strategic partition keys preserve strict ordering per Event/Account; manual offset commits after processing; includes Dead Letter Queues (DLQs) with retry backoff. | Implements the **Transactional Outbox Pattern** (zero dual-write bugs), backpressure-driven queue admission, consumer rebalance safety, and proves crash recovery during live load. |
| **2. Concurrency & Race-Condition Prevention** | **25 pts** | Relies on naive read-then-write queries (`SELECT` then `UPDATE`); fails under concurrent load with double-booked seats or deadlocks. | Uses basic database transactions or coarse global locks (`synchronized` / global mutex) that prevent double-booking but bottleneck throughput. | Uses fine-grained locking (Optimistic Concurrency Control via `version` column or Redis atomic locks) and worker pools for parallel tasks. | Combines sub-millisecond fast-path locks with DB optimistic locking; enforces **deterministic lock ordering** to mathematically prevent deadlocks on multi-seat holds; handles the payment-vs-expiration race cleanly. |
| **3. Fintech Double-Entry Escrow & Saga Logic** | **20 pts** | Stores money as a single mutable `balance` float column; no escrow separation; partial group payments get stuck if a friend times out. | Uses integer cents (`BIGINT`); tracks Split-Pay state and refunds users when a group hold expires, though edge-case retries may cause duplicates. | Implements an immutable **Double-Entry Ledger** using integer minor units (cents); enforces `Idempotency-Key` on all payment endpoints; supports Saga rollback on timeout. | Full double-entry accounting (`SUM(debits) == SUM(credits)`) verified by automated audit; supports both Strict Rollback and Partial-Keep group sagas plus atomic 3-way waitlist resale swaps. |
| **4. Fault Tolerance, Testing & Chaos Simulation** | **15 pts** | Only tested manually via Postman/cURL one request at a time; no automated concurrency or stress tests. | Includes unit tests and a basic script firing 50–100 concurrent requests against the API. | Includes a dedicated concurrent load generator simulating thousands of fans, group holds, and expirations with automated pass/fail assertions. | Includes a full **Chaos & Flash-Sale Simulator** testing double-spend attacks, scalper floods, multi-seat deadlock attempts, and mid-flight container kills with a 100% pass rate on all 5 Golden Invariants. |
| **5. Observability, Documentation & Capstone UI** | **15 pts** | Minimal README; hard to run locally; no visual way to observe Kafka lag or seat state transitions. | Clean `docker compose up` setup and README architecture diagrams; logs show events flowing through services. | Real-time stadium seat grid UI (via WebSockets/SSE) + Kafka UI / Prometheus metrics showing throughput and consumer lag. | Polished Figma-designed UI with live stadium heatmap, Squad Split-Pay countdown screen, and a live Chaos Audit Dashboard proving zero double-bookings and zero ledger drift in real time. |

---

## 9. Capstone Defense / Technical Interview Questions

When you present this project to assessors or software engineering interviewers, be prepared to answer these 6 questions clearly:

1. **Partitioning vs. Parallelism:** *"Why did you choose `EventID` (or `SeatSectionID` / `AccountID`) as your Kafka partition key? What would break if two events for the same seat or wallet landed on different partitions?"*
2. **The Dual-Write Problem:** *"When a user pays for their Split-Pay share, you update PostgreSQL and publish an event to Kafka. What happens if the server crashes right after the database commit but before Kafka receives the message? How did your architecture solve this?"*
3. **Lock Contention & Deadlocks:** *"Suppose User A tries to lock seats `A1` and `A2` at the exact same millisecond User B tries to lock `A2` and `A1`. How does your backend guarantee they never deadlock?"*
4. **The Expiration Race:** *"Friend #4's payment arrives at `04:59.999` while your background expiration thread is already mid-execution cancelling the group hold. Walk me through the exact state transition and lock mechanism that prevents the user from being charged for a cancelled ticket."*
5. **At-Least-Once Delivery & Idempotency:** *"Kafka guarantees at-least-once delivery by default, meaning a consumer can receive the exact same `payment.authorized` event twice after a rebalance. Why doesn't your ledger credit the escrow account twice?"*
6. **Optimistic vs. Pessimistic Locking:** *"Where in your system did you choose Optimistic Concurrency Control (`version` checks) versus Pessimistic Locking (`SELECT ... FOR UPDATE`), and why?"*
