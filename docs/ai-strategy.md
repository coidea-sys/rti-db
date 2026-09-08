# rti-db and AI: A Strategic Position Paper

*Version 1.0 — September 2026 · Applies to rti-db v0.6.0 and the public roadmap.*

This document states, explicitly and durably, how rti-db relates to artificial
intelligence: where it sits in the AI stack, what it will and will not do, and the
strategic reasoning behind those choices. It is a communication tool for users,
integrators, contributors, and competitors alike.

---

## 1. The premise: intelligence is becoming real-time, and AI is why

Industrial control, aerospace, transport, and robotics are converging on the same
architecture: machine-learned models making decisions over streaming physical
state. The driver is the maturation of embodied AI — vision-language-action (VLA)
policies, world models, and dual-system stacks (Helix S1/S2, NVIDIA GR00T, pi0)
that pair a slow, deliberate reasoning system with a fast, reactive one.

These systems share a structural tension:

- **Models reason at 10–100 ms.** A VLA forward pass, even with speculative
  decoding, early exit, MoE routing, and action chunking, costs tens of
  milliseconds. Dual-system designs accept this: S2 (the VLM) thinks slowly and
  hands latent intent to S1 (the reactive policy), which runs fast.
- **The physical world streams at 1–100 kHz.** Motor currents, IMUs, joint
  encoders, vibration, telemetry. Safety loops and reflexes live here, three
  orders of magnitude below the model's clock.

That gap — between how fast the world speaks and how fast models think — is where
embodied AI lives or dies. It is not a model problem. No amount of parameter
scaling makes a 50 ms policy read a 10 kHz current loop. **It is a data-plane
problem**, and it has no incumbent owner: general-purpose databases are built for
dashboards and transactions, and real-time operating systems are built for tasks,
not data. rti-db exists to own this layer.

## 2. Three roles vis-à-vis AI

### 2.1 Inference time: the working memory of embodied AI

An LLM's KV cache gives the model attention over its recent context. Embodied AI
needs the same structure for physical state — a place where "what the world just
did" is held losslessly and served at two speeds:

- **The reflex path (S1-class consumers).** Lock-free SPSC rings deliver the
  latest sensor state with ~0.5 µs enqueue cost and zero allocation on the hot
  path. Deterministic, `no_std`-capable, safe to place on a safety island.
- **The attention path (S2-class consumers).** Columnar segments with
  delta-of-delta + XOR compression let a planner scan millions of points of
  recent history in milliseconds — episodic context windows for reasoning, fault
  anticipation, and in-context adaptation.

rti-db is, in this sense, **a KV cache for the physical world** — with one
deliberate difference: it is durable. A robot's working memory survives power
loss (`put_durable`, group commit, WAL checkpointing), because the physical world
does not pause when the computer reboots.

### 2.2 Training time: the flight recorder and the data flywheel

Text models trained on the internet; embodied models have no internet of
physical experience. Every serious robotics program is therefore a data engine:
deploy → record episodes → curate → train → redeploy. The flywheel's chokepoint
is not the model — it is *faithful capture* at the edge:

- **Zero-loss episode logging.** `put_durable()` + `durable_watermark()` give a
  verified, kill -9-tested zero-loss write path; what the robot experienced is
  what the trainer sees.
- **Economics of retention.** 1.58× measured compression over raw (1M points in
  10.1 MB) and a SigV4-authenticated S3 cold tier make "keep everything" a
  default rather than a budget line.
- **Replay-identical reproduction.** TSN-aligned timestamps and zone-mapped
  segments mean any decision window can be reconstructed exactly — for training
  example mining, counterfactual analysis, and Sim2Real gap measurement.

Whoever logs the fleet's experience owns the raw material of the next policy
generation. rti-db intends to be that logger — openly, for everyone's fleet.

### 2.3 Governance: determinism as the audit substrate

Aerospace, industrial, and transport deployments will face regulation of learned
controllers. Certification asks questions only a deterministic data plane can
answer: *What did the model see, in what order, with what timing? What did it
decide? Can you reproduce it?* rti-db's separation of reflex/cognition/planning
paths, bounded-latency guarantees, and exact replay provide that substrate. This
is a role no model vendor is positioned to play — and a durable reason for
regulators and integrators to prefer an open, inspectable layer.

## 3. Strategic posture

### 3.1 Be Switzerland

rti-db's posture toward the AI industry is deliberately complementary:

- **We do not train models.** No foundation model, no policy network, no world
  model. We will never compete with our users' core IP.
- **We do not serve models.** rti-db is not an inference engine, not an LLM
  serving stack, and not a vector database. Embedding storage may appear as an
  *integration convenience*, never as a competing retrieval product.
- **We integrate with everyone.** ROS 2, NVIDIA Isaac/GR00T, LeRobot, and VLA
  runtimes are ecosystems to plug into, not standards wars to fight. In the
  boxed-pigs game of platform economics, the model vendors are the large
  players who must press the lever; rti-db eats well by staying at the trough —
  and by being indispensable to every lever-presser equally.

### 3.2 Open source as a commitment device

Adoption of infrastructure fails on fear: *if I build on you, will you later
enter my layer or hold my data plane hostage?* The MIT OR Apache-2.0 dual
license, public roadmap, and reproducible public benchmarks are rti-db's answer
— a credible commitment (in the game-theoretic sense) that the data plane stays
open and neutral. This converts potential competitors into integrators and
removes the strongest argument for in-house reinvention.

### 3.3 Own the feedback loop, openly

The enduring asset in embodied AI is the feedback loop: experience → data →
policy → experience. Model weights commoditize; the capture layer compounds.
rti-db's strategy is to own the data plane of that loop **structurally** —
through deterministic memory control, zero-copy pipelines, and durability
semantics that are properties of the architecture, not of tuning — and to own it
**openly**, so that owning it does not provoke the ecosystem into routing around
us.

## 4. What this means for the roadmap

| Horizon | AI-facing commitment |
|---|---|
| v0.7 (next) | Episode/segment compaction tuned for flight-recorder workloads; multi-shard Raft for fleet-scale capture |
| Mid | Reference integrations: ROS 2 topic bridge, LeRobot episode exporter, VLA runtime working-memory adapter |
| Long | Certified-replay profiles for safety audit; edge-to-cloud flywheel tooling (curate, version, export) |

Two invariants govern everything above: **the reflex path never regresses**
(determinism is the product), and **no feature may make rti-db a competitor to
its users' models** (neutrality is the strategy).

## 5. Summary in one paragraph

AI made real-time intelligence a necessity; it did not make the real-time data
plane a solved problem. rti-db serves AI at inference time (working memory), at
training time (flight recorder), and at governance time (deterministic replay),
while strategically refusing to enter the model layer. AI is the demand, not the
competition. The data plane is the position — and it is held openly, so the
whole ecosystem can stand on it.
