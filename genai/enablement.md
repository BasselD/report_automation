# Claude/Perplexity
This gives solid grounding for the tech stack recommendations. Now let me put together the full evaluation.

## Portfolio Overview

Your deck lays out six initiatives that split cleanly into a **shared platform** and **five business applications**, following a proven "capability once, apply many times" pattern common in enterprise LLM rollouts. That's a sound structural choice — it avoids the classic mistake of building a bespoke RAG stack per use case.

## Evaluating Each Proposal

**1. Clinical Documentation Intelligence (Capability)**
This is the foundational capability — turning scanned/handwritten/digital charts into structured, citable evidence via OCR/multimodal extraction plus a governed schema. It's correctly scoped as a *capability*, not an application, since Audit the Auditor, Stars evidence detection, and future clinical analytics all depend on it. The main risk is underestimating extraction quality variance across payer/provider chart formats — handwritten and fax-scanned charts will need a human-in-the-loop review queue, which the deck does account for ("route uncertain items to review"). This is the highest-leverage build because everything downstream reuses it.

**2. Audit the Auditor (Application)**
Comparing extracted evidence against audit decisions to target secondary review is a strong, bounded pilot — it has ground truth (existing audit outcomes), a clear KPI (reviewer time, reconciliation accuracy), and doesn't require the LLM to make final coding decisions, just flag discrepancies. This is a good "prove value fast" candidate because the audit function already generates labeled outcomes you can validate against.

**3. CareAllies AI Intelligence Layer (Shared Platform)**
This is the connective tissue: RAG for governed knowledge, controlled SQL/rules for deterministic answers, and an LLM for explanation — with permissions and audit logging wrapped around all of it. Correctly identified as shared infrastructure. The "retrieve knowledge, calculate with governed tools, explain with the LLM" pattern is the right design — it keeps the LLM out of doing arithmetic on claims/risk data (a common and costly failure mode) and instead has it narrate results computed deterministically in SQL.

**4. Automated Risk Score Investigator (Application)**
Explaining RAF/risk-score drivers is high value but higher risk since it touches HCC/risk-adjustment methodology directly tied to reimbursement — any hallucinated driver explanation is a compliance exposure, not just an inconvenience. The mitigation in your deck (deterministic SQL/rules doing the actual calculation, LLM only narrating) is exactly right and should be a non-negotiable guardrail, not just an option. [raapidinc](https://www.raapidinc.com/)

**5. Stars Performance Investigator (Application, split into performance investigation + clinical evidence detection)**
Splitting into two sub-capabilities is smart: performance investigation is mostly SQL/rules-driven (gap prioritization), while clinical evidence detection leans on the Documentation Intelligence + RAG capabilities to find chart evidence for measure closure. This mirrors how vendors like Navina and Innovaccer structure HCC/Stars evidence surfacing at the point of care. [navina](https://www.navina.ai/)

**6. Provider Behavior Profiling (Application/ML)**
Correctly flagged as able to "begin with traditional ML" rather than needing LLM/RAG at all — clustering/segmentation on provider claims features doesn't need generative AI, and embeddings are listed as optional rather than required. This is the most independent track and a good place to show quick wins without waiting on the shared LLM platform to mature.

**Overall assessment**: the portfolio is architecturally disciplined — it separates deterministic calculation (SQL/rules) from retrieval (RAG) from generation (LLM explanation), which is the single most important design decision in regulated healthcare AI. The sequencing note ("select bounded pilots based on data access, ground truth, measurable value, approval readiness") suggests you're already thinking about de-risking rollout order, and Audit the Auditor plus Risk Score Investigator are the strongest first pilots because they have existing ground truth to validate against.

## Data Reality: Teradata + Redshift Split

Since claims/membership live in Teradata and clinical notes live in Redshift, your Intelligence Layer needs a federation or consolidation strategy before RAG/LLM can reason across both. Three viable patterns, in order of typical enterprise preference:

- **Federated query layer**: keep data in place, use a query federation/virtualization tool so the LLM's "governed SQL" tool can pull from both sources in one logical query without a full migration. [linkedin](https://www.linkedin.com/pulse/automating-teradata-amazon-redshift-migration-practical-implementation-jv3oc)
- **Unified analytics layer**: replicate/sync both into a common analytics warehouse or lakehouse (e.g., Redshift as target, or a lakehouse like Databricks/Snowflake) purpose-built for AI workloads, leaving Teradata as system-of-record.
- **Direct migration**: full Teradata-to-Redshift migration consolidates everything, but this is a multi-month undertaking better treated as a separate initiative than a prerequisite for these AI pilots. [linkedin](https://www.linkedin.com/pulse/automating-teradata-amazon-redshift-migration-practical-implementation-jv3oc)

Given your pilots need speed, the federated or "sync a governed subset" approach is more realistic than a full migration up front.

## Recommended Tools and Tech Stack

| Layer | Purpose | Recommended Tools |
|---|---|---|
| Document ingestion/OCR | Convert scanned/handwritten charts to text | AWS Textract (native to Redshift's AWS ecosystem) or Azure Document Intelligence; multimodal LLM (GPT-4o/Claude/Gemini vision) for handwriting and complex layouts |
| Data access/federation | Query Teradata + Redshift together | Teradata QueryGrid or AWS Glue/Athena federated query connectors; alternatively a lakehouse sync (Fivetran/Airbyte → S3 → Redshift) for a "governed copy" of Teradata claims data |
| Orchestration/ETL | Pipeline scheduling, transformation | Airflow or dbt for transformation logic; Python/PySpark for schema mapping between Teradata and Redshift types [linkedin](https://www.linkedin.com/pulse/automating-teradata-amazon-redshift-migration-practical-implementation-jv3oc) |
| Vector database | Store embeddings for RAG over clinical notes/policies | pgvector on Amazon RDS/Aurora, OpenSearch (AWS-native, HIPAA-eligible), or Redshift's native vector support — keep it inside your AWS boundary since notes already live there [aws.amazon](https://aws.amazon.com/blogs/big-data/build-a-rag-data-ingestion-pipeline-for-large-scale-ml-workloads/) |
| Embeddings | Convert text/chunks to vectors | Amazon Bedrock embedding models (Titan) or Cohere/OpenAI embeddings via Bedrock, since Bedrock offers HIPAA-eligible, BAA-covered access without data leaving your AWS account |
| LLM inference | Generation, explanation, narration | Amazon Bedrock (Claude, Llama, or Titan models) — HIPAA-eligible with BAA, keeps inference inside AWS rather than routing PHI to third-party APIs [hipaavault](https://www.hipaavault.com/resources/hipaa-compliant-llm/) |
| Governed SQL/rules engine | Deterministic calculations (risk scores, Stars gaps) | Existing SQL against Teradata/Redshift, wrapped in a tool-calling layer (LangChain/LlamaIndex SQL agents, or a custom function-calling API) so the LLM never computes numbers itself |
| RAG orchestration | Retrieval, citation, prompt assembly | LangChain, LlamaIndex, or Bedrock Knowledge Bases (native AWS RAG service with built-in citation support) |
| PHI de-identification/redaction | Strip identifiers before any PHI reaches generation | AWS Comprehend Medical (de-identification API) or Presidio (open-source), applied before prompts hit the LLM [kalviumlabs](https://www.kalviumlabs.ai/blog/ai-for-healthcare-2026-hipaa-safe-llm-architecture/) |
| Governance/permissions/audit | Access control, logging, evaluation | RBAC/ABAC via IAM (AWS side) and Teradata roles (data side); centralized audit logging (CloudTrail + a policy-proxy layer); an LLM eval framework (Ragas, DeepEval, or Bedrock's built-in evaluation) to test hallucination/accuracy before each pilot ships [edenlab](https://edenlab.io/blog/hipaa-compliant-ai-best-practices) |
| Application/UI layer | Analyst-facing dashboards for each application | Streamlit or Dash, backed by DuckDB for local dev/prototyping — consistent with your existing stack — deployed via Cloud Run or ECS |
| ML for Provider Profiling | Clustering/segmentation, no LLM needed | scikit-learn or XGBoost on features pulled directly from Teradata claims; no vector DB/RAG dependency required initially |

## Key Architectural Decisions to Nail Down

Two things matter more than tool choice: whether PHI ever leaves your network boundary, and whether the LLM is ever allowed to compute a number instead of just explaining one. Given clinical notes are already in AWS, routing LLM inference through Amazon Bedrock (rather than external APIs) keeps PHI inside your existing AWS boundary and BAA coverage, while Teradata claims data should flow through a governed SQL/rules layer that the LLM calls as a tool rather than reasons over directly. This mirrors the deck's own design principle — "retrieve knowledge, calculate with governed tools, explain with the LLM" — and is the single guardrail most likely to determine whether the Risk Score Investigator and Audit the Auditor pilots pass compliance review. [hipaavault](https://www.hipaavault.com/resources/hipaa-compliant-llm/)
All three diagrams are generated. Now let me finalize the answer.

## Core Technical Requirements for the Intelligence Layer

The Intelligence Layer is the shared platform every application depends on, so its requirements need to be locked down before any application pilot ships. Based on the deck's own description ("LLM services | RAG/citations | SQL + rules | APIs | permissions + evaluation"), the concrete requirements break into five buckets:

- **Governed data access**: a federated or virtualized query path that reaches both Teradata (claims/membership) and Redshift (clinical notes) without duplicating sensitive data unnecessarily — via Teradata QueryGrid or AWS Glue/Athena federation, plus row/column-level RBAC so each application only sees the fields it's cleared for.
- **Deterministic SQL/rules engine**: a tool-calling layer that lets the LLM invoke exact SQL against governed views rather than compute numbers itself — this is the single guardrail that keeps risk-score and Stars-gap calculations audit-safe.
- **RAG with citations**: a vector store (pgvector, OpenSearch, or Redshift-native vector) holding embedded SOPs, methodologies, and structured clinical evidence, returning source-and-date-stamped citations on every retrieval, not just free-text answers.
- **LLM service layer**: an inference endpoint via Amazon Bedrock (Claude/Titan) or an equivalent HIPAA-eligible, BAA-covered service so PHI-adjacent prompts never leave your AWS security boundary.
- **Permissions, audit, and evaluation**: IAM/ABAC access control, CloudTrail-style logging of every query and generation, and a pre-release evaluation harness (Ragas/DeepEval or similar) to test hallucination and accuracy rates before any application goes live on top of the layer.

The diagram below shows how these pieces connect end-to-end, from source systems through the API layer that applications consume.

## Prioritization: Build Order Across the Six Proposals

The right sequence isn't the numbering in the deck — it's driven by dependency and risk. Three factors should govern order: which pilots have existing ground truth to validate against, which are compliance-sensitive (reimbursement-linked), and which can prove value without waiting on the shared platform.

- **Wave 0 — Foundation**: build the core Intelligence Layer (data access, governed SQL/rules, permissions, audit) since every other project depends on it. Run Provider Behavior Profiling in parallel — it needs only traditional ML on Teradata claims features, no LLM/RAG dependency, so it's a fast independent win.
- **Wave 1 — First proof**: stand up Clinical Documentation Intelligence and pilot Audit the Auditor. Audit already has labeled ground truth (past audit decisions), so it's the fastest way to prove the evidence-extraction capability actually works before betting bigger initiatives on it.
- **Wave 2 — Expand into compliance-sensitive scoring**: build the Automated Risk Score Investigator once the SQL/rules engine and evidence layer are validated. This is deliberately sequenced after Wave 1 because RAF/risk-score explanations carry direct reimbursement and audit exposure — you want the deterministic-calculation guardrail proven elsewhere first.
- **Wave 3 — Full Stars suite**: Stars Performance Investigator and Stars Clinical Evidence Detection come last since they reuse everything built in Waves 0–2 (documentation evidence, RAG, governed SQL) and benefit most from a mature, tested platform underneath them.

This order lets you show measurable wins early (Provider Profiling and Audit the Auditor) while de-risking the more compliance-sensitive work until the guardrails are field-tested.

## Diagrams

Three diagrams are attached:

1. **Overall Portfolio Architecture** — shows how Teradata, Redshift, inbound documents, and business knowledge feed the Documentation Intelligence capability and the shared Intelligence Layer, which in turn powers all five applications.
2. **Intelligence Layer Technical Architecture** — a detailed internal view of the shared platform: data access/governance, the four core services (SQL/rules, RAG, LLM, orchestration), permissions/audit/evaluation, and the governed API layer applications call into.
3. **Implementation Prioritization Roadmap** — a four-wave sequencing view showing what to build first, what runs in parallel, and how later applications reuse earlier capabilities.

# ChatGPT
My read is that this is a **very encouraging proof of concept**, but I would not select a production model from this benchmark yet. Four models showing 100% exact-code accuracy on 23 conditions tells you the task is technically feasible. It does **not yet tell you which model is safest, fastest, cheapest, or most robust for CareAllies**.

## What stands out

| Observation                                                       | My interpretation                                                       |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------- |
| 4 of 5 models hit **23/23 exact code accuracy**                   | Strong signal that modern vision models can handle this controlled task |
| Test 03 shows **100% with ~5 sec thinking**                       | Looks especially promising if end-to-end runtime is also good           |
| Test 01 shows **100% with ~10 sec thinking**                      | Also strong candidate                                                   |
| LightOn returned demographics/vitals in addition to target fields | Shows potential **instruction-following / output-control issue**        |
| Nemotron was **20/23** and ~2 min                                 | Hard to justify unless it has another major advantage                   |
| Full OCR runtime is unavailable                                   | Major missing variable. You cannot yet compare operational performance  |
| Only **23 conditions**                                            | Too small to estimate production accuracy confidently                   |

So I would probably advance **Tests 01 and 03** into the next round first, with Test 02 as a lightweight challenger if its true runtime and resource consumption are attractive.

---

# The biggest question to answer next

Right now you are testing:

> **Can the model read these handwritten ICD-10 entries?**

For CareAllies, the actual business requirement is harder:

> **Can the model reliably find relevant clinical evidence in messy real charts, distinguish what is and is not supported, and know when it is uncertain?**

That should become Round 2.

## I would expand the benchmark in five directions

### 1. Make the documents harder

Test at least 100–300 examples containing:

* Good handwriting
* Very poor handwriting
* Cursive
* Abbreviations
* Cross-outs/corrections
* Overlapping writing
* Multiple diagnoses
* Low-resolution scans
* Fax artifacts
* Rotated/skewed pages
* Multiple-page charts
* Mixed typed + handwritten content

The current results could collapse once you introduce actual production messiness. Better to discover that now.

---

### 2. Test diagnoses without an explicit ICD code

This is critical.

If the chart literally contains:

> **E11.22**

the task is mostly visual recognition.

A much more valuable test is:

> **“Type 2 DM with diabetic CKD”**

and ask the system to extract:

```text
Condition: Type 2 diabetes with diabetic CKD
Candidate ICD-10: E11.22
Evidence: page X / line Y
Confidence: 0.94
```

Then another case where the clinical evidence is **insufficient** and the correct result should be:

> **Unable to determine. Human review required.**

That is much closer to Audit the Auditor.

---

# 3. Measure false positives, not just accuracy

For risk adjustment, **hallucinating one diagnosis can be much worse than missing one**.

I would add these metrics:

| Metric                         | Why it matters                                      |
| ------------------------------ | --------------------------------------------------- |
| **Exact ICD accuracy**         | Current metric                                      |
| **Diagnosis recall**           | Did it find everything actually present?            |
| **Precision**                  | Did it invent anything?                             |
| **False positive rate**        | Especially important for compliance                 |
| **Evidence citation accuracy** | Can the reviewer locate the source?                 |
| **Abstention accuracy**        | Does it know when it cannot read something?         |
| **Confidence calibration**     | Does 90% confidence actually mean ~90% correctness? |
| **Structured output validity** | Does JSON/schema stay consistent?                   |

For this particular use case, I would favor a model that scores:

> **97% accuracy + excellent abstention**

over one that appears to be:

> **99% accurate but confidently invents the other 1%.**

---

# 4. Separate extraction from interpretation

I would test two architectures.

### Architecture A. Single multimodal model

```text
Chart
 ↓
Vision LLM
 ↓
Diagnosis + evidence + ICD
```

Very simple and potentially fast.

### Architecture B. Two-stage

```text
Chart
 ↓
Vision / Document model
 ↓
Structured clinical evidence
 ↓
LLM + RAG / rules
 ↓
ICD/HCC interpretation
```

For CareAllies, I suspect **B eventually wins** because it gives much better QA.

You can separately ask:

> Did we read the chart correctly?

and:

> Did we interpret the evidence correctly?

That matters enormously when someone asks why the system challenged an auditor.

---

# 5. Benchmark the actual infrastructure

The missing runtime row is more important than it looks.

For each model capture:

* End-to-end seconds/page
* Tokens/sec
* CPU/GPU utilization
* GPU RAM requirement
* Batch throughput
* Maximum pages/context
* Model loading time
* Cost per 1,000 charts
* Concurrent-user performance

Then you can answer Brad's question:

> **“What would this actually cost us?”**

For example, the model that finishes a page in 5 seconds but requires an enormous GPU may be economically worse than a smaller model that takes 12 seconds.

---

# Questions I would ask Yuze / the team next

### About the benchmark

1. **Were these 23 conditions known to the model through the prompt, or was it completely blind?**
2. **Did the chart explicitly contain ICD codes, or did the model derive codes from diagnosis text?**
3. **Are the 23 examples independent handwriting styles or one document/one writer?**
4. **Were any prompts changed between models?**
5. **Was temperature fixed at zero or equivalent?**
6. **If we run each case five times, do we get the same answer?**
7. **What happens when the correct answer is “I cannot read this”?**
8. **Are we recording page-level evidence for every extracted diagnosis?**
9. **How much GPU/RAM does each model require?**
10. **What are the licensing restrictions for enterprise healthcare use?**

That last question should happen **before** you invest heavily in a particular open model.

---

# For Audit the Auditor specifically

I'd make the next benchmark something like:

### Input

Clinical chart + auditor decision.

### Model must return

```text
Auditor decision:
Unsupported diagnosis

AI assessment:
Potential disagreement

Diagnosis:
CKD Stage 3

Supporting evidence:
Page 4, Assessment
Page 6, nephrology follow-up
Page 8, relevant laboratory evidence

Rule / methodology:
Retrieved through RAG

Confidence:
High

Recommended action:
Secondary human review
```

Then compare that output to a **blind SME/coder ground truth**.

That is much closer to demonstrating ROI than simply showing that the model can decode handwriting.

---

# One improvement to the benchmark deck

I'd add a second page titled:

## **Production Readiness Scorecard**

with something like:

| Model   | Accuracy | Hallucination | Abstention | Runtime | Hardware | Output Control | Overall |
| ------- | -------: | ------------: | ---------: | ------: | -------: | -------------: | ------: |
| Test 01 |        5 |           TBD |        TBD |       4 |      TBD |              5 |     TBD |
| Test 02 |        5 |           TBD |        TBD |     TBD |      TBD |              3 |     TBD |
| Test 03 |        5 |           TBD |        TBD |       5 |      TBD |              5 |     TBD |
| Test 04 |        3 |           TBD |        TBD |       2 |      TBD |              5 |     TBD |
| Test 05 |        5 |           TBD |        TBD |       2 |      TBD |              5 |     TBD |

Right now you have an **accuracy benchmark**.

What leadership ultimately needs is a:

> **production-readiness benchmark.**

And from what is shown here, **Test 03 looks like the most interesting candidate for deeper testing, with Test 01 very close behind**, but the next round should be designed to make the models fail. That is where you'll learn which one you can actually trust.

**CareAllies needs a shared technology foundation, with different analytical workflows built on top. Most projects require configuring existing AI models and building the surrounding system. Provider profiling and prediction are where training your own ML models becomes more relevant.**

One adjustment to the earlier slide discussion: **document extraction, structured analytics, and provider ML can develop in parallel.** Chart extraction is a dependency for chart-based applications, not for the entire platform.

**The shared technology foundation**

These are proposed components, subject to HCSC’s approved environment. You do not need every component before starting a pilot.

| Capability                       | What we need to build                                                                        | Potential technology                                                     | Projects enabled                                                   |
| -------------------------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| **Data integration**             | Repeatable pipelines joining members, providers, claims, Stars, risk, and engagement history | Existing Teradata/Redshift, Python, Airflow, Parquet                     | Risk, Stars, provider intelligence                                 |
| **Document storage**             | Store original charts, guidance, contracts, and document versions                            | Approved S3, Azure Blob, or enterprise document storage                  | Clinical extraction, audit, knowledge applications                 |
| **Clinical evidence extraction** | Read documents and produce structured evidence with page references                          | PDF text parser, OCR/document intelligence, approved vision-capable LLM  | Audit the Auditor, Stars chart evidence                            |
| **Knowledge retrieval / RAG**    | Ingest guidance, split into meaningful sections, index, retrieve relevant passages           | Embedding model plus Azure AI Search or PostgreSQL/pgvector              | Audit, CMS intelligence, knowledge and contract applications       |
| **Governed analytical tools**    | Reusable functions that calculate metrics and investigate changes using approved data        | Parameterized SQL, Python, API endpoints, versioned rules                | Risk and Stars investigators                                       |
| **LLM access and orchestration** | Route requests, retrieve context, call analytical tools, validate responses                  | Approved enterprise model API; Python/FastAPI                            | Applications using language or document interpretation             |
| **ML development**               | Create features, train models, evaluate predictions, store model versions                    | scikit-learn, XGBoost, MLflow                                            | Provider segmentation, engagement prediction, optional forecasting |
| **Application and workflow**     | Present findings, source evidence, reviewer decisions, and follow-up tasks                   | Streamlit for pilot; supported web application for production            | All user-facing projects                                           |
| **Security and evaluation**      | Enforce access, protect sensitive data, track versions, measure errors, monitor cost         | Enterprise identity, secrets management, audit logs, evaluation datasets | All projects                                                       |

For example, Azure Document Intelligence supports printed and handwritten text extraction with locations. That makes it a candidate to benchmark on your charts, not proof that it will read your handwriting accurately enough. [Microsoft documentation](https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/prebuilt/read?view=doc-intel-4.0.0)

RAG search can combine keyword and vector retrieval. Access restrictions must also apply during retrieval so users only receive permitted content. [Microsoft RAG guidance](https://learn.microsoft.com/en-us/azure/foundry/concepts/retrieval-augmented-generation)

**Do we need a database other than Teradata?**

**Keep Teradata as the source for structured analytical data.** Add storage capabilities according to their purpose:

| Information                                             | Recommended location                                                      |
| ------------------------------------------------------- | ------------------------------------------------------------------------- |
| Claims, membership, provider metrics, calculated scores | Existing Teradata/Redshift                                                |
| Original PDFs, scans, document images                   | Object/document storage                                                   |
| Searchable guidance passages and embeddings             | Search/vector index, if required                                          |
| Extracted clinical evidence                             | Structured tables with links back to original documents                   |
| Review decisions, processing jobs, application state    | Existing suitable transactional database, or a small application database |
| Trained models and evaluation history                   | Model registry and artifact storage                                       |

You do not need to copy the entire warehouse into a vector database. **SQL handles exact counts and calculations; search retrieves relevant text.**

**How we build the model and system for each project**

| Project                                   | Model approach                                                             | System build sequence                                                                                                                                                    | What CareAllies develops                                                                           |
| ----------------------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| **Clinical Documentation Intelligence**   | Existing OCR and/or multimodal LLM. No initial custom training             | Receive chart → extract digital text or read image → identify evidence → validate structured fields → store evidence with source locations                               | Evidence schema, document routing, extraction prompts, validation checks, review screen            |
| **Audit the Auditor**                     | Existing LLM + retrieved guidance + deterministic rules                    | Retrieve clinical evidence → retrieve applicable guidance → perform exact mappings/checks → assess support → compare with auditor decision → send exceptions to reviewer | Audit comparison logic, versioned rule tables, disagreement categories, adjudicated evaluation set |
| **CareAllies Intelligence Layer**         | Existing LLM and embedding model                                           | Authenticate user → identify question → retrieve authorized knowledge and/or call analytical tools → validate results → return answer with sources                       | Data-tool APIs, retrieval pipeline, permissions, prompt templates, evaluation framework            |
| **Risk Score Investigator**               | SQL/Python decomposition + LLM explanation. Predictive ML optional         | Select population and periods → reconcile risk scores → quantify membership, coding, and methodology effects → rank drivers → generate explanation                       | Reproducible investigation functions, comparable-period logic, risk methodology implementation     |
| **Stars Performance Investigator**        | SQL/Python rules + LLM explanation. Prediction optional                    | Retrieve measure trends → analyze denominator and population changes → identify gap/PDC drivers → prioritize opportunities → explain findings                            | Measure-specific calculations, recoverability logic, prioritization, intervention tracking         |
| **Stars Clinical Evidence Gap Detection** | Shared document extraction + measure rules + optional RAG interpretation   | Read chart → extract dated evidence → match to member and measure → check eligibility, timing, exclusions, and accepted evidence → reviewer validation                   | Measure evidence definitions, matching logic, false-closure controls, reviewer workflow            |
| **Provider Behavior Profiling**           | Train clustering models on engineered features. LLM optional for summaries | Build provider-period dataset → normalize features → fit clusters → assess stability → have SMEs interpret segments → publish profiles                                   | Feature definitions, segmentation model, actionable segment descriptions                           |
| **New-Market Provider Prediction**        | Train supervised ML on historical outcomes                                 | Define engagement outcome → assemble information available before engagement → train → validate on held-out markets → calibrate → score new providers                    | Outcome labels, predictive model, uncertainty estimates, monitoring                                |

For Audit the Auditor, **retrieve the guidance applicable to the encounter and audit context**, which is not necessarily the newest published guidance. Exact ICD/HCC mappings should be versioned table lookups, not something the LLM guesses from retrieved paragraphs.

For Stars, chart evidence supports only the measures and evidence pathways that permit it. A clinical note should not automatically override a claims-based adherence calculation.

**What “building the model” actually involves**

For the LLM applications, I would follow this sequence:

1. **Define the output.** For example: member, encounter date, evidence type, finding, source page, applicable rule, review reason.
2. **Create an SME-reviewed benchmark.** Include clean digital charts, difficult handwriting, missing information, contradictions, and negative cases.
3. **Test approved existing models.** Compare extraction quality, supported conclusions, processing time, and cost.
4. **Build the retrieval and analytical tools.** Give the model relevant evidence and calculated results.
5. **Improve prompts, document processing, and validation.**
6. **Consider fine-tuning only if repeatable errors remain and sufficient reviewed training examples exist.**

RAG ingestion does **not** train the LLM. Generating embeddings also normally uses an existing model.

For provider ML, training is a different process:

1. Define observable outcomes, such as response within a specified period or completion of an agreed action.
2. Build historical features using only information available at prediction time.
3. Establish a simple baseline, then compare more complex models.
4. Separate training and evaluation by time and, for new-market prediction, by market.
5. Check calibration, stability, and operational usefulness before deployment.

Clustering can begin with ordinary numeric and categorical features. **Embeddings are optional**, especially useful if you later incorporate narrative engagement notes. Standard clustering methods are available in [scikit-learn](https://scikit-learn.org/stable/modules/clustering.html).

Avoid assigning psychological labels such as “financially motivated” unless you have evidence. A defensible segment might be **“high meeting attendance, low action completion.”**

**How I would sequence the work**

| Workstream               | First concrete deliverable                                                  | Next application                                 |
| ------------------------ | --------------------------------------------------------------------------- | ------------------------------------------------ |
| **Shared platform**      | Approved model endpoint, authenticated API, logging, small evaluation suite | Reusable foundation across projects              |
| **Clinical evidence**    | Chart-to-evidence pipeline with page-linked review                          | Audit the Auditor and Stars evidence detection   |
| **Structured analytics** | Tested risk/Stars investigation functions exposed through an API            | Conversational investigators                     |
| **Provider ML**          | Provider-period feature table and baseline segmentation                     | Engagement prediction after outcomes are defined |

Across these workstreams, version the data snapshot, rules, prompts, model, and evaluation results. MLflow is one option for managing trained-model versions and lineage. [MLflow model registry](https://mlflow.org/docs/latest/ml/model-registry/)

**My initial infrastructure request would be:** an approved LLM/vision API, secure read-only data connectivity, document storage, one retrieval service, an application runtime, and SME evaluation time. Managed model APIs generally remove the need to provision your own LLM GPUs.

The largest CareAllies investment is then the **evidence definitions, analytical logic, provider features, validated examples, and operational workflows**. Those are the reusable capabilities that make the system specific to your business.
