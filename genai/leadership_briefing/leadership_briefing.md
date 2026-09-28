The key distinction for leadership is that **the work discussed in this meeting is an AI developer-enablement pilot, not yet the full CareAllies LLM platform needed for business applications such as Audit the Auditor or the CareAllies LLM/Data Lake initiative**.

Below is the executive summary I would use.

## Executive Summary

CareAllies is currently working toward a **controlled enterprise LLM development capability using Azure AI Foundry and VS Code**. The immediate objective is to provide technical teams with approved access to enterprise-hosted LLMs so they can accelerate coding, testing, documentation, automation, and proof-of-concept development.

The proposed pilot intentionally has a small security footprint. Developers would access approved models through Azure-hosted endpoints, either directly through the corporate network/VPN or through a controlled Azure VM using Bastion. Model usage would be authenticated, metered, and governed, and the AI environment would not have direct access to production databases or production infrastructure.

### What is currently being requested / worked on

**1. Approved enterprise LLM access**

The immediate request is to establish Azure AI Foundry endpoints that can expose approved OpenAI or Anthropic models to CareAllies technical users.

The goal is to replace unsupported or local experimentation with an enterprise-controlled model-access layer.

**2. VS Code-based AI development capability**

VS Code is being positioned as the initial user interface for developers.

The proposed experience would allow an approved AI coding agent to work with repositories and project files to assist with:

- Python and SQL development
- Debugging and refactoring
- Documentation
- Test development
- Code review
- Automation development
- Proof-of-concept creation

**3. Controlled access architecture**

Two access patterns are currently being evaluated:

- Existing Invista-connected users accessing Azure Foundry through the corporate VPN.
- CareAllies users accessing an Azure-hosted VM through Azure Bastion, with VS Code and the approved AI tooling installed inside the controlled environment.

**4. Enterprise security and governance design**

The architecture is being documented for security review. Open questions currently being worked through include:

- Model hosting location
- Prompt and response retention
- Telemetry
- Whether data can be used by providers for debugging, optimization, or training
- RBAC
- API-key management
- Private endpoints
- Network boundaries
- Approved model versions
- Usage monitoring
- Per-user cost controls

The stated goal is to keep CareAllies information inside an approved enterprise boundary and prevent organizational data from being reused externally.

**5. Cost and usage management**

The proposed architecture would meter usage at the user/API level instead of providing unrestricted model access.

A rough pilot assumption discussed was approximately $20-$30 of model usage per person per month, with model selection balancing capability and cost.

**6. Productivity measurement**

The team has been asked to establish baseline metrics before the pilot begins so the impact of AI enablement can be demonstrated.

Examples include:

- Development turnaround time
- Jira resolution time
- Manual hours per process
- Test coverage
- Documentation coverage
- Throughput
- Project completion time
- Ability to absorb additional work without additional FTE capacity

---

## What this enables immediately

This architecture would provide an important first layer of enterprise AI capability:

**Developer AI Enablement**

```text
Developer
   ↓
VS Code / Python / SQL
   ↓
Approved AI Agent
   ↓
Azure AI Foundry
   ↓
Enterprise-approved LLM
```

This supports development and experimentation, but it does **not yet constitute the complete CareAllies AI platform**.

The initial capability primarily gives technical teams access to an approved model.

---

## What is still missing for CareAllies LLM business enablement

To support projects such as **Audit the Auditor, Risk Score Investigator, Stars evidence investigation, Provider Behavior Profiling, Clinical Documentation Intelligence, and the CareAllies LLM/Data Lake**, additional shared capabilities are required.

### 1. Governed enterprise data-access layer

The current AI pilot does not provide the LLM with governed access to CareAllies business data.

For the broader architecture, the AI layer needs controlled access to sources including:

- Claims and membership in Teradata
- Clinical notes and clinical evidence in AWS Redshift
- Provider demographics and alignment
- Risk adjustment / HCC information
- Stars measures and evidence
- Utilization information
- Engagement data
- Audit results
- Internal methodology and business rules

The LLM should not connect directly to databases without controls. A governed service or tool layer is needed between the model and enterprise data.

Conceptually:

```text
Teradata / Redshift / Enterprise Data
               ↓
      Governed Data Services
               ↓
          LLM Tool Layer
               ↓
             LLM
```

### 2. Clinical document and evidence extraction

Audit the Auditor and similar use cases require the platform to interpret clinical documents, not simply structured database tables.

A reusable capability is still needed for:

- Clinical note ingestion
- PDF/chart parsing
- OCR where necessary
- Evidence extraction
- Diagnosis/date/provider extraction
- Structured conversion of clinical evidence
- Evidence lineage back to the source document

For Audit the Auditor, this is fundamental because the system must compare **what is documented in the chart** with **what the auditor decided**.

### 3. Enterprise RAG / knowledge layer

CareAllies business applications require authoritative knowledge that changes over time.

Examples include:

- CMS guidance
- ICD mappings
- HCC model versions
- Risk-adjustment guidance
- Stars specifications
- Internal methodology
- Policies
- Business rules

A versioned RAG capability is therefore still required.

For example:

```text
CMS / HCC / ICD / Internal Guidance
                ↓
       Document Processing
                ↓
       Search / Vector Index
                ↓
              RAG
                ↓
               LLM
```

The LLM must be able to cite the specific rule, document, version, and evidence supporting its conclusion.

### 4. Shared CareAllies AI orchestration layer

A reusable service is needed between applications and the models.

Rather than every project independently calling an LLM, CareAllies needs a governed orchestration layer capable of handling:

- Prompting
- Model selection
- Tool calling
- SQL/data requests
- Retrieval
- Document evidence
- Business rules
- Validation
- Logging
- Human review
- Application APIs

Conceptually:

```text
Applications
     ↓
CareAllies AI Intelligence Layer
     ↓
 ┌────────────┬────────────┬──────────────┐
 Data Tools   RAG/Search   Document AI
 └────────────┴────────────┴──────────────┘
                    ↓
             Approved LLMs
```

This shared layer is what allows multiple CareAllies use cases to reuse the same enterprise AI foundation.

### 5. Deterministic business-rule services

For healthcare use cases, the LLM should not independently decide everything.

Projects such as Audit the Auditor and the CareAllies LLM/Data Lake require deterministic calculations and rule engines alongside the LLM.

Examples include:

- ICD-to-HCC mapping
- CMS model-year logic
- CPT/CPT-II validation
- NDC logic
- PDC calculations
- Stars measure calculations
- Eligibility logic
- Risk-score calculations

The architecture should therefore combine:

```text
LLM reasoning
       +
RAG / evidence
       +
Deterministic rules
       +
Structured analytics
```

rather than using the LLM as the calculation engine.

### 6. Evaluation and human-review framework

A reusable AI evaluation layer is also still needed.

For Audit the Auditor specifically, the workflow requires comparison of:

```text
Clinical Evidence
        +
CMS / HCC Rules
        +
Auditor Decision
        ↓
Agree / Disagree / Review
        ↓
Human Adjudication
```

The human decision becomes an important source of evaluation data and may eventually support model improvement or fine-tuning.

Similar evaluation frameworks will be needed for other CareAllies AI applications.

### 7. PHI-enabled architecture

The current developer pilot is intentionally restricted and does not yet establish the complete architecture for member-level or PHI-enabled AI processing.

CareAllies business use cases will eventually require approval for:

- PHI processing
- Private network paths
- Data retention
- Encryption
- BAA/vendor requirements
- Logging and auditability
- Identity and access controls
- Model-provider boundaries
- Data lineage

This is one of the most important remaining enterprise dependencies.

---

## How the current work fits the larger CareAllies architecture

The proposed Azure AI Foundry pilot should be viewed as **Layer 1 of the broader LLM enablement strategy**.

```text
LAYER 5
Business AI Applications
Audit the Auditor
Risk Score Investigator
Stars Investigator
Provider Profiling
Clinical Documentation Intelligence

                    ↑

LAYER 4
CareAllies AI Intelligence / Orchestration Layer
Agents | Tool Calling | Validation | APIs | Human Review

                    ↑

LAYER 3
Enterprise AI Services
RAG | Vector Search | Document AI | Embeddings
Deterministic Rules | SQL/Data Tools

                    ↑

LAYER 2
CareAllies Data Foundation
Teradata | Redshift | Claims | Membership
Clinical Notes | Providers | Stars | Risk | Documents

                    ↑

LAYER 1
Enterprise Model Enablement
Azure AI Foundry
Approved LLMs
VS Code
Authentication
Metering
Security Controls
```

**The work currently underway is primarily Layer 1.**

CareAllies already has much of the structured data required for Layer 2, but the governed integration between that data and the AI layer is not yet established.

Layers 3 and 4 represent the largest capability gaps between the current developer-enablement pilot and a production CareAllies LLM platform.

---

## Example. Audit the Auditor

Audit the Auditor illustrates why model access alone is insufficient.

The target workflow requires:

```text
Arcadia / Clinical Charts
          ↓
Clinical Evidence Extraction
          ↓
Structured Evidence
          ↓
                         CMS / HCC Guidance
                                ↓
                           RAG / Rules
                                ↓
Auditor Decision ───────────────┤
                                ↓
                       AI Reconciliation
                                ↓
                Agree / Exception / Review
                                ↓
                       Human Adjudication
```

The currently proposed Foundry capability supplies the **model** in this architecture.

Still required are:

- Clinical chart ingestion
- Evidence extraction
- CMS/HCC RAG
- Versioned business rules
- Auditor-result integration
- Deterministic validation
- Human-review workflows
- Evaluation
- PHI-enabled production architecture

---

## Example. CareAllies LLM / Data Lake

The CareAllies LLM/Data Lake concept requires similar shared infrastructure.

The proof of concept has already demonstrated the pattern of combining:

- Clinical notes
- Stars member/provider information
- Claims evidence

and using deterministic CPT, CPT-II, NDC, and PDC reconciliation before asking the LLM to explain and cite matches, mismatches, unsupported gaps, or apparent evidence inconsistencies.

The enterprise version would require:

```text
Claims / Membership      Clinical Notes       Stars / Risk / Provider
       ↓                       ↓                        ↓
    Teradata                Redshift              Enterprise Data
       └──────────────────────┬─────────────────────────┘
                              ↓
                    Governed Data Layer
                              ↓
             Rules + Evidence Extraction + RAG
                              ↓
                CareAllies AI Intelligence Layer
                              ↓
                       Approved LLM
                              ↓
                 Business Applications
```

---

## Leadership takeaway

**The organization is making progress on the first foundational requirement: secure enterprise access to advanced LLMs.**

However, **LLM access by itself does not deliver CareAllies AI business capability**.

To move from AI-assisted development to applications such as Audit the Auditor and the CareAllies LLM/Data Lake, the next investment needs to establish the reusable middle layers:

1. Governed connectivity to CareAllies data
2. Clinical document/evidence extraction
3. Enterprise RAG and versioned knowledge
4. Deterministic healthcare rule services
5. AI orchestration and application APIs
6. Evaluation and human-review workflows
7. PHI-compliant production controls

The strategic opportunity is to build these as **shared CareAllies capabilities rather than independently for each AI project**. Once established, the same architecture can support Risk Adjustment, Stars, provider intelligence, clinical documentation, utilization analysis, and future AI applications without recreating the foundation for every use case.

The most important leadership framing is the **Layer 1 vs. Layers 2–4 distinction**. It prevents the Foundry/VS Code pilot from being interpreted as “we now have our enterprise AI platform.” It is the model-access foundation. The **data, RAG, evidence, rules, orchestration, and governance layers are what turn it into CareAllies-specific intelligence**.
