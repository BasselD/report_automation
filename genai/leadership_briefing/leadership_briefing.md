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

----
# Detail
## 1. What is going on

The meeting centered on **two related technology initiatives**:

1. Building stronger **data observability and self-service visibility** around data sources, freshness, ownership, dependencies, and pipeline health.
2. Designing a **low-risk enterprise AI development environment** that gives analytics/development teams access to frontier LLMs through Azure without giving the models direct access to production systems.

The AI discussion was the larger portion of the meeting. The proposed approach is intentionally minimal. The goal is to get useful AI-assisted development capabilities approved without first implementing a large AI platform, introducing substantial new infrastructure, or allowing autonomous AI access to production.

---

# 2. Data observability and data-freshness discussion

The stated target is an internal capability where users do not have to email other teams to determine:

- Where a dataset is located
- Where it originated
- Who owns it
- How frequently it refreshes
- When it was last refreshed
- When the next refresh is expected
- Whether the dataset and its upstream dependencies are currently healthy

The longer-term concept is effectively a **green/red operational health indicator for data assets**.

For example:

```text
Source / Feed
      ↓
Landing / ingestion
      ↓
Transformation dependencies
      ↓
Data product / report
      ↓
Current freshness state
      ↓
GREEN / DEGRADED / RED
      ↓
Last refresh + expected next refresh
```

### Existing examples already in use

For Stars reporting in **Arcadia**, there is already a table or view that exposes freshness information for dependency tables.

That information is used by automation logic to decide whether a downstream process should execute.

A specific example discussed was Stars data having two different dependency structures:

- Numerator/denominator information used to calculate Stars results
- Supporting evidence information containing details such as the most recent provider

If the evidence table has not refreshed, overall Stars calculations can still work, but detailed fields begin appearing as null. The automation can therefore distinguish between a fully refreshed state and a partially usable state.

### Pharmacy claims monitoring

The team also monitors pharmacy claims volumes because refresh sizes vary considerably.

Examples given ranged from only a handful of new rows on one day to roughly 500,000 rows on another.

Statistical logic is used to determine whether the incoming volume is large enough to justify triggering another refresh. The discussion referenced using a comparison around the median rather than simply executing the process every time any new rows appear.

### Airflow

Airflow was referenced as another area where dependencies already exist.

DAGs can require one upstream process or dataset to be refreshed before downstream work begins.

The broader proposal is to extend this type of dependency awareness beyond individual pipelines and make it visible more systematically.

---

# 3. Arcadia data-feed visibility

There was discussion of a possible Arcadia administrative capability that provides metadata about incoming feeds and pipeline activity.

The exact tool was **not confirmed during the meeting**.

The discussion described a possible Arcadia-maintained dashboard or set of views containing information about:

- Incoming feeds
- Flat-file ingestion
- Copy/load processes
- Source identifiers
- Pipeline state
- Data arriving into Arcadia

One participant had heard that such a capability exists but did not have access to it.

### What is currently visible

Within **Amazon Redshift**, there is an Arcadia schema referred to as **Origin**.

That schema exposes relatively raw or minimally transformed incoming data from health plans.

The limitation discussed was that seeing raw data in the Origin schema does not necessarily provide the higher-level metadata needed to answer questions such as:

- Which CDI feed produced the data?
- Which HIE is the source?
- Is that feed currently operational?
- When did the feed last successfully deliver data?

The group believes some additional metadata or administrative layer may exist within Arcadia, but that needs to be investigated.

### Why this matters

A specific example was given involving a Philadelphia-related CDI/ADT feed whose contract was not renewed.

The feed subsequently stopped operating, and the problem was discovered later when someone investigated utilization data.

The desired future state would have produced something like:

```text
Philadelphia ADT/CDI Feed
Status: RED
Last successful delivery: January 1
Expected delivery: Daily
Owner: <owner>
Dependency impact: ED utilization reporting
```

That is one of the practical use cases motivating the observability initiative.

---

# 4. Dashboard adoption and usage analytics

The meeting also covered the problem of building analytical tools that receive little adoption.

An older **Arcadia EMR Coverage dashboard** was discussed.

Its historical purpose included measuring EMR coverage and helping identify provider panels with low EMR connectivity so teams could prioritize outreach.

The dashboard apparently had a limited audience and received little ongoing maintenance. At different points it may have been removed from Tableau because of inactivity.

Someone is now working on improving it.

The discussion emphasized understanding usage before continuing to invest heavily in dashboards.

### Tableau

Tableau provides usage information natively, but access is restricted by the Tableau server administrators.

The team appears to receive some summarized usage information, rather than full underlying activity logs.

### Arcadia

Arcadia provides more usage visibility, including metrics such as:

- User logins
- Dashboard interaction
- Session duration
- Page stay time

There is already work underway to use these metrics for Population Health reporting and adoption analysis.

The stated objective is to understand whether analytical products are actually being used and whether existing products should be improved rather than creating new ones.

---

# 5. Proposed enterprise AI architecture

This was the primary technical discussion.

The proposal is to create a **minimal enterprise AI development architecture using Azure AI Foundry**.

The design is specifically intended to minimize:

- New procurement
- New vendor contracts
- Security exposure
- Infrastructure requirements
- Production-system access
- AI risk-profile complexity

The core concept is:

```text
Developer
   ↓
VS Code
   ↓
AI extension / agent harness
   ↓
Azure AI Foundry endpoint
   ↓
Approved LLM
```

Instead of purchasing a completely separate development environment, the team would expose an approved model through an Azure endpoint and connect VS Code to that endpoint.

---

# 6. Azure AI Foundry

**Azure AI Foundry** would act as the managed model hosting/inference layer.

The speaker's proposal is to deploy or expose an LLM through Foundry and provide developers with controlled access to that endpoint.

The models mentioned during the discussion included variants from:

- OpenAI
- Anthropic / Claude
- Other models available through Azure Foundry

The discussion referenced several current model names and variants. The exact model selected for the pilot has **not yet been determined**.

The preference is for models whose inference infrastructure is provided directly through Azure wherever possible.

That preference is driven primarily by security and compliance requirements.

---

# 7. Why Azure-hosted models are preferred

Azure Foundry includes models with different hosting arrangements.

Some are effectively provided directly by Azure.

Others are available through Azure but are ultimately hosted or served by another provider.

The discussion specifically distinguished examples such as:

```text
Azure Foundry
   ├── Model served directly through Azure
   │
   ├── Anthropic-hosted model
   │
   └── Third-party hosted model
```

The direct-from-Azure option was described as having the smaller security and compliance footprint.

This becomes particularly important for healthcare because the team needs to document:

- Where prompts are processed
- Where data is retained
- Whether telemetry is captured
- Whether prompts can be used for debugging
- Whether prompts can be used for training
- Which external organizations can access data
- Retention periods
- Private networking
- Access controls

---

# 8. Why they are not proposing locally hosted open-source models

Open-source models were explicitly discussed.

A team member asked whether models such as Qwen or similar models could be hosted instead of paying for commercial frontier models.

The answer was that this is technically possible.

However, locally hosting and securing the model would introduce additional infrastructure and operational responsibilities.

The organization would need to manage things such as:

```text
Model runtime
+ GPU / compute infrastructure
+ patching
+ network isolation
+ telemetry controls
+ security testing
+ authentication
+ authorization
+ monitoring
+ model upgrades
+ compliance documentation
```

The speaker stated that the current infrastructure/operations capacity is not sufficient to comfortably own that additional responsibility.

Therefore, the proposed pilot intentionally favors managed Azure services.

---

# 9. Developer interface. VS Code

The proposed primary user interface is **Visual Studio Code**.

VS Code would contain an AI extension or coding-agent interface.

The speaker demonstrated the type of experience being considered using a Claude-related extension.

Conceptually:

```text
VS Code
│
├── Repository
├── Source files
├── Terminal
├── Local project files
│
└── AI Agent
      ↓
 Azure AI Foundry
      ↓
 Approved model
```

The agent would be able to see the repository or project opened in VS Code.

It could therefore assist with activities such as:

- Reviewing code
- Modifying code
- Troubleshooting errors
- Reading README files
- Applying coding conventions
- Generating features
- Producing documentation
- Writing or improving tests
- Assisting with refactoring

---

# 10. Code repositories

The discussion made a distinction between **code** and **supporting project context**.

Production Python and other code should remain in normal repositories such as:

- Bitbucket
- Azure DevOps
- GitHub

The AI-enabled development environment would clone or open those repositories in the same way developers already work with repositories.

The model would then operate against the local working copy.

---

# 11. SharePoint and OneDrive

**SharePoint** and potentially **OneDrive** would primarily be used for project context and supporting materials, not as the code repository.

Examples included:

- Business requirements
- Documentation
- Input/output examples
- De-identified examples
- Debugging information
- Project collateral
- Reference files

There is already an Invista/CareAllies collaboration SharePoint site.

The proposal is to make that accessible from the controlled environment.

A SharePoint folder could potentially also be synchronized through OneDrive.

---

# 12. Two proposed user-access patterns

Because the organizations and technology environments are not fully merged, two different access patterns were described.

### Path 1. Users already operating from the Invista environment

```text
Invista-managed workstation
       ↓
Corporate VPN
       ↓
Azure network
       ↓
Azure AI Foundry endpoint
       ↓
Approved model
```

These users already have VPN connectivity capable of reaching Azure resources.

Their local VS Code instance could therefore communicate with the Azure endpoint.

### Path 2. CareAllies / other users without equivalent workstation access

The second population would use a controlled Azure virtual machine.

```text
CareAllies / HillSpring workstation
       ↓
Browser
       ↓
Azure Portal
       ↓
Azure Bastion
       ↓
Controlled Azure VM / jump box
       ↓
VS Code
       ↓
Azure AI Foundry endpoint
```

This avoids requiring direct connectivity from their existing workstation.

---

# 13. Azure Bastion and VM architecture

**Azure Bastion** was demonstrated as the controlled remote-access mechanism.

Instead of a traditional unmanaged remote desktop workflow, users would authenticate through Azure and enter a managed virtual machine.

Inside that VM, the organization could preconfigure:

- VS Code
- Approved extensions
- Repository access
- SharePoint/OneDrive access
- Foundry endpoint configuration
- Required development tooling

This VM becomes the controlled AI development workspace.

---

# 14. Per-user API keys and usage metering

The proposal includes assigning access to the AI endpoint per user.

Each user could receive an API key or equivalent identity tied to their account.

That allows usage to be tracked and limited.

The speaker discussed a rough budget concept in the neighborhood of:

**$20 to $30 of model consumption per user per month**

This was contrasted with a GitHub Copilot-style pricing model where a fixed amount is charged per seat.

With Foundry, usage could instead be metered according to actual token consumption.

Conceptually:

```text
User
 ↓
Unique credential / API access
 ↓
Azure AI endpoint
 ↓
Usage meter
 ↓
Per-user token budget
```

---

# 15. Model cost management

Model selection would consider both capability and token cost.

The meeting noted that highly capable frontier models can consume very large token volumes during long-running agentic tasks.

Because some models are substantially more expensive than others, the pilot would likely use a strong but relatively cost-controlled model.

The design allows model choice to be changed without redesigning the entire developer environment.

---

# 16. Production access is intentionally excluded

One of the strongest architectural boundaries discussed was **no direct production-system access**.

The proposed AI tooling would not be expected to connect directly to:

- Production databases
- Production applications
- Production infrastructure
- Administrative Azure resources

This is deliberate.

The speaker gave examples of why autonomous AI access to production systems creates unacceptable risk.

The model should be able to assist with development, but should not be able to independently execute changes against production infrastructure.

---

# 17. Testing strategy

Because production database connectivity will not be available, developers may need to create local or synthetic test fixtures.

For example:

```text
Production database
      X
      │ no direct access
      │
Developer repository
      ↓
Synthetic / dummy database
      ↓
SQLite or other test fixture
      ↓
AI-assisted development/testing
```

A Python process could therefore still be developed and tested without giving the AI direct production access.

---

# 18. Sandboxes and autonomous execution

Modern coding agents often create local environments or sandboxes where they execute code.

Examples discussed included:

- Linux environments
- Containers
- Docker
- Temporary VMs
- Shell/terminal execution

This creates a security challenge because it can effectively permit arbitrary code execution.

For the initial pilot, some of those capabilities may therefore be disabled or restricted.

The guiding principle is to reduce the pilot's risk profile rather than immediately enable every autonomous capability.

---

# 19. Role-based access and workstation controls

The proposal also benefits from existing workstation restrictions.

If a user cannot:

- Install software
- Elevate privileges
- Access certain infrastructure
- Execute administrative commands

then the AI agent operating under that user's credentials should inherit those same restrictions.

This existing security model is expected to help constrain AI behavior.

---

# 20. Healthcare data and compliance requirements

Before the pilot can be approved, the architecture needs to address the organization's internal AI policy.

Specific items mentioned included:

- Prompt retention
- Telemetry
- Training usage
- Debugging usage
- Data retention periods
- Third-party access
- RBAC
- API access
- Private endpoints
- Network isolation
- Model versions
- Preview versus generally available models
- Deployment architecture
- Vendor/business-associate implications
- Data boundaries

A particularly important requirement is ensuring organizational data is **not reused by external model providers for training, debugging, troubleshooting, or optimization**.

The speaker noted that disabling training alone is not considered sufficient. Debugging and telemetry flows also need to be addressed.

---

# 21. Security and AI risk approval

The architecture is being developed with **Paul Johnson / security**.

The speaker described creating detailed documentation, potentially around ten pages, describing:

- Architecture
- Risks
- Security controls
- Operators
- Boundaries
- RBAC
- Model behavior
- Data handling

The initiative cannot proceed simply because the technical components work.

It needs formal approval against the internal AI policy and security standards.

---

# 22. GitHub Copilot versus the proposed approach

GitHub Copilot had previously been considered.

The speaker described a GitHub product with:

- GitHub integration
- Coding-agent functionality
- Repository awareness
- Pull-request functionality
- A development interface
- Per-seat pricing

However, adopting it would require additional procurement and AI risk review.

That led to the question:

> Can the organization obtain most of the useful development capability using infrastructure and contracts already available through Azure?

The current proposal is intended to answer that question.

---

# 23. Existing local proof-of-concept work

One participant described an existing local proof of concept using:

```text
Ollama
   ↓
Local model runtime
   ↓
Qwen
   ↓
Python process
   ↓
Input → model → output
```

The proposed enterprise architecture would replace the local Ollama/model-serving layer with the approved Azure endpoint.

Conceptually:

```text
Current local POC

Python
 ↓
Ollama
 ↓
Qwen


Proposed enterprise model

Python / VS Code
 ↓
Azure AI Foundry endpoint
 ↓
Approved managed model
```

That means much of the conceptual workflow could remain intact while the inference layer becomes enterprise-managed.

---

# 24. Broader strategic use case

The AI pilot is not only intended for interactive coding assistance.

One stated motivation is supporting development of the **CareAllies data warehouse** and allowing a relatively small technical team to deliver more work.

Potential benefits discussed included:

- Faster development
- More automation
- Faster onboarding of new contracts
- Better quality
- Improved testing
- Better documentation
- Reduced manual work
- Increased throughput
- Reduced need for additional FTE capacity
- Potentially avoiding purchases of software that could instead be built internally

No specific savings or productivity percentage was committed to during the meeting.

---

# 25. How the pilot will be evaluated

A significant portion of the discussion focused on **establishing baseline metrics before the AI tools are introduced**.

Suggested metrics included:

| Area | Possible measurement |
|---|---|
| Request delivery | Time from request to completion |
| Development throughput | Projects or tickets completed |
| Jira | Average/median ticket resolution time |
| Backlog | Number of tickets older than N days |
| Development effort | Hours required for a known task |
| Testing | Test coverage before vs. after |
| Quality | Validation and testing added |
| Documentation | Documentation coverage/quality |
| Delivery capacity | Work completed per team/member |
| Security | Secrets/passwords/PII kept out of repositories |

The key point made during the meeting was that the baseline needs to be captured **before deployment**.

Otherwise there will be no credible pre/post comparison.

---

# 26. Immediate next actions discussed

### Architecture owner / AI initiative

- Complete the detailed architecture.
- Answer the unresolved security and compliance questions.
- Determine Azure model hosting and data-handling characteristics.
- Document retention and telemetry behavior.
- Define RBAC and API access.
- Determine private endpoint/enclave requirements.
- Identify the model or model family for the pilot.
- Present the architecture and risk documentation to security.
- Continue working with Paul Johnson toward approval.
- Produce a polished version of the pilot material once the architecture is sufficiently defined.

### Data / analytics team

- Begin identifying potential pilot use cases.
- Establish baseline productivity measurements now.
- Pull historical Jira information if available.
- Measure delivery time for representative tasks.
- Record current manual effort for candidate automation use cases.
- Identify processes that could demonstrate meaningful improvement during the pilot.
- Identify current testing/documentation gaps that an AI coding assistant could potentially address.

### Arcadia / observability

- Investigate whether Arcadia's administrative metadata/feed-monitoring capability exists.
- Determine whether Becky or the Arcadia administrative team can provide access.
- Clarify what information is available beyond the Redshift Origin schema.
- Continue developing usage/adoption reporting for analytical products.

---

# Key takeaways

1. **The proposed AI pilot is primarily an AI-assisted development environment, not an autonomous production AI platform.**

2. **Azure AI Foundry is the proposed inference layer.** The intent is to use existing Azure infrastructure rather than introduce a large new vendor stack.

3. **VS Code is the proposed developer interface**, connected to an Azure-hosted model through an extension or agent harness.

4. **Two access paths are being considered** because the technology environments are not fully integrated: direct VPN access for users already connected to the Invista environment, and Azure Bastion + managed VM access for others.

5. **Production-system connectivity is intentionally excluded from the pilot.** The AI would assist with repositories, code, documentation, and local/synthetic testing rather than directly modifying production.

6. **Security approval is the main gating factor.** Model retention, telemetry, third-party debugging, training, RBAC, networking, and data-handling policies must be documented before deployment.

7. **Managed Azure-hosted models are preferred over locally hosted open-source models** because they reduce infrastructure and compliance complexity.

8. **Per-user metering is expected.** A rough model budget of approximately $20–$30 per user per month was discussed as an example.

9. **The team needs baseline metrics now.** Jira cycle time, development effort, test coverage, documentation, throughput, and similar measurements are needed to demonstrate whether the pilot provides measurable value.

10. **The meeting also identified a related need for stronger data observability**, particularly around source identification, refresh health, feed failures, ownership, and downstream dependency impact.

---

## Leadership-ready update

The team is evaluating a controlled enterprise AI development capability using Azure AI Foundry and existing Azure infrastructure. The proposed pilot
