For **Audit the Auditor**, I would not host one giant model. I would build a small local model stack where each model has a specific job.

## Recommended local stack

| Role                     | Model                             | Why I like it                                                              | Priority           |
| ------------------------ | --------------------------------- | -------------------------------------------------------------------------- | ------------------ |
| **Primary chart/VLM**    | **Qwen/Qwen3.5-9B**               | Best balance of document understanding, OCR, multimodal reasoning and size | **#1**             |
| **Document/OCR parser**  | **PaddlePaddle/PaddleOCR-VL-1.6** | Very small 1B model, strong structured document parsing                    | **#2**             |
| **Medical validator**    | **google/medgemma-1.5-4b-it**     | Medical/EHR/domain understanding                                           | **#3 / benchmark** |
| **Text RAG embedding**   | **Qwen/Qwen3-Embedding-0.6B**     | Tiny, strong, 32K context, instruction-aware                               | **#4**             |
| **Text reranking**       | **Qwen/Qwen3-Reranker-0.6B**      | Cheap second-stage retrieval accuracy boost                                | **#5**             |
| **Visual RAG embedding** | **Qwen/Qwen3-VL-Embedding-2B**    | Index actual chart-page images                                             | Phase 2            |
| **Visual RAG reranker**  | **Qwen/Qwen3-VL-Reranker-2B**     | Reranks chart images/pages against clinical questions                      | Phase 2            |

### My primary choice: Qwen3.5-9B

This is where I would start.

Qwen3.5-9B is a **9B multimodal model**, Apache 2.0 licensed, has native image + text input, a native 262K context window, and is directly supported by Transformers, vLLM and SGLang for local serving. The BF16 Hugging Face repository is about **19.3 GB**. ([Hugging Face][1])

More importantly for your project, Qwen reports particularly strong document results:

* OmniDocBench 1.5: **87.7**
* MMLongBench-Doc: **57.7**
* CC-OCR: **79.3**
* OCRBench: **89.2**

It actually scores above the much larger Qwen3-VL-30B-A3B on several of those document/OCR evaluations. ([Hugging Face][1])

That makes this a very interesting model for:

```text
Scanned medical chart
        ↓
identify relevant clinical sections
        ↓
read typed / semi-structured content
        ↓
understand chart context
        ↓
extract diagnosis evidence
        ↓
produce structured JSON
        ↓
explain why evidence may support HCC candidate
```

### I would start with this output schema

```json
{
  "clinical_concept": "Type 2 diabetes with diabetic CKD",
  "documented_diagnosis": "...",
  "evidence_text": "...",
  "page_number": 7,
  "evidence_type": "assessment_plan",
  "source_type": "handwritten_note",
  "encounter_date": "2026-04-12",
  "provider": "...",
  "icd10_candidate": "...",
  "confidence": 0.91
}
```

That is much better than prompting it simply with *"Which HCCs are in this chart?"*

---

# 2. Use PaddleOCR-VL-1.6 in front of it

This is a surprisingly attractive model for your architecture.

**PaddleOCR-VL-1.6** was released in June 2026. It is only about **1B parameters / 1.93 GB**, is Apache 2.0 licensed, and handles document parsing, text, tables, formulas, charts and structured output. Paddle reports 96.33% on OmniDocBench v1.6. ([Hugging Face][2])

I would therefore use:

```text
PDF / Chart
    ↓
PaddleOCR-VL-1.6
    ↓
layout + text + tables
    ↓
Qwen3.5-9B
    ↓
clinical reasoning + evidence extraction
```

That is far cheaper than having Qwen reason over every pixel of every page.

For example, a 50-page chart might contain:

* 15 administrative pages
* 10 labs
* 5 medication lists
* 15 notes
* 5 potentially relevant handwritten pages

PaddleOCR can do the cheap first pass. Qwen gets the pages worth thinking about.

---

# 3. MedGemma 1.5 4B deserves an A/B test

I **would not automatically make MedGemma the primary model**, even though this is healthcare.

But I would absolutely benchmark it.

Google's current **MedGemma 1.5 4B** explicitly supports:

* medical document understanding
* structured extraction from medical reports
* EHR understanding
* medical text reasoning
* multimodal medical data

It is only 4B and roughly 8.6 GB in its BF16 Hugging Face weights. It can also be served locally through Transformers/vLLM and has quantized versions available. ([Hugging Face][3])

The model is gated under Google's Health AI Developer Foundations terms rather than Apache/MIT, so CareAllies would need license/legal review before adopting it. ([Hugging Face][3])

### I would test:

```text
Same 200 chart pages

Qwen3.5-9B
     vs
MedGemma-1.5-4B
```

Measure:

| Metric                         | Qwen | MedGemma |
| ------------------------------ | ---: | -------: |
| Diagnosis recall               |      |          |
| Diagnosis precision            |      |          |
| Exact evidence extraction      |      |          |
| Handwriting accuracy           |      |          |
| Medical-context interpretation |      |          |
| Hallucination rate             |      |          |
| Structured JSON success        |      |          |
| Latency                        |      |          |
| GPU memory                     |      |          |

My hypothesis is:

**Qwen may win at reading the document. MedGemma may win at understanding some of the medicine.**

If that proves true, don't choose between them.

Use:

```text
Qwen3.5
   ↓
extract evidence
   ↓
MedGemma
   ↓
clinical validation
```

Only send difficult cases to MedGemma.

---

# 4. For CMS RAG, use a tiny model

You absolutely do **not** need an 8B embedding model for CMS guidance.

My starting choice would be:

### Qwen3-Embedding-0.6B

It is only **0.6B**, has a **32K context window**, supports up to 1,024-dimensional embeddings, is instruction-aware, and is Apache 2.0 licensed. ([Hugging Face][4])

Use it for:

```text
CMS-HCC specifications
ICD-10 mapping
CMS guidance
internal auditing standards
coding policy
CareAllies audit rules
```

Example instruction:

```text
Retrieve authoritative CMS or internal policy passages
that determine whether the documented clinical evidence
supports the proposed ICD-10/HCC classification.
```

Because the model is instruction-aware, Qwen reports that task-specific retrieval instructions typically improve downstream performance by roughly 1-5% in its evaluations. ([Hugging Face][5])

---

# 5. Add Qwen3-Reranker-0.6B

Embedding retrieval gives you perhaps:

```text
100,000 CMS/internal passages
           ↓
Top 30 candidates
```

Then:

```text
Qwen3-Reranker-0.6B
           ↓
Top 5
```

The reranker is only about **1.2 GB**, supports 32K context and is designed specifically for text ranking. ([Hugging Face][6])

So your RAG stack becomes:

```text
CMS / ICD / Internal Guidance
              ↓
       chunk + metadata
              ↓
 Qwen3-Embedding-0.6B
              ↓
        Vector search
              ↓
       Top 20 passages
              ↓
 Qwen3-Reranker-0.6B
              ↓
        Top 3-5 evidence
              ↓
         Qwen3.5-9B
```

This is a much better architecture than letting Qwen hallucinate CMS rules.

---

# 6. There is an even more interesting option for chart RAG

Since we last discussed ColQwen/ColPali, Qwen has released something I think is even more applicable here:

### Qwen3-VL-Embedding-2B

It embeds:

* images
* screenshots
* text
* combinations of image + text

into the same semantic space.

It's a **2B multimodal embedding model**, 32K context, Apache 2.0, with up to 2,048-dimensional embeddings. ([Hugging Face][7])

That means you could literally index **chart pages as images**.

Instead of:

```text
chart
↓
OCR
↓
text
↓
embedding
```

you can do:

```text
chart page image
↓
Qwen3-VL-Embedding
↓
visual embedding
```

Then ask:

> Find pages containing evidence of diabetic kidney disease.

The retrieval engine can return the original scanned pages.

That is extremely interesting for handwritten and poorly structured charts.

---

# 7. Pair it with Qwen3-VL-Reranker-2B

Qwen now also has **Qwen3-VL-Reranker-2B**.

It can rank:

```text
query ↔ text
query ↔ image
query ↔ image + text
```

The model is 2B with 32K context. Qwen reports material improvements over its base multimodal embedding model on visual document retrieval benchmarks such as ViDoRe v3. ([Hugging Face][8])

So eventually you could have:

```text
Query:
"Evidence supporting diabetic CKD"

                ↓

Qwen3-VL-Embedding-2B

                ↓

Top 30 chart pages

                ↓

Qwen3-VL-Reranker-2B

                ↓

Top 5 chart pages

                ↓

Qwen3.5-9B

                ↓

Extract exact evidence
```

That is a very modern local **Visual RAG** architecture.

---

# What I would actually build first

Do **not** start with seven models.

Start with three:

```text
┌───────────────────────────────┐
│ PaddleOCR-VL-1.6             │
│ Document parsing / OCR        │
│ ~1B                           │
└──────────────┬────────────────┘
               ↓
┌───────────────────────────────┐
│ Qwen3.5-9B                   │
│ Chart understanding           │
│ Clinical evidence extraction  │
│ Reasoning                     │
└──────────────┬────────────────┘
               ↓
┌───────────────────────────────┐
│ Qwen3-Embedding-0.6B         │
│ + Qwen3-Reranker-0.6B        │
│ CMS / policy RAG              │
└───────────────────────────────┘
```

Technically that's four models, but the two RAG models are tiny.

Then benchmark **MedGemma** against Qwen before deciding whether it deserves a permanent place.

---

## Hardware implications

Approximate practical sizing, not guaranteed VRAM requirements:

| GPU               | What I would run                                                   |
| ----------------- | ------------------------------------------------------------------ |
| **12-16 GB VRAM** | Qwen3.5-4B, PaddleOCR-VL, Qwen3 0.6B embedding/reranker            |
| **24 GB VRAM**    | **Qwen3.5-9B quantized**, PaddleOCR-VL, embedding/reranker         |
| **32-48 GB VRAM** | Qwen3.5-9B BF16 comfortably + supporting models                    |
| **48 GB+**        | Qwen3.5-9B + MedGemma + Visual RAG concurrently                    |
| **80 GB**         | Plenty of room for experimentation and larger models               |
| **Multi-GPU**     | Scale throughput, parallel page processing, larger VLM experiments |

Qwen's unquantized Qwen3.5-9B repository itself is 19.3 GB, so a 24 GB GPU is tight once KV cache, image processing and runtime overhead are included. ([Hugging Face][9])

For a 24 GB GPU, I would target a good **4-bit/AWQ/GPTQ quantization of Qwen3.5-9B** rather than BF16.

---

# One model I would also test for OCR

**GLM-OCR** is another very interesting 2026 option.

It is only **1B**, about **2.65 GB BF16**, MIT licensed, supports local Transformers/vLLM/SGLang serving, and the authors provide a complete document-parsing pipeline. ([Hugging Face][10])

So I would benchmark:

**PaddleOCR-VL-1.6 vs GLM-OCR**

using CareAllies-like scans.

And importantly, I'd create a dedicated **handwriting test set**. General OCR benchmarks will not tell us enough about physician handwriting.

---

# Models I would *not* prioritize initially

**Phi-4 Multimodal** is still attractive at ~6B with 128K context and MIT licensing, but for this particular project I think Qwen3.5 is now the stronger first VLM candidate given its current document-understanding results. ([Hugging Face][11])

**DeepSeek-OCR-2** is worth benchmarking later, but its BF16 weights are around 6.78 GB and require `trust_remote_code`; I would first evaluate the smaller and simpler PaddleOCR-VL/GLM-OCR choices. ([Hugging Face][12])

**Qwen3.6-27B** is powerful and locally deployable, but its repository is about 55.6 GB BF16. I don't think you need that compute expense to prove this use case. ([Hugging Face][13])

---

## My shortlist for CareAllies

If I were writing the technical proposal today:

**Primary model:** `Qwen/Qwen3.5-9B`

**Document ingestion:** `PaddlePaddle/PaddleOCR-VL-1.6`

**Medical challenger:** `google/medgemma-1.5-4b-it`

**CMS RAG embedding:** `Qwen/Qwen3-Embedding-0.6B`

**CMS RAG reranker:** `Qwen/Qwen3-Reranker-0.6B`

**Phase 2 visual chart retrieval:** `Qwen/Qwen3-VL-Embedding-2B` + `Qwen/Qwen3-VL-Reranker-2B`

That gives you a surprisingly sophisticated **fully self-hosted Audit-the-Auditor AI stack** without needing a 70B+ model.

The next thing I would do is design the **local POC architecture and exact hardware recommendation**, including what fits on **1× RTX 4090/5090, L40S, A100 or H100**, and show how the models, vector database, CMS RAG and Arcadia chart pipeline would connect.

[1]: https://huggingface.co/Qwen/Qwen3.5-9B "Qwen/Qwen3.5-9B · Hugging Face"
[2]: https://huggingface.co/PaddlePaddle/PaddleOCR-VL-1.6/tree/main?utm_source=chatgpt.com "PaddlePaddle/PaddleOCR-VL-1.6 at main"
[3]: https://huggingface.co/google/medgemma-1.5-4b-it "google/medgemma-1.5-4b-it · Hugging Face"
[4]: https://huggingface.co/Qwen/Qwen3-Embedding-0.6B/tree/main?utm_source=chatgpt.com "Qwen/Qwen3-Embedding-0.6B at main"
[5]: https://huggingface.co/Qwen/Qwen3-Embedding-0.6B?utm_source=chatgpt.com "Qwen/Qwen3-Embedding-0.6B · Hugging Face"
[6]: https://huggingface.co/Qwen/Qwen3-Reranker-0.6B/tree/main?utm_source=chatgpt.com "Qwen/Qwen3-Reranker-0.6B at main"
[7]: https://huggingface.co/Qwen/Qwen3-VL-Embedding-2B?utm_source=chatgpt.com "Qwen/Qwen3-VL-Embedding-2B · Hugging Face"
[8]: https://huggingface.co/Qwen/Qwen3-VL-Reranker-2B "Qwen/Qwen3-VL-Reranker-2B · Hugging Face"
[9]: https://huggingface.co/Qwen/Qwen3.5-9B/tree/main?utm_source=chatgpt.com "Qwen/Qwen3.5-9B at main"
[10]: https://huggingface.co/zai-org/GLM-OCR?utm_source=chatgpt.com "zai-org/GLM-OCR · Hugging Face"
[11]: https://huggingface.co/microsoft/Phi-4-multimodal-instruct?utm_source=chatgpt.com "microsoft/Phi-4-multimodal-instruct · Hugging Face"
[12]: https://huggingface.co/deepseek-ai/DeepSeek-OCR-2?utm_source=chatgpt.com "deepseek-ai/DeepSeek-OCR-2 · Hugging Face"
[13]: https://huggingface.co/Qwen/Qwen3.6-27B/tree/main?utm_source=chatgpt.com "Qwen/Qwen3.6-27B at main"

Yes. I would **not buy GPUs for this POC**. The architecture should treat cloud GPU capacity as infrastructure that you spin up when needed, while the models themselves remain self-hosted inside the approved CareAllies/HCSC cloud boundary.

## Proposed cloud-hosted POC architecture

```text
                           CAREALLIES / HCSC PRIVATE CLOUD BOUNDARY
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  ┌───────────────────────┐                                                  │
│  │       ARCADIA         │                                                  │
│  │                       │                                                  │
│  │ • Medical charts      │                                                  │
│  │ • Auditor decisions   │                                                  │
│  │ • Encounter metadata  │                                                  │
│  └───────────┬───────────┘                                                  │
│              │                                                              │
│              │ Approved API / Export / File Transfer                        │
│              ▼                                                              │
│  ┌──────────────────────────────┐                                            │
│  │ Secure Cloud Landing Zone    │                                            │
│  │                              │                                            │
│  │ Original PDFs / Images       │                                            │
│  │ Auditor results              │                                            │
│  │ Encounter metadata           │                                            │
│  └──────────────┬───────────────┘                                            │
│                 │                                                           │
│                 ▼                                                           │
│  ┌──────────────────────────────┐                                            │
│  │ Document Processing Pipeline │                                            │
│  │                              │                                            │
│  │ 1. Split PDF into pages      │                                            │
│  │ 2. Detect page type          │                                            │
│  │ 3. OCR / document parsing    │                                            │
│  │ 4. Extract metadata          │                                            │
│  └──────────────┬───────────────┘                                            │
│                 │                                                           │
│                 ▼                                                           │
│  ┌──────────────────────────────┐                                            │
│  │ Cloud GPU Model Endpoint     │                                            │
│  │                              │                                            │
│  │ Qwen / MedGemma / etc.       │                                            │
│  │ Self-hosted model container  │                                            │
│  │                              │                                            │
│  │ • Clinical concepts          │                                            │
│  │ • Evidence                   │                                            │
│  │ • Page references            │                                            │
│  │ • Confidence                 │                                            │
│  └──────────────┬───────────────┘                                            │
│                 │                                                           │
│                 ▼                                                           │
│        STRUCTURED CHART EVIDENCE                                             │
│                 │                                                           │
│        ┌────────┴───────────┐                                                │
│        │                    │                                                │
│        ▼                    ▼                                                │
│ ┌───────────────┐   ┌──────────────────┐                                    │
│ │ Chart Evidence│   │ CMS / Policy RAG │                                    │
│ │ Retrieval     │   │                  │                                    │
│ └───────┬───────┘   └────────┬─────────┘                                    │
│         │                    │                                              │
│         └─────────┬──────────┘                                              │
│                   ▼                                                         │
│       ┌───────────────────────────┐                                         │
│       │ Retrieval + Reranking     │                                         │
│       │                           │                                         │
│       │ Relevant clinical evidence│                                         │
│       │ + applicable CMS guidance │                                         │
│       └─────────────┬─────────────┘                                         │
│                     │                                                       │
│                     ▼                                                       │
│       ┌───────────────────────────┐                                         │
│       │ AI Evidence Reasoning     │                                         │
│       │                           │                                         │
│       │ "What does the evidence  │                                         │
│       │ appear to support?"       │                                         │
│       └─────────────┬─────────────┘                                         │
│                     │                                                       │
│                     ▼                                                       │
│       ┌───────────────────────────┐                                         │
│       │ Deterministic HCC Engine  │                                         │
│       │                           │                                         │
│       │ ICD mappings              │                                         │
│       │ HCC hierarchy             │                                         │
│       │ Payment-year rules        │                                         │
│       └─────────────┬─────────────┘                                         │
│                     │                                                       │
│                     ▼                                                       │
│       ┌───────────────────────────┐                                         │
│       │ AUDITOR RECONCILIATION    │                                         │
│       │                           │                                         │
│       │ AI evidence               │                                         │
│       │       vs                  │                                         │
│       │ Auditor decision          │                                         │
│       └─────────────┬─────────────┘                                         │
│                     │                                                       │
│                     ▼                                                       │
│       ┌───────────────────────────┐                                         │
│       │ Exception / Scoring Layer │                                         │
│       │                           │                                         │
│       │ HCC match %               │                                         │
│       │ Evidence confidence       │                                         │
│       │ Potential missed HCC      │                                         │
│       │ Unsupported HCC           │                                         │
│       │ Needs review              │                                         │
│       └─────────────┬─────────────┘                                         │
│                     │                                                       │
│                     ▼                                                       │
│               HUMAN REVIEW                                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 1. Arcadia is the starting point, not the AI platform

Arcadia would provide three primary things:

**Charts**

* PDFs
* scanned images
* potentially handwritten pages

**Auditor results**

* diagnoses found
* ICD codes
* HCCs
* accepted/rejected decisions

**Encounter metadata**

* member
* DOS
* provider
* encounter type
* chart identifiers

The exact interface could eventually be an API, approved export, batch transfer, or another integration mechanism. For the POC, I would **not make Arcadia integration itself the project**.

If necessary, start with an approved batch export such as:

```text
/charts/
   chart_001.pdf
   chart_002.pdf

auditor_results.csv

encounters.csv
```

Prove the intelligence first.

---

# 2. Put the files into private cloud object storage

Think:

```text
Arcadia
   ↓
Secure ingestion
   ↓
Cloud object storage
```

Examples:

| Vendor-neutral  | Azure         | AWS             |
| --------------- | ------------- | --------------- |
| Object storage  | Blob Storage  | S3              |
| Private network | VNet          | VPC             |
| Secrets         | Key Vault     | Secrets Manager |
| Encryption keys | Key Vault/HSM | KMS             |

The important thing is **PHI never needs to leave the enterprise cloud boundary**.

---

# 3. Cloud GPU becomes the model server

This is where I would slightly change the terminology from "local model."

Instead call it:

> **Privately hosted open-weight model**

For example:

```text
Private cloud GPU compute

        ↓

Docker container

        ↓

vLLM / SGLang / Transformers

        ↓

Qwen3.5-9B
```

Your application calls it through an internal REST endpoint:

```text
POST /extract-clinical-evidence
```

The GPU could be:

* cloud VM
* managed Kubernetes GPU node
* managed container endpoint
* internal inference service

You don't particularly care which at the POC stage.

The architectural rule is simply:

> **Model weights live inside your environment and inference runs inside your environment.**

No ChatGPT/OpenAI/Anthropic/external API has to receive the chart.

---

# 4. OCR and multimodal LLM should be separate services

This becomes:

```text
PDF
 │
 ├── Page 1 → clean digital text
 │
 ├── Page 2 → scanned note
 │
 ├── Page 3 → handwriting
 │
 ├── Page 4 → lab table
 │
 └── Page 5 → administrative
```

First service:

### Document Intelligence

```text
PaddleOCR-VL
or
GLM-OCR
or approved cloud OCR
```

Produces:

```json
{
  "page": 7,
  "page_type": "progress_note",
  "source_type": "handwritten",
  "text": "...",
  "ocr_confidence": 0.83
}
```

Then the harder pages go to:

### Multimodal reasoning model

```text
Qwen3.5
       ↓
Clinical evidence extraction
```

This keeps GPU usage much more reasonable.

---

# 5. CMS RAG should be a completely separate pipeline

This is important architecturally.

You have another ingestion pipeline:

```text
CMS
 │
 ├── HCC specifications
 ├── ICD-10 mappings
 ├── model documentation
 ├── coding guidance
 └── applicable rules

Internal sources
 │
 ├── auditor guidelines
 ├── coding standards
 └── CareAllies procedures
```

Then:

```text
Documents
    ↓
Parse
    ↓
Chunk
    ↓
Attach metadata
    ↓
Embedding model
    ↓
VECTOR DATABASE
```

Metadata should include things such as:

```text
document_type
source
effective_year
payment_year
model_version
publication_date
section
page
rule_type
```

This matters because you don't want:

> 2024 guidance

being retrieved when evaluating:

> a 2026 encounter.

---

# 6. The CMS vector database is persistent

I would make this one of the central POC components.

```text
CMS documents
      ↓
Qwen3-Embedding-0.6B
      ↓
Vector DB
```

For example:

```text
CMS_RAG_INDEX
─────────────────────────────
CMS V28 specification
2026 ICD mappings
coding guidance
audit requirements
CareAllies policy
```

The application asks:

> What rules apply to this ICD/HCC candidate for payment year 2026?

Retrieval returns perhaps 20 passages.

Then:

```text
Qwen3-Reranker
      ↓
Top 3-5 authoritative passages
```

Those passages accompany the chart evidence into the reasoning model.

---

# 7. I would treat patient-chart RAG differently

This is one architecture decision I'd make early.

### CMS RAG

**Persistent**

No member PHI involved.

### Patient chart RAG

Preferably **temporary or case-scoped** during the POC.

Instead of putting every patient's entire medical history into one massive vector database:

```text
Chart 12345
      ↓
Temporary index
      ↓
Retrieve relevant pages
      ↓
Complete review
      ↓
Expire according to retention policy
```

That gives you better isolation.

Conceptually:

```text
Vector database
│
├── CMS_INDEX
│     Persistent
│
├── INTERNAL_POLICY_INDEX
│     Persistent
│
└── CHART_SESSION_938271
      Temporary / isolated
```

That also reduces the possibility of accidentally retrieving **Patient B's information while evaluating Patient A**.

For a healthcare system, that separation is valuable.

---

# 8. Then combine the two retrieval paths

Suppose the system is investigating:

> Possible diabetic CKD HCC

### Clinical retrieval asks:

```text
Find evidence concerning:

diabetes
CKD
renal disease
assessment
treatment
monitoring
```

Returns:

```text
Page 7
Page 19
Page 31
```

### CMS retrieval asks:

```text
Retrieve applicable guidance for:

ICD candidate
HCC mapping
payment year
documentation requirements
```

Returns:

```text
CMS passage A
CMS mapping B
internal audit rule C
```

Now the model sees:

```text
QUESTION

+ Patient evidence

+ CMS evidence

+ Internal policy

+ Encounter metadata
```

This is what the LLM should reason over.

---

# 9. Then use deterministic logic

This component should **not be an LLM**.

Something like Python/SQL:

```python
icd_candidate
     ↓
CMS mapping table
     ↓
HCC
     ↓
hierarchy / exclusion logic
     ↓
valid candidate
```

The model proposes:

> ICD candidate X appears supported.

Your deterministic engine determines:

> ICD X maps to HCC Y under model Z.

That separation is important.

### LLM

Understands messy clinical documentation.

### RAG

Provides authoritative knowledge.

### Rules engine

Performs exact mappings.

Much safer than expecting the language model to remember everything.

---

# 10. Finally compare against Arcadia's auditor decision

You now have:

```text
AI determination
        +
Auditor determination
```

Example:

| HCC   | AI          | Auditor        | Result     |
| ----- | ----------- | -------------- | ---------- |
| HCC A | Supported   | Accepted       | Match      |
| HCC B | Supported   | Not identified | **Review** |
| HCC C | Unsupported | Accepted       | **Review** |
| HCC D | Unsupported | Rejected       | Match      |

And produce:

```json
{
  "hcc": "HCC B",
  "auditor_decision": "not captured",
  "ai_evidence_status": "supported",
  "evidence_confidence": 0.94,
  "cms_rule_confidence": 0.98,
  "match": false,
  "needs_review": true
}
```

---

# 11. Human review closes the loop

The reviewer sees something closer to:

### AI Review Exception

**Potential missed HCC**

**Auditor**
Not captured

**AI evidence**
High confidence

**Chart evidence**

* Page 7: Assessment
* Page 19: Treatment
* Page 31: Monitoring

**Applicable rule**

* CMS source
* internal guideline

**Recommended action**
Secondary review

Then:

```text
Agree
Disagree
Insufficient evidence
```

Save that outcome.

That dataset becomes enormously important later:

```text
Chart
+
Auditor answer
+
AI answer
+
Senior reviewer final answer
```

That's your future fine-tuning/evaluation dataset.

---

# 12. POC cloud components

I would keep the POC intentionally small:

| Component            | Purpose                  |
| -------------------- | ------------------------ |
| Cloud object storage | PDFs and inputs          |
| CPU container        | PDF processing           |
| Cloud GPU endpoint   | Qwen / MedGemma          |
| OCR service          | Document extraction      |
| Embedding service    | CMS/chart vectors        |
| Vector DB            | CMS + evidence retrieval |
| Python API           | orchestration            |
| Python rules engine  | ICD/HCC mappings         |
| SQL/database         | structured results       |
| Simple dashboard     | human review             |
| Audit log            | complete lineage         |

You **do not need** Kubernetes, microservices, Kafka, 14 databases and an enterprise agent platform to prove this works.

For the POC, a simpler architecture is better.

---

# What I would actually deploy first

```text
                    PRIVATE CLOUD
                         │
         ┌───────────────┴───────────────┐
         │                               │
       Arcadia                         CMS
         │                               │
         ▼                               ▼
   Object Storage                  CMS Documents
         │                               │
         ▼                               ▼
    OCR Service                    Embedding Model
         │                               │
         ▼                               ▼
 Multimodal LLM                  Vector Database
         │                               │
         └──────────────┬────────────────┘
                        ▼
                 Reasoning Service
                        │
                        ▼
                 HCC Rules Engine
                        │
                        ▼
                Compare to Auditor
                        │
                        ▼
                  Review Queue
                        │
                        ▼
                 Human Decision
```

That is the **POC architecture I would take to the team**.

## And the key architectural principle

I would explicitly call out four different kinds of intelligence:

**1. Perception**

> OCR/VLM reads the chart.

**2. Retrieval**

> RAG finds relevant chart evidence and authoritative CMS guidance.

**3. Reasoning**

> LLM determines whether the evidence appears clinically relevant.

**4. Deterministic validation**

> Code applies exact ICD/HCC mappings and compares against the auditor.

That separation makes Audit the Auditor much more credible than:

> *"We're going to upload medical charts into an LLM and ask it what HCCs it sees."*

It also gives you modularity. If Qwen gets surpassed six months from now, you can replace the **model service** without rebuilding Arcadia ingestion, CMS RAG, the vector store, the rules engine, or the human-review workflow. That is the architecture I would optimize for.
