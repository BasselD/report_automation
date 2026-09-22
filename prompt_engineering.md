Two-Stage Prompt Framework: CMS Stars Measure Specifications

The objective is to process a lengthy, 70-page Stars specification document in two stages. First, establish a comprehensive understanding of the document. Second, extract detailed, actionable specifications for individual measures.

This approach is particularly useful for CMS Stars documentation, where measure definitions, eligibility criteria, exclusions, calculation methodologies, and reporting requirements may be distributed across multiple sections.

Stage 1: Document ingestion and knowledge extraction

PROMPT 1 · DOCUMENT-LEVEL ANALYSIS

Upload the complete 70-page document before running this prompt.

Act as a senior healthcare quality analytics specialist with expertise in CMS Star Ratings, HEDIS, Medicare Advantage, and Part D quality measures.

I am providing a 70-page document containing Stars measure specifications.

Your task is to thoroughly process the ENTIRE document and establish a structured understanding of its contents before performing any measure-specific analysis.

Instructions:

1. Review all pages, including tables, appendices, footnotes, definitions, and technical specifications.
2. Identify every quality measure referenced in the document, including its measure ID, official name, measurement year, and applicable population when available.
3. Identify the document’s overall structure and locate the sections containing measure definitions, eligibility criteria, numerator and denominator logic, exclusions, calculation methodologies, and reporting requirements.
4. Identify general rules that apply across multiple measures, such as enrollment requirements, continuous enrollment, data sources, measurement periods, and special population considerations.
5. Identify any measure-specific exceptions, cross-references, or technical notes that could affect implementation.
6. Distinguish official measure specifications from explanatory examples, recommendations, and supplemental commentary.
7. Preserve the document’s exact numerical thresholds, dates, age ranges, and technical terminology. Do not substitute assumptions or external knowledge for the specifications provided.
8. If any pages, tables, or sections are inaccessible, incomplete, or ambiguous, explicitly identify them rather than assuming their contents.

Required output:

A. Document overview: Title, publication date, measurement year, applicable programs, and overall scope.

B. Measure inventory: A table listing every measure, its identifier, official name, category, and relevant page numbers.

C. General specifications: A structured summary of rules that apply across measures.

D. Document navigation: A map of the sections and page ranges containing detailed specifications for each measure.

E. Implementation considerations: Important technical definitions, exceptions, dependencies, and potential ambiguities.

Critical requirements:

* Cite the relevant document page number for every extracted rule or specification.
* Do not omit measures or combine distinct measures.
* Do not invent missing definitions or technical requirements.
* If the document is too large to process completely, identify the unprocessed sections and continue systematically rather than claiming complete coverage.

Do not produce detailed measure-by-measure summaries yet.

Once document processing is complete, confirm which measures are available for detailed analysis in Stage 2.

Stage 2: Measure-specific specification extraction

Once the document has been processed, use the following prompt to generate detailed, implementation-ready specifications for an individual measure or a selected group of measures.

PROMPT 2 · MEASURE-LEVEL ANALYSIS

Run this prompt in the same conversation after Stage 1.

Using the Stars specification document reviewed in Stage 1, provide a comprehensive, structured analysis of the following measure:

Measure name or ID: [INSERT MEASURE]

Act as a senior healthcare quality analytics specialist responsible for translating official measure specifications into actionable business and technical requirements.

Review ALL relevant sections of the document, including general specifications, measure-specific requirements, tables, footnotes, appendices, and cross-references.

Present your findings in the following bulleted structure.

1. Measure overview

* Official measure name and ID.
* Measurement year and reporting period.
* Purpose and clinical or operational significance.
* Applicable population and program.
* Relevant document page references.

2. Eligible population and denominator

* Age and demographic requirements.
* Enrollment and continuous enrollment criteria.
* Qualifying diagnoses, services, medications, or clinical events.
* Required observation or treatment periods.
* Denominator inclusion logic.
* Special population considerations.

3. Numerator and compliance criteria

* Exact conditions required for numerator compliance.
* Applicable clinical, pharmacy, or administrative events.
* Required timing, frequency, or duration.
* Specific numerical thresholds and calculation rules.
* Conditions under which a member is considered compliant.

4. Exclusions and exceptions

* Required exclusions.
* Allowable exclusions and exceptions, if applicable.
* Applicable diagnoses, procedures, medications, or other qualifying conditions.
* Timing requirements for each exclusion.
* Whether an exclusion removes a member from the denominator or affects another component of the calculation.

5. Calculation methodology

* Official measure formula.
* Numerator and denominator definitions.
* Measurement windows and lookback periods.
* Applicable thresholds.
* Rounding rules and handling of missing data.
* Special calculation rules, if specified.

6. Data and technical requirements

* Required source data, such as medical claims, pharmacy claims, enrollment, or clinical records.
* Applicable diagnosis, procedure, medication, or value-set references.
* Relevant dates and data elements.
* Data dependencies and cross-references.

7. Implementation considerations

* Recommended sequence of operations for calculating the measure.
* Potential edge cases and exceptions.
* Important technical nuances that could cause incorrect calculations.
* Items requiring additional clarification before implementation.

Output requirements:

* Use concise but comprehensive bullet points under each heading.
* Include the exact page number supporting each important specification.
* Preserve all numerical thresholds, eligibility conditions, and timing requirements.
* Clearly distinguish official specifications from your own implementation recommendations.
* Do not introduce requirements that are not supported by the document.
* If a specification is not explicitly provided, state “Not specified in the provided document.”
* Do not omit critical information for the sake of brevity.

The final output should be sufficiently detailed for a healthcare analytics team to translate the specifications into SQL or Python calculation logic and develop appropriate validation rules.

Recommended workflow

Upload the 70-page document

Provide the complete specification document, including all tables and appendices.

Execute Prompt 1

Establish document coverage, build the measure inventory, and identify shared specification rules.

Execute Prompt 2

Extract detailed specifications for each selected measure, one at a time.

Important: Processing a document in Stage 1 does not guarantee that every detail will remain accessible for Stage 2, especially in a long conversation. Keep the original document attached and require the AI to revisit the relevant source pages for each measure. This reduces the risk of missing requirements or generating unsupported specifications.
---
Advanced Prompt Engineering: Questions to Demonstrate Depth

Using your 70-page CMS Stars specifications document, you can demonstrate that prompt engineering goes far beyond writing detailed instructions. The goal is to show how AI can perform structured analysis, identify gaps, reason across multiple sources, challenge its own conclusions, and generate implementation-ready outputs.

The following questions build upon your original two-stage workflow and introduce more advanced techniques.

1. Cross-document reasoning and dependency mapping

Technique: Contextual reasoning and knowledge integration

Question: Can ChatGPT connect information scattered across different sections of a lengthy document?

Identify all requirements for the [MEASURE NAME] measure that are defined outside its primary specification section.

Examine general eligibility rules, enrollment requirements, appendices, technical notes, footnotes, and cross-references.

For each requirement, provide:

* The requirement and its impact on the measure.
* The page where it is defined.
* Whether it applies universally or only under specific conditions.
* What could go wrong if this requirement were overlooked during implementation.

Do not limit your analysis to the measure’s primary section.

Why this is impressive: It demonstrates that AI can connect information distributed throughout a document rather than simply summarize adjacent paragraphs.

2. Compare two measures and identify critical differences

Technique: Comparative reasoning and structured extraction

Question: Can ChatGPT distinguish subtle differences between two related quality measures?

Compare the following CMS Stars measures:

Measure A: [MEASURE A]

Measure B: [MEASURE B]

Create a side-by-side comparison covering:

* Eligible population and age requirements.
* Numerator and denominator definitions.
* Enrollment criteria.
* Exclusions and exceptions.
* Measurement and lookback periods.
* Calculation methodologies.
* Required data sources.

Then identify the five most important differences that could lead to incorrect calculations if the same implementation logic were reused for both measures.

Cite the source document for each comparison and clearly identify any requirements that are not specified.

Why this is impressive: The model must perform both extraction and comparison while preserving important differences between measures.

3. Challenge the model’s initial answer

Technique: Self-critique and iterative refinement

Question: What happens when we ask AI to critically evaluate its own previous response?

Critically review your previous measure analysis as if you were an independent quality assurance analyst.

Do not assume your previous interpretation was correct.

Identify:

1. Requirements that may have been omitted.
2. Statements that lack direct supporting evidence.
3. Potentially incorrect interpretations of eligibility or exclusion criteria.
4. Ambiguous wording that could result in multiple implementations.
5. Any conclusions that rely on general CMS knowledge rather than the supplied document.

Return a table with the original statement, identified issue, supporting source reference, proposed correction, and verification status.

Revisit the original source document before making corrections.

Why this is impressive: The audience can see that an AI-generated answer can become an input for a second quality-control process.

It also demonstrates an important limitation: AI self-review is useful but is not a substitute for independent validation.

4. Convert specifications into executable business logic

Technique: Specification-to-logic transformation

Question: Can the model translate complex regulatory language into a sequence of actionable operations?

Using the official specifications for [MEASURE NAME], translate the measure requirements into an implementation-ready decision framework.

Do not write SQL or Python code yet.

Provide the following:

1. Required input data and fields.
2. Sequential steps to identify the eligible population.
3. Logic for applying exclusions and exceptions.
4. Conditions required for numerator compliance.
5. Calculation methodology and applicable thresholds.
6. Final member-level classification logic.
7. Required validation checks.

Represent the calculation workflow as a decision tree using IF/THEN statements.

For each decision, cite the relevant specification page and clearly identify any assumptions or unresolved requirements.

Why this is impressive: You demonstrate that an LLM can help bridge the gap between business requirements and technical implementation.

5. Generate edge cases and validation scenarios

Technique: Scenario generation and systematic testing

Question: Can AI anticipate situations where otherwise reasonable calculation logic might fail?

Act as a healthcare data quality and testing specialist.

Using the official specifications for [MEASURE NAME], identify edge cases that should be tested before deploying the calculation logic into production.

Consider scenarios involving:

* Members entering or leaving the eligible population.
* Enrollment gaps or partial measurement periods.
* Overlapping clinical or pharmacy events.
* Exclusions occurring at different points in the measurement period.
* Missing or conflicting source data.
* Exact numerical thresholds and boundary conditions.

For each scenario, provide the relevant member characteristics, test conditions, expected outcome, supporting specification reference, and reason the scenario matters.

Generate at least 10 test cases.

Distinguish source-supported expected outcomes from scenarios that require business clarification. Do not invent eligibility or calculation rules.

Why this is impressive: This demonstrates that AI can support quality assurance and generate test scenarios, not simply produce descriptive summaries.

6. Introduce a confidence and evidence framework

Technique: Evidence-grounded reasoning and uncertainty management

Question: Can the model distinguish between directly documented requirements, interpretations, and missing information?

Review the extracted specifications for [MEASURE NAME].

Classify every important requirement into one of the following evidence categories:

A. Explicitly documented: The source directly states the requirement.

B. Derived: The requirement follows logically from multiple documented statements.

C. Ambiguous: The document supports more than one reasonable interpretation.

D. Missing: The information is required for implementation but is not provided.

Create a table containing the requirement, evidence category, supporting source reference, explanation, and recommended next action.

Do not assign numerical confidence scores. Do not treat an inferred requirement as an official specification.

Prioritize ambiguous and missing requirements that could materially affect calculation accuracy.

Why this is impressive: It shows how prompt engineering can make uncertainty visible, rather than encouraging the model to generate an answer for every question.

7. Build an interactive expert interview

Technique: Role prompting and adaptive questioning

Question: Can AI identify what information is missing and ask targeted questions before proposing a solution?

Act as a senior healthcare analytics solution architect.

Your objective is to determine whether our organization has the information and data necessary to implement [MEASURE NAME].

Using the official specifications, interview me about our available data sources, enrollment records, claims, pharmacy data, calculation methodology, and reporting infrastructure.

Ask one focused question at a time.

After each response:

* Identify which implementation requirements have been satisfied.
* Explain what important information is still missing.
* Adapt your next question based on my answer.

Do not propose a final implementation architecture until you have collected sufficient information or explicitly identified the remaining gaps.

At the end, produce a requirements checklist, a data-readiness assessment, and a list of unresolved implementation decisions.

Why this is impressive: Rather than treating prompting as a one-directional question-and-answer process, you turn ChatGPT into an adaptive requirements-gathering assistant.

This can be especially effective in a live demonstration because the audience sees how each answer changes the next question.

8. Generate different outputs for different stakeholders

Technique: Audience adaptation and controlled output transformation

Question: Can the same information be transformed for executives, clinicians, and technical analysts without changing the underlying facts?

Using the verified specifications for [MEASURE NAME], prepare three versions of the same measure explanation.

Version 1. Executive audience:
Explain the measure’s purpose, business significance, population, and major operational considerations in no more than 150 words.

Version 2. Clinical audience:
Explain the eligible population, qualifying clinical events, compliance requirements, and exclusions using clinically appropriate terminology.

Version 3. Data analytics audience:
Provide the numerator, denominator, eligibility criteria, calculation requirements, data dependencies, and implementation considerations in a structured technical format.

Ensure all three versions remain consistent with the same source specifications.

Do not introduce facts, requirements, or assumptions that are absent from the verified analysis.

Conclude by identifying which information is common across all three versions and which details were adapted to suit each audience.

Why this is impressive: It shows the difference between changing how information is communicated and changing the information itself.

9. Identify changes between specification years

Technique: Longitudinal document comparison and change-impact analysis

Question: Can AI identify changes between two annual specification documents and explain their technical consequences?

Compare the attached CMS Stars specification documents for [YEAR A] and [YEAR B].

Focus on [MEASURE NAME].

Identify every documented change affecting:

* Measure definition and eligible population.
* Numerator and denominator.
* Exclusions and exceptions.
* Calculation methodology.
* Measurement periods and thresholds.
* Required data sources and reporting requirements.

Create a change-impact matrix containing:

Previous requirement | Updated requirement | Type of change | Source references | Potential implementation impact

Classify each change as added, removed, modified, or unchanged.

For every change, explain whether the existing calculation logic, data pipeline, or validation process may require modification.

Do not classify a requirement as unchanged unless the relevant sections of both documents have been verified.

Separate documented specification changes from your proposed technical responses.

Why this is impressive: This demonstrates how prompt engineering can support ongoing maintenance of production analytics, not just initial document analysis.

10. Turn the document into a traceable knowledge base

Technique: Structured knowledge extraction and retrieval-ready output

Question: Can the model transform an unstructured regulatory document into a reusable knowledge asset?

Using the verified CMS Stars specifications, create a structured knowledge repository for all measures in the document.

For each distinct requirement, extract:

* Measure ID and name.
* Requirement category.
* Exact requirement description.
* Numerical thresholds and measurement periods.
* Applicable conditions and exceptions.
* Source document and page number.
* Dependencies on other requirements.
* Any unresolved interpretation issues.

Organize the output as a structured table suitable for exporting into a relational database or knowledge management system.

Assign each requirement a unique identifier.

Do not combine distinct requirements into a single record when they have different conditions, sources, or calculation implications.

Include a separate table mapping each measure to its applicable general requirements.

Why this is impressive: You demonstrate how an LLM can be used as part of a larger document intelligence architecture, potentially supporting searchable specifications, downstream analytics, and RAG applications.

A live demonstration that ties everything together

If you want to showcase these techniques in a training session, I would organize them around a single Stars measure and progressively increase the complexity.

From a simple question to an AI-powered analytical workflow

Suggested 20-minute demonstration

* Minutes 0–3: Baseline prompt
    Ask ChatGPT to summarize the selected measure. Save the response.
* Minutes 3–7: Structured extraction
    Run your Stage 2 prompt. Compare the two outputs, focusing on eligibility, exclusions, calculation requirements, and source references.
* Minutes 7–11: Cross-document reasoning
    Ask the model to find requirements outside the primary measure section that affect the calculation.
* Minutes 11–15: Challenge the answer
    Run the self-critique prompt and investigate any identified omissions or unsupported statements.
* Minutes 15–20: Convert knowledge into action
    Generate an implementation decision tree and several edge-case validation scenarios.

The central lesson for your audience: Prompt engineering is not simply about getting ChatGPT to write a better answer. It is about designing a repeatable analytical workflow that turns source information into structured knowledge, tests the quality of that knowledge, and produces outputs that can support real business decisions and technical implementation.