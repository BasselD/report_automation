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