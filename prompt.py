import json

prompt = """
<|begin_of_text|>
<|start_header_id|>system<|end_user_id|>
You are a deterministic system that converts user questions about contracts and agreements
into structured search instructions for a Qdrant vector database.

YOUR TASK:
Convert the input query into a SINGLE JSON object following the exact schema below.

INTENT CLASSIFICATION
Choose EXACTLY ONE intent:
- "date_lookup"
- "clause_lookup"
- "smart_search"
- "yes_no_check"

ENTITY EXTRACTION RULES:
Extract ONLY entities that are explicitly stated or strongly implied.
If an entity is not present, set it to null.
DO NOT infer or guess values.

FILTER RULES (CRITICAL):
1. Use ONLY the indexed payload fields listed below.
2. DO NOT create filters for fields that are not mentioned in the query.
3. If no filters apply, return an empty object.
4. Keyword fields → exact match only.
5. DATETIME filters → use ISO format with meta_value_iso only.

INDEXED PAYLOAD FIELDS
KEYWORD:
- doc_type
- customer_id
- customer_name
- agreement_id
- clause_id
- clause_title
- amendment_id
- template_id
- meta_field

TEXT:
- clause_text
- clause_title
- product_lines
- business_unit
- type_amendment
- type_of_pricing

OTHER:
- is_current (boolean)
- meta_value_iso (datetime)

SEMANTIC SEARCH RULES:
- Use semantic search ONLY when exact filters are insufficient.
- semantic_search_field must be ONE of the TEXT fields or null.

OUTPUT FORMAT (STRICT):
Return ONLY valid JSON.
No markdown.
No explanations.
No extra keys.
No trailing text.

JSON SCHEMA:
  "intent": "<one_of_the_intents>",
  "filters": "<field>": "<value>" ,
  "semantic_search_field": "<text_field_or_null>",
  "reasoning": "<max 1 short sentence>"

FIELD EXAMPLES:
doc_type: {doc_type_examples}
customer_name: {customer_name_examples}
clause_title: {clause_title_examples}
product_lines: {product_lines_examples}
business_unit: {bussiness_unit_examples}

INPUT OUTPUT EXAMPLES:
{examples}

<|eot_id|>
<|start_header_id|>user<|end_header_id|>  
    Query: {user_query}       
<|eot_id|>
<|start_header_id|>assistant<|end_header_id|>
"""

doc_type_examples = ["customer", "agreement", "amendment", "template", "clause", "metadata"]
customer_name_examples = ["Banner Health", "Baptish Health South Florida"]
clause_title_examples = ["Audit Rights", "Assignment", "Term and Termination", "Confidentiality", "Data Privacy"]
product_lines_examples = ["Neuwave", "Suture", "OnQ", "Sensus", "Endo"]
bussiness_unit_examples = ["Depuy Synthes", "Codman", "Cerenovus", "Ethicon"]

def build_prompt(user_query: str) -> str:
    examples = query_and_output_examples()
    
    return prompt.format(
        user_query=user_query,
        doc_type_examples=", ".join(doc_type_examples),
        customer_name_examples=", ".join(customer_name_examples),
        clause_title_examples=", ".join(clause_title_examples),
        product_lines_examples=", ".join(product_lines_examples),
        bussiness_unit_examples=", ".join(bussiness_unit_examples),
        examples=json.dumps(examples, indent=2)
    )

def query_and_output_examples():
    examples = [
        {
          "query": "When does the NeuWave PA expire?",
          "output": {
              "intent": "date_lookup",
              "filters": {
                  "product_lines": "NeuWave",
                  "clause_title": "Term and Termination"
              },
              "semantic_search_field": None,
              "reasoning": "NeuWave product line used to filter relevant agreements"
          }
      },
        {
          "query": "Does Banner Health have an audit rights clause in their agreement?",
          "output": {
              "intent": "yes_no_check",
              "filters": {
                  "customer_name": "Banner Health",
                  "clause_title": "Audit"
              },
              "semantic_search_field": None,
              "reasoning": ""
          }
      },
        {
          "query": "Which agreements will expire within the next 30 days, and which were extended via amendment for Banner Health?",
          "output": {
              "intent": "smart_search",
              "filters": {
                  "doc_type": "agreement",
                  "type_amendment": "ext"
              },
              "semantic_search_field": None,
              "reasoning": "type_amendment field used to filter amendments related to extensions"
          }
        }
    ]
    return examples
# a = json.dumps(query_and_output_examples(), indent=0)

# b = build_prompt("Does Banner Health have an audit rights clause in their agreement?")
# print(b)