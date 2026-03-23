# MIT License

# Copyright (c) 2024-2025 Neo4j

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging

from neo4j import AsyncDriver, RoutingControl

logger = logging.getLogger('mcp_neo4j_cypher')


DISCARD_EDGE_PROPERTIES = [
    'reference_time_fetch',
    'reference_url_info',
    'reference_time_modification',
    'reference_url_data']


def simplify_node_properties(properties: dict) -> list[str]:
    """Collapse in a list and filter out `existence`, `array` and `indexed`"""
    return [f"{key} ({val['type']})" for key, val in properties.items()]


def simplify_relationship_properties(properties: dict) -> list[str]:
    """Collapse in a list and filter out `existence`, `array` and `indexed`.

    Also remove non-essential properties.
    """
    return [f"{key} ({val['type']})" for key, val in properties.items() if key not in DISCARD_EDGE_PROPERTIES]


def clean_schema(schema: dict) -> dict:
    """Modified version of neo4j's to reduce APOC schema inspection verbosity."""
    cleaned = {}

    for key, entry in schema.items():

        # node or relationship
        entry_type = entry['type']
        new_entry = {'type': entry_type}
        if 'count' in entry:
            new_entry['count'] = entry['count']

        labels = entry.get('labels', [])
        if labels:
            new_entry['labels'] = labels

        props = entry.get('properties', {})
        if props:
            if entry_type == 'node':
                new_entry['properties'] = simplify_node_properties(props)
            elif entry_type == 'relationship':
                new_entry['properties'] = simplify_relationship_properties(props)
            else:
                raise ValueError(f'Unknown schema entry type: {entry_type}')

        if entry.get('relationships'):
            rels_out = {}
            for rel_name, rel in entry['relationships'].items():
                clean_rel = {}
                if 'direction' in rel:
                    clean_rel['direction'] = rel['direction']
                # nested labels
                rel_labels = rel.get('labels', [])
                if rel_labels:
                    clean_rel['labels'] = rel_labels
                # nested properties
                rel_props = rel.get('properties', {})
                clean_rel['properties'] = simplify_relationship_properties(rel_props)
                if clean_rel:
                    rels_out[rel_name] = clean_rel

            if rels_out:
                new_entry['relationships'] = rels_out

        cleaned[key] = new_entry

    return cleaned


async def retrieve_schema(neo4j_driver: AsyncDriver, sample_size: int = 100) -> dict:
    """Call APOC procedure to retrieve the schema."""

    logger.info(f'Running `get_neo4j_schema` with sample size {sample_size}.'
                f'(this might take few minutes)')

    get_schema_query = f'CALL apoc.meta.schema({{sample: {sample_size}}}) YIELD value RETURN value'

    results_json = await neo4j_driver.execute_query(
        get_schema_query,
        routing_control=RoutingControl.READ,
        database_='neo4j',
        result_transformer_=lambda r: r.data(),
    )

    schema_clean = clean_schema(results_json[0].get('value'))

    return schema_clean
