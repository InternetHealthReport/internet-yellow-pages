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


import argparse
import asyncio
import json
import logging
from typing import Any, Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from neo4j import AsyncGraphDatabase, Query, RoutingControl
from neo4j.exceptions import Neo4jError
from pydantic import BaseModel, Field

from mcp_server.documentation.models import (DatasetBase, DatasetFull,
                                             NodeType, RelationshipType)
from mcp_server.documentation.parsers import (parse_datasets, parse_node_types,
                                              parse_relationship_types)
from mcp_server.mcp_neo4j.schema import retrieve_schema
from mcp_server.mcp_neo4j.utils import (is_write_query,
                                        truncate_string_to_tokens,
                                        value_sanitize)

# Setup Logging
logger = logging.getLogger('mcp_neo4j_cypher')
logger.setLevel(logging.INFO)

# Constants


class ServerConfig(BaseModel):
    neo4j_read_timeout: int = Field(default=60, description='Timemout for Neo4j read in seconds (default 60)')
    token_limit: int | None = Field(default=None, description='GPT token limit per response (default None)')
    port: int = Field(default=8010)
    sample_size: int = Field(default=5000, description='Sample size used when querying the schema at server startup')


config = ServerConfig()

datasets = {dataset.reference_name: dataset for dataset in parse_datasets()}
node_types = {node_type.name: node_type for node_type in parse_node_types()}
rel_types = {rel_type.name: rel_type for rel_type in parse_relationship_types()}


# Server factory function following neo4j structure
# Not an expert but apparently needed for async event context
def create_mcp_server(neo4j_driver, schema):
    """Creates the FastMCP server instance with all tools registered.

    We pass the 'neo4j_driver' and 'schema' in as arguments so the tools can close over
    them safely.
    """

    instructions = (
        'This MCP server exposes the Internet Yellow Pages (IYP) knowledge graph. '
        'IYP is an open source Neo4j knowledge database that gathers information '
        'about Internet resources (for example ASNs, IP prefixes, and domain names).'
        'To help the AI assistant retrieve domain-specific knowledge (Internet concept, datasets)'
        ', this MCP exposes the documentation and schema of IYP, as well as'
        'tools to retrieve or modify data in IYP via Cypher queries.'
        'Using a tool to retrieve the schema before making a cypher query is strongly advised.'
    )

    mcp = FastMCP(
        'Internet Yellow Pages (IYP)',
        instructions=instructions
    )

    # Part 1: documentation tool

    @mcp.tool()
    async def list_iyp_datasets() -> list[DatasetBase]:
        """List a light view of datasets in Internet Yellow Pages."""
        return [DatasetBase.model_validate(dataset) for dataset in datasets.values()]

    @mcp.tool()
    async def list_iyp_node_types() -> list[NodeType]:
        """List all node types in Internet Yellow Pages."""
        return [node_type for node_type in node_types.values()]

    @mcp.tool()
    async def list_iyp_relationship_types() -> list[RelationshipType]:
        """List all relationship types in Internet Yellow Pages."""
        return [rel_type for rel_type in rel_types.values()]

    # Helper getters for resources
    def get_iyp_dataset_documentation(reference_name: str) -> DatasetFull:
        return datasets[reference_name]

    def get_iyp_node_type_documentation(name: str) -> NodeType:
        return node_types[name]

    def get_iyp_relationship_type_documentation(name: str) -> RelationshipType:
        return rel_types[name]

    scheme2getter = {
        'dataset': get_iyp_dataset_documentation,
        'node-type': get_iyp_node_type_documentation,
        'relationship-type': get_iyp_relationship_type_documentation,
    }

    @mcp.tool()
    async def get_iyp_resource(
        scheme: Literal['dataset', 'node-type', 'relationship-type'] = Field(
            description='The scheme to access the resource.',
        ),
        name: str = Field(
            description='Unique identifier of the resource.'
        ),
    ):
        """Get the Internet Yellow Pages documentation associated to the resource."""
        if scheme not in scheme2getter:
            raise ToolError(f"Unknown scheme: '{scheme}://'. Supported: dataset, node-type, relationship-type.")

        try:
            return scheme2getter[scheme](name)
        except KeyError:
            raise ToolError(f"Resource '{scheme}://{name}' does not exist")
        except Exception as e:
            raise ToolError(f'Unexpected error {e}')

    # Non-templated resource access
    # Resources are registered one by one to make them discoverable by MCP clients
    def register_dataset_resource(reference_name: str, dataset: DatasetFull):
        # Create a specific URI for this dataset
        uri = f'dataset://{reference_name}'

        dynamic_description = f'Returns documentation of the {dataset.name} dataset.'

        def get_dataset() -> DatasetFull:
            return dataset

        get_dataset.__doc__ = dynamic_description
        mcp.resource(uri)(get_dataset)

    for reference_name, dataset in datasets.items():
        register_dataset_resource(reference_name, dataset)

    def register_node_type_resource(name: str, node_type: NodeType):
        uri = f'node-type://{name}'

        dynamic_description = f'Returns documentation of the {name} node type'

        def get_node_type() -> NodeType:
            return node_type

        get_node_type.__doc__ = dynamic_description
        mcp.resource(uri)(get_node_type)

    for name, node_type in node_types.items():
        register_node_type_resource(name, node_type)

    def register_relationship_type_resource(name: str, rel_type: RelationshipType):
        uri = f'relationship-type://{name}'

        dynamic_description = f'Returns documentation of the {name} relation type'

        def get_relationship_type() -> RelationshipType:
            return rel_type

        get_relationship_type.__doc__ = dynamic_description
        mcp.resource(uri)(get_relationship_type)

    for name, rel_type in rel_types.items():
        register_relationship_type_resource(name, rel_type)

    # Part 2: Neo4j tools

    # Version where we force to use specify node types
    # advantage: reduce context, force LLM to read doc
    # disadvantage: add more steps, sometimes LLM get it wrong
    # Use `include_node_types` to project the schema onto a smaller, relevant
    # subgraph, as the full schema is often too large to process.
    @mcp.tool()
    async def get_neo4j_schema_projected(include_node_types: list[str] = Field(
        description='A list of node types to include in the returned schema.'),
        include_relationship_types: list[str] = Field(
        description='An optional list of relationship types to include in the returned schema.',
        default=[]
    )
    ) -> str:
        """Returns the neo4j schema, including node types, their properties, and
        relationship types.

        Confirm the node type with `list_iyp_node_types` before calling this tool.
        """

        if not include_node_types:
            raise ToolError('Provide a list of node types to project the schema')

        desired_keys = include_node_types + include_relationship_types
        projected_schema = {}

        for key, entry in schema.items():
            if key not in desired_keys:
                continue

            new_entry = {'type': entry['type']}

            if 'count' in entry:
                new_entry['count'] = entry['count']

            labels = entry.get('labels', [])
            if labels:
                new_entry['labels'] = labels

            props = entry.get('properties', {})
            new_entry['properties'] = props

            rels = entry.get('relationships', {})
            if rels:
                simplified_rels = {}
                for rel_type, data in rels.items():
                    data = {k: v for k, v in data.items()
                            if k not in ['properties']}
                    simplified_rels[rel_type] = data
                new_entry['relationships'] = simplified_rels

            projected_schema[key] = new_entry

        results_str = json.dumps(projected_schema, default=str)
        return truncate_string_to_tokens(results_str, config.token_limit) if config.token_limit else results_str

    # Version withouth node types filtering
    # advantage: less opinionated, less steps
    # disadvantage: context (around 30k)
    @mcp.tool()
    async def get_neo4j_schema() -> str:
        """Returns the neo4j schema, including node types, their properties, and
        relationship types."""
        results_str = json.dumps(schema, default=str)
        return truncate_string_to_tokens(results_str, config.token_limit) if config.token_limit else results_str

    @mcp.tool()
    async def read_neo4j_cypher(
        query: str = Field(..., description='The Cypher query to execute.'),
        params: dict[str, Any] = Field(dict(), description='Query parameters.'),
    ) -> str:
        """Execute a read Cypher query on the neo4j database."""

        if is_write_query(query):
            raise ValueError('Only MATCH queries are allowed for read-query')

        try:
            # We use the 'neo4j_driver' passed into create_mcp_server
            query_obj = Query(query, timeout=float(config.neo4j_read_timeout))
            results = await neo4j_driver.execute_query(
                query_obj,
                parameters_=params,
                routing_control=RoutingControl.READ,
                database_='neo4j',
                result_transformer_=lambda r: r.data(),
            )
            sanitized = [value_sanitize(el) for el in results]
            res_str = json.dumps(sanitized, default=str)

            if config.token_limit:
                res_str = truncate_string_to_tokens(res_str, config.token_limit)

            return res_str

        except Neo4jError as e:
            raise ToolError(f'Neo4j Error: {e}\n{query}\n{params}')
        except Exception as e:
            raise ToolError(f'Error: {e}\n{query}\n{params}')

    @mcp.tool()
    async def write_neo4j_cypher(
        query: str = Field(..., description='The Cypher query to execute.'),
        params: dict[str, Any] = Field(dict(), description='Query parameters.'),
    ) -> str:
        """Execute a write Cypher query on the neo4j database."""

        if not is_write_query(query):
            raise ValueError('Only write queries are allowed for write-query')

        try:
            _, summary, _ = await neo4j_driver.execute_query(
                query,
                parameters_=params,
                routing_control=RoutingControl.WRITE,
                database_='neo4j',
            )
            return json.dumps(summary.counters.__dict__, default=str)

        except Neo4jError as e:
            raise ToolError(f'Neo4j Error: {e}\n{query}\n{params}')
        except Exception as e:
            raise ToolError(f'Error: {e}\n{query}\n{params}')

    return mcp


async def main():

    db_url = 'bolt://iyp:7687'
    auth = ('neo4j', 'password')

    parser = argparse.ArgumentParser(description='MCP Neo4j Server')
    parser.add_argument('--port', type=int, default=config.port)
    parser.add_argument('--sample-size', type=int, default=config.sample_size)
    parser.add_argument('--neo4j-read-timeout', type=int, default=config.neo4j_read_timeout)
    parser.add_argument('--token-limit', type=int, default=config.token_limit)

    args = parser.parse_args()

    config.port = args.port
    config.sample_size = args.sample_size
    config.neo4j_read_timeout = args.neo4j_read_timeout
    config.token_limit = args.token_limit

    logging.info(f'Starting server with config {config}')

    logger.info(f'Connecting to Neo4j at {db_url}...')

    async with AsyncGraphDatabase.driver(db_url, auth=auth) as driver:

        try:
            await driver.verify_connectivity()
            logger.info('Neo4j connectivity verified.')
        except Exception as e:
            logger.error(f'Failed to connect to Neo4j: {e}')
            return

        logger.info('Retrieving schema from IYP...')
        try:
            schema = await retrieve_schema(driver, sample_size=config.sample_size)
            logger.info('Schema retrieved successfully.')
        except Exception as e:
            logger.error(f'Failed to retrieve schema: {e}')
            schema = {}

        mcp = create_mcp_server(driver, schema)

        logger.info(f'Starting MCP Server on port {config.port}')

        await mcp.run_http_async(host='0.0.0.0', port=config.port)

if __name__ == '__main__':
    import sys
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    asyncio.run(main())
