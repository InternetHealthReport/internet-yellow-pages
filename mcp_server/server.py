from mcp.server.fastmcp import FastMCP
from mcp_server.documentation.parsers import (
    parse_datasets,
    parse_node_types,
    parse_relationship_types,
)
from mcp_server.documentation.models import (
    DatasetFull,
    DatasetBase,
    NodeType,
    RelationshipType,
)
from pydantic import Field
from typing import Literal

# Load documentation
datasets = {dataset.reference_name: dataset for dataset in parse_datasets()}
node_types = {node_type.name: node_type for node_type in parse_node_types()}
rel_types = {rel_type.name: rel_type for rel_type in parse_relationship_types()}

instructions = """This MCP server exposes the Internet Yellow Pages (IYP) documentation.
IYP is an open source Neo4j knowledge database that gathers information about Internet resources (for example ASNs, IP prefixes, and domain names)."""

mcp = FastMCP(
    "Internet Yellow Pages (IYP)", host="0.0.0.0", port=8002, instructions=instructions
)


@mcp.tool()
def list_iyp_datasets() -> list[DatasetBase]:
    """List a light view of datasets in Internet Yellow Pages. For a full description a dataset, call `get_resource`."""
    # Downcast `DatasetFull` to `DatasetBase`` to save context
    return [DatasetBase.model_validate(dataset) for dataset in datasets.values()]


@mcp.tool()
def list_iyp_node_types() -> list[NodeType]:
    """List all node types in Internet Yellow Pages."""
    return [node_type for node_type in node_types.values()]


@mcp.tool()
def list_iyp_relationship_types() -> list[RelationshipType]:
    """List all relationship types in Internet Yellow Pages."""
    return [rel_type for rel_type in rel_types.values()]


# General templated resource access
# Not discoverable by clients, but callable by an LLM if wrapped in a tool
@mcp.resource("dataset://{reference_name}")
def get_iyp_dataset_documentation(reference_name: str) -> DatasetFull:
    """Get complete documentation of a dataset associated to `reference_name` in Internet Yellow Pages"""
    return datasets[reference_name]


@mcp.resource("node-type://{name}")
def get_iyp_node_type_documentation(name: str) -> NodeType:
    """Get complete documentation of a node type associated to `name` in Internet Yellow Pages"""
    return node_types[name]


@mcp.resource("relationship-type://{name}")
def get_iyp_relationship_type_documentation(name: str) -> RelationshipType:
    """Get complete documentation of a relationship type associated to `name` in Internet Yellow Pages"""
    return rel_types[name]


# Wrap general templated resource access in a tool.
# LLM can decide himself to fetch a resource
scheme2getter = {
    "dataset": get_iyp_dataset_documentation,
    "node-type": get_iyp_node_type_documentation,
    "relationship-type": get_iyp_relationship_type_documentation,
}


@mcp.tool()
def get_resource(
    scheme: Literal["dataset", "node-type", "relationship-type"] = Field(
        description="The scheme to access the resource.",
    ),
    name: str = Field(
        description="Unique identifier of the resource (`reference_name` for datasets, `name` otherwise)"
    ),
):
    """Get the Internet Yellow Pages documentation associated to the resource."""

    if scheme not in scheme2getter:
        return {
            "error": f"Unknown or unsupported resource URI scheme: '{scheme}://'. Only 'dataset://', 'node-type://' and relationship-type:// are supported.",
            "status": 400,
        }

    try:
        resource_data = scheme2getter[scheme](name)

        return resource_data

    except ValueError as e:
        # Handle the specific error raised by the getter (e.g., resource not found)
        return {"error": str(e), "status": 404}
    except Exception as e:
        # General error handling
        return {
            "error": f"An unexpected error occurred while fetching dataset: {e}",
            "status": 500,
        }


# Non-templated resource access
# Resources are registered one by one to make them discoverable by MCP clients
def register_dataset_resource(reference_name: str, dataset: DatasetFull):
    # Create a specific URI for this dataset
    uri = f"dataset://{reference_name}"

    dynamic_description = f"Returns documentation of the {dataset.name} dataset."

    def get_dataset() -> DatasetFull:
        return dataset

    get_dataset.__doc__ = dynamic_description
    mcp.resource(uri)(get_dataset)


for reference_name, dataset in datasets.items():
    register_dataset_resource(reference_name, dataset)


def register_node_type_resource(name: str, node_type: NodeType):
    uri = f"node-type://{name}"

    dynamic_description = f"Returns documentation of the {name} node type"

    def get_node_type() -> NodeType:
        return node_type

    get_node_type.__doc__ = dynamic_description
    mcp.resource(uri)(get_node_type)


for name, node_type in node_types.items():
    register_node_type_resource(name, node_type)


def register_relationship_type_resource(name: str, rel_type: RelationshipType):
    uri = f"relationship-type://{name}"

    dynamic_description = f"Returns documentation of the {name} relation type"

    def get_relationship_type() -> RelationshipType:
        return rel_type

    get_relationship_type.__doc__ = dynamic_description
    mcp.resource(uri)(get_relationship_type)


for name, rel_type in rel_types.items():
    register_relationship_type_resource(name, rel_type)


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
