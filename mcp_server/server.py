from mcp.server.fastmcp import FastMCP
from mcp_server.documentation.parsers import parse_datasets, parse_node_types, parse_relationship_types
from mcp_server.documentation.models import DatasetFull, DatasetBase, NodeType, RelationshipType
from urllib.parse import urlparse

mcp = FastMCP("Internet Yellow Pages (IYP)", host="0.0.0.0", port=8002)

# Load doc
datasets = {dataset.reference_name: dataset for dataset in parse_datasets()}
node_types = {node_type.name: node_type for node_type in parse_node_types()}
rel_types = {rel_type.name: rel_type for rel_type in parse_relationship_types()}


# Register dataset resources one by one to make them discoverable
def register_dataset_resource(reference_name: str, dataset: DatasetFull):
    # Create a specific URI for this dataset
    uri = f"dataset://{reference_name}"
    
    dynamic_description = (
        f"Returns documentation of the {dataset.name} dataset."
    )
    
    def get_dataset() -> DatasetFull:
        return dataset
    get_dataset.__doc__ = dynamic_description
    mcp.resource(uri)(get_dataset)

for reference_name, dataset in datasets.items():
    register_dataset_resource(reference_name, dataset)
    

# Register node type resources one by one
def register_node_type_resource(name: str, node_type: NodeType):
    uri = f"nodetype://{name}"
    
    dynamic_description = f"Returns documentation of the {name} node type"
    
    def get_node_type() -> NodeType:
        return node_type
    get_node_type.__doc__ = dynamic_description
    mcp.resource(uri)(get_node_type)
    
for name, node_type in node_types.items():
    register_node_type_resource(name, node_type)
    
    
# Register relationship type resources one by one
def register_relationship_type_resource(name: str, rel_type: RelationshipType):
    uri = f"relationshiptype://{name}"

    dynamic_description = f"Returns documentation of the {name} relation type"

    def get_relationship_type() -> RelationshipType:
        return rel_type

    get_relationship_type.__doc__ = dynamic_description
    mcp.resource(uri)(get_relationship_type)
    
for name, rel_type in rel_types.items():
    register_relationship_type_resource(name, rel_type)

# General template resource access
# Not discovered by clients, but callable by an LLM if wrapped in a tool
@mcp.resource("dataset://{reference_name}")
def get_dataset(reference_name: str) -> DatasetFull:
    """Get dataset associated to `reference_name`"""
    return datasets[reference_name]

@mcp.resource("nodetype://{name}")
def get_node_type(name: str) -> NodeType:
    """Get node type associated to`name`"""
    return node_types[name]

@mcp.resource("relationshiptype://{name}")
def get_relationship_type(name: str) -> RelationshipType:
    """Get relationship type associated to `name`"""
    return rel_types[name]

scheme2getter = {
    "dataset": get_dataset,
    "nodetype": get_node_type,
    "relationshiptype": get_relationship_type,
}

# Wrap general templated resource access in a tool.
@mcp.tool()
def get_resource(uri: str):
    """Get the resource associated to the uri (e.g. dataset://<reference_name>, nodetype://<name>, relationshiptype://<name>)."""
    
    scheme = urlparse(uri).scheme
    if scheme not in scheme2getter:
        return {
            "error": f"Unknown or unsupported resource URI scheme: '{scheme}://'. Only 'dataset://', 'nodetype://' and relationshiptype:// are supported.",
            "status": 400,
        }
    
    identifier = uri[len(f"{scheme}://") :]

    try:
        resource_data = scheme2getter[scheme](identifier)

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


@mcp.tool()
def list_datasets() -> list[DatasetBase]:
    """List all datasets."""
    # I downcast to save context
    return [DatasetBase.model_validate(dataset) for dataset in datasets.values()]

@mcp.tool()
def list_node_types() -> list[NodeType]:
    """List all node types."""
    return [node_type for node_type in node_types.values()]

@mcp.tool()
def list_rel_types() -> list[RelationshipType]:
    """List all relationship types."""
    return [rel_type for rel_type in rel_types.values()]
    

if __name__ == "__main__":

    mcp.run(transport="streamable-http")
