from mcp.server.fastmcp import FastMCP
from mcp_server.documentation.parsers import parse_datasets
from mcp_server.documentation.models import DatasetFull, DatasetBase

mcp = FastMCP("Internet Yellow Pages (IYP)", port=8001)

datasets = {dataset.reference_name: dataset for dataset in parse_datasets()}


# Register dataset resources one by one to make them discoverable
def register_dataset_resource(reference_name: str, data: DatasetFull):
    # Create a specific URI for this dataset
    uri = f"dataset://{reference_name}"
    
    dynamic_description = (
        f"Returns documentation of the {data.name} dataset."
    )
    
    def get_dataset() -> DatasetFull:
        return data
    get_dataset.__doc__ = dynamic_description
    mcp.resource(uri)(get_dataset)

for reference_name, dataset_obj in datasets.items():
    register_dataset_resource(reference_name, dataset_obj)


@mcp.resource("dataset://{reference_name}")
def get_dataset(reference_name: str) -> DatasetFull:
    """Get dataset associated to `reference_name`"""
    return datasets[reference_name]


@mcp.tool()
def list_datasets() -> list[DatasetBase]:
    """List all datasets."""
    # I downcast to save context
    return [DatasetBase.model_validate(dataset) for dataset in datasets.values()]


@mcp.tool()
def get_resource(uri: str):
    """Get the resource associated to the uri (e.g. dataset://<reference_name>)."""
    
    if uri.startswith("dataset://"):
        # 1. Extract the unique identifier (the part after the scheme)
        reference_name = uri[len("dataset://") :]

        # 2. Call the specific handler
        try:
            # We call the resource handler directly
            resource_data = get_dataset(reference_name)

            return resource_data

        except ValueError as e:
            # Handle the specific error raised by get_dataset (e.g., resource not found)
            return {"error": str(e), "status": 404}
        except Exception as e:
            # General error handling
            return {
                "error": f"An unexpected error occurred while fetching dataset: {e}",
                "status": 500,
            }

    # 3. Handle unknown schemes
    scheme, _, _ = uri.partition("://")
    return {
        "error": f"Unknown or unsupported resource URI scheme: '{scheme}://'. Only 'dataset://' is supported.",
        "status": 400,
    }
    

if __name__ == "__main__":

    mcp.run(transport="streamable-http")
