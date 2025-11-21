from pydantic import BaseModel


class Dataset(BaseModel):
    """Dataset imported in IYP"""
    organization: str
    name: str
    url: str
    readme_url: str
    reference_name: str | None = None
        
    
class NodeType(BaseModel):
    node_type: str
    description: str
    
    
class RelationshipType(BaseModel):
    relationship_type: str
    description: str
