from pydantic import BaseModel

class DatasetBase(BaseModel):
    """Lightweight view of a IYP dataset."""
    organization: str
    name: str
    url: str
    readme_url: str
    reference_name: str | None
    
    readme_header: str


class DatasetFull(DatasetBase):
    """Heavy view of a IYP dataset"""
    readme_content: str
        
    
class NodeType(BaseModel):
    """View of a IYP node type"""
    name: str
    description: str
    
    
class RelationshipType(BaseModel):
    """View of a IYP relationship type"""
    name: str
    description: str
