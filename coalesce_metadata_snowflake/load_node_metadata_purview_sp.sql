/*
Description: 
    This stored procedure fetches node metadata from Coalesce API and loads it into Microsoft Purview
    using the Atlas Entity API. It handles both the initial node list retrieval and detailed metadata
    for each node, transforming the data into Atlas entity format.

Parameters:
    WORKSPACE_ID      - NUMBER   : The Coalesce workspace ID to fetch nodes from
    PURVIEW_ENDPOINT  - STRING   : The Purview Atlas API endpoint (e.g., 'https://your-account.purview.azure.com')
    PURVIEW_TOKEN     - STRING   : The OAuth token for Purview authentication
    COALESCE_BASE_URL - STRING   : Base URL for the Coalesce API (e.g., 'https://app.coalescesoftware.io')

Returns:
    STRING containing execution logs and error messages if any

Prerequisites:
    1. A configured Microsoft Purview account with Atlas API access
    2. Valid OAuth token for Purview authentication
    3. Valid Coalesce API token
    4. Appropriate network access to both APIs
*/

CREATE OR REPLACE PROCEDURE LOAD_COALESCE_NODES_TO_PURVIEW(
    WORKSPACE_ID NUMBER,
    PURVIEW_ENDPOINT STRING,
    PURVIEW_TOKEN STRING,
    COALESCE_BASE_URL STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('requests')
HANDLER = 'run_load'
AS
$$
import requests
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

class CoalescePurviewLoader:
    """
    Handles loading Coalesce node metadata into Microsoft Purview using Atlas Entity API.
    
    This class manages the entire ETL process:
    - Fetching node metadata from Coalesce API
    - Transforming data into Atlas entity format
    - Loading data into Purview via Atlas Entity API
    """
    
    def __init__(self, workspace_id: int, purview_endpoint: str, 
                 purview_token: str, coalesce_base_url: str):
        """Initialize the loader with API connection details."""
        self.workspace_id = workspace_id
        self.purview_endpoint = purview_endpoint.rstrip('/')
        self.coalesce_base_url = coalesce_base_url.rstrip('/')
        
        # Coalesce API headers
        self.coalesce_headers = {
            "accept": "application/json",
            "authorization": f"Bearer {purview_token}"  # Using same token for demo
        }
        
        # Purview API headers
        self.purview_headers = {
            "Authorization": f"Bearer {purview_token}",
            "Content-Type": "application/json"
        }
        
        self.log_message = ""

    def append_log(self, message: str) -> None:
        """Add a message to the log with a newline."""
        self.log_message += f"{message}\n"

    def get_node_list(self) -> List[str]:
        """Fetch list of node IDs from Coalesce API."""
        url = f"{self.coalesce_base_url}/api/v1/workspaces/{self.workspace_id}/nodes"
        response = requests.get(url, headers=self.coalesce_headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch node list. Status code: {response.status_code}, Response: {response.text}")
        
        response_data = response.json()
        return [node['id'] for node in response_data['data']]

    def get_node_metadata(self, node_id: str) -> Optional[Dict]:
        """Fetch detailed metadata for a specific node."""
        url = f"{self.coalesce_base_url}/api/v1/workspaces/{self.workspace_id}/nodes/{node_id}"
        response = requests.get(url, headers=self.coalesce_headers)
        
        if response.status_code != 200:
            self.append_log(f"Warning: Failed to fetch metadata for node {node_id}")
            return None
            
        return response.json()

    def transform_to_atlas_entity(self, node_data: Dict, node_id: str) -> Dict:
        """Transform Coalesce node metadata into Atlas entity format."""
        # Generate a qualified name that combines workspace and node ID
        qualified_name = f"coalesce://{self.workspace_id}/nodes/{node_id}"
        
        # Map Coalesce node types to Atlas entity types
        node_type = node_data.get('nodeType', 'unknown').lower()
        atlas_type = {
            'table': 'coalesce_table',
            'view': 'coalesce_view',
            'procedure': 'coalesce_procedure',
            'script': 'coalesce_script'
        }.get(node_type, 'coalesce_node')
        
        # Create Atlas entity
        atlas_entity = {
            "typeName": atlas_type,
            "attributes": {
                "qualifiedName": qualified_name,
                "name": node_data.get('name', f"node_{node_id}"),
                "description": node_data.get('description', ''),
                "nodeType": node_data.get('nodeType', ''),
                "workspace_id": str(self.workspace_id),
                "node_id": node_id,
                "database": node_data.get('database', ''),
                "schema": node_data.get('schema', ''),
                "createTime": datetime.now().isoformat(),
            },
            "status": "ACTIVE"
        }
        
        # Add any custom attributes
        if 'properties' in node_data:
            atlas_entity['customAttributes'] = node_data['properties']
            
        return atlas_entity

    def bulk_load_to_purview(self, entities: List[Dict]) -> None:
        """Load multiple entities into Purview using bulk API."""
        if not entities:
            return
            
        url = f"{self.purview_endpoint}/api/atlas/v2/entity/bulk"
        
        # Prepare bulk request payload
        payload = {
            "entities": entities,
            "referredEntities": {}
        }
        
        response = requests.post(url, headers=self.purview_headers, json=payload)
        
        if response.status_code not in (200, 201):
            raise Exception(f"Failed to load entities to Purview. Status: {response.status_code}, Response: {response.text}")
        
        self.append_log(f"Successfully loaded {len(entities)} entities to Purview")

    def execute(self) -> str:
        """Execute the node metadata loading process and return log messages."""
        self.log_message = ""
        self.append_log(f"Starting procedure for workspace {self.workspace_id}...")
        
        try:
            # Get node list
            self.append_log("Fetching node list from Coalesce API...")
            node_ids = self.get_node_list()
            self.append_log(f"Found {len(node_ids)} nodes to process")
            
            # Process nodes in batches of 100
            batch_size = 100
            atlas_entities = []
            
            for i, node_id in enumerate(node_ids, 1):
                node_data = self.get_node_metadata(node_id)
                if node_data is not None:
                    atlas_entity = self.transform_to_atlas_entity(node_data, node_id)
                    atlas_entities.append(atlas_entity)
                    self.append_log(f"Processed node {i}/{len(node_ids)}: {node_id}")
                
                # Load batch if we've reached batch_size or this is the last node
                if len(atlas_entities) >= batch_size or i == len(node_ids):
                    self.bulk_load_to_purview(atlas_entities)
                    atlas_entities = []  # Reset for next batch
            
            self.append_log("Completed loading all nodes to Purview")
            
        except Exception as e:
            self.append_log(f"Error: {str(e)}")
            import traceback
            self.append_log(f"Stack trace: {traceback.format_exc()}")
            
        return self.log_message


def run_load(snowpark_session, WORKSPACE_ID, PURVIEW_ENDPOINT, PURVIEW_TOKEN, COALESCE_BASE_URL):
    """Entry point for the stored procedure"""
    loader = CoalescePurviewLoader(
        WORKSPACE_ID, PURVIEW_ENDPOINT, PURVIEW_TOKEN, COALESCE_BASE_URL
    )
    return loader.execute()
$$;

-- Example usage:
/*
CALL LOAD_COALESCE_NODES_TO_PURVIEW(
    15,                                         -- WORKSPACE_ID
    'https://your-account.purview.azure.com',   -- PURVIEW_ENDPOINT
    'your-purview-token',                       -- PURVIEW_TOKEN
    'https://app.coalescesoftware.io'           -- COALESCE_BASE_URL
);
*/
