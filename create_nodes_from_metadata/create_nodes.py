"""
Coalesce Node Creation Script

This script creates multiple Coalesce nodes from a JSON configuration file.
It reads node configurations and creates them via the Coalesce API.

Usage:
    1. Edit the WORKSPACE_ID variable at the top of this script to your workspace ID
    2. Set your environment variables:
       export COALESCE_AUTH_TOKEN='your-api-token-here'
       export COALESCE_BASE_URL='app.coalescesoftware.io'  # optional, defaults to this
    
    3. Run the script:
       python create_nodes.py

Configuration:
    - WORKSPACE_ID: Coalesce workspace ID (edit the variable at the top of the script)

Environment Variables:
    - COALESCE_AUTH_TOKEN: Required Coalesce API authentication token
    - COALESCE_BASE_URL: Optional Coalesce API base URL (defaults to app.coalescesoftware.io)

What the script does:
    - Fetches all existing nodes from the workspace using GET API
    - For each existing node, creates a 3-level hierarchy:
      * Stage (child of existing node)
      * persistentStage (child of Stage)
      * Stage (child of persistentStage)

Requirements:
    - Python 3.6+
    - requests library (pip install requests)
    - Valid Coalesce API token
    - Network access to app.coalescesoftware.io
"""

import requests
import json
import sys
import os

# Configuration
WORKSPACE_ID = "15"  # Change this to your workspace ID


def create_node(base_url, workspace_id, node_config, auth_token):
    """
    Create a single node using the Coalesce API.
    
    Args:
        base_url (str): Coalesce API base URL (e.g., "app.coalescesoftware.io" or "https://app.coalescesoftware.io")
        workspace_id (str): Coalesce workspace ID
        node_config (dict): Node configuration containing nodeType and predecessorNodeIDs
        auth_token (str): Coalesce API authentication token
        
    Returns:
        dict: Result dictionary containing:
            - status_code: HTTP response status code
            - response: API response body
            - node_name: Name of the node (for logging)
    """
    # Ensure base_url has proper protocol prefix
    if not base_url.startswith(('http://', 'https://')):
        base_url = f"https://{base_url}"
    
    # Prepare payload with nodeType and predecessorNodeIDs
    payload = {
        "nodeType": node_config.get("nodeType"),
        "predecessorNodeIDs": node_config.get("predecessorNodeIDs", [])
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {auth_token}'
    }
    
    # Make the API request
    endpoint = f"/api/v1/workspaces/{workspace_id}/nodes"
    url = f"{base_url}{endpoint}"
    
    # Make the request using requests library
    response = requests.post(url, json=payload, headers=headers)
    
    # Print API response for debugging
    print(f"API Response:")
    print(f"Status Code: {response.status_code}")
    
    # Try to pretty print JSON response, fallback to raw text if not JSON
    try:
        response_json = response.json()
        print(f"Response Body:")
        print(json.dumps(response_json, indent=2))
    except (ValueError, json.JSONDecodeError):
        print(f"Response Body: {response.text}")
    
    print("-" * 50)
    
    # Extract nodeID from response if successful
    node_id = None
    if response.status_code in [200, 201]:
        try:
            response_json = response.json()
            node_id = response_json.get("id")
        except (ValueError, json.JSONDecodeError):
            pass
    
    return {
        'status_code': response.status_code,
        'response': response.text,
        'node_name': node_config.get("name", "Unnamed"),
        'node_id': node_id
    }


def get_workspace_nodes(base_url, workspace_id, auth_token):
    """
    Get all nodes from a workspace using the Coalesce API.
    
    Args:
        base_url (str): Coalesce API base URL
        workspace_id (str): Coalesce workspace ID
        auth_token (str): Coalesce API authentication token
        
    Returns:
        list: List of node IDs from the workspace
    """
    # Ensure base_url has proper protocol prefix
    if not base_url.startswith(('http://', 'https://')):
        base_url = f"https://{base_url}"
    
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {auth_token}'
    }
    
    # Make the API request
    endpoint = f"/api/v1/workspaces/{workspace_id}/nodes"
    url = f"{base_url}{endpoint}"
    
    print(f"Fetching existing nodes from workspace...")
    print(f"GET {url}")
    print("-" * 50)
    
    # Make the request using requests library
    response = requests.get(url, headers=headers)
    
    # Print API response for debugging
    print(f"API Response:")
    print(f"Status Code: {response.status_code}")
    
    # Try to pretty print JSON response, fallback to raw text if not JSON
    try:
        response_json = response.json()
        print(f"Response Body:")
        print(json.dumps(response_json, indent=2))
    except (ValueError, json.JSONDecodeError):
        print(f"Response Body: {response.text}")
    
    print("-" * 50)
    
    # Extract node IDs from response
    node_ids = []
    if response.status_code == 200:
        try:
            response_json = response.json()
            if 'data' in response_json and isinstance(response_json['data'], list):
                node_ids = [node.get('id') for node in response_json['data'] if node.get('id')]
                print(f"Found {len(node_ids)} existing nodes in workspace")
            else:
                print("No 'data' field found in response or data is not a list")
        except (ValueError, json.JSONDecodeError):
            print("Failed to parse response JSON")
    else:
        print(f"Failed to fetch nodes. Status code: {response.status_code}")
    
    return node_ids

def main():
    """
    Main function to orchestrate node creation.
    
    This function:
    1. Fetches existing nodes from workspace using GET API
    2. Validates required parameters (workspaceID, auth token)
    3. Creates 3-level hierarchy (Stage -> persistentStage -> Stage) for each existing node
    4. Reports progress and results
    
    Configuration:
        WORKSPACE_ID: Coalesce workspace ID (set as variable at top of script)
        
    Environment Variables:
        COALESCE_AUTH_TOKEN: Required Coalesce API authentication token
        COALESCE_BASE_URL: Coalesce API base URL (defaults to app.coalescesoftware.io)
    """
    # Check environment variables
    auth_token = os.getenv("COALESCE_AUTH_TOKEN")
    base_url = os.getenv("COALESCE_BASE_URL", "app.coalescesoftware.io")
    workspace_id = WORKSPACE_ID
    
    if not auth_token:
        print("Error: COALESCE_AUTH_TOKEN environment variable not set.")
        print("Please set your Coalesce API token: export COALESCE_AUTH_TOKEN='your-token-here'")
        sys.exit(1)
    
    # Step 1: Get existing nodes from workspace
    existing_node_ids = get_workspace_nodes(base_url, workspace_id, auth_token)
    if not existing_node_ids:
        print("Error: No existing nodes found in workspace.")
        print("Please ensure there are nodes in the workspace before running this script.")
        sys.exit(1)
    
    # Step 2: Display progress header
    print(f"Found {len(existing_node_ids)} existing nodes in workspace {workspace_id}")
    print(f"Creating 3-level hierarchy: Stage -> persistentStage -> Stage for each source node...")
    print("-" * 50)
    
    # Step 3: Create the 3-level hierarchy for each existing node
    # Each source node gets: Stage -> persistentStage -> Stage
    results = []
    
    print(f"\nCreating 3-level hierarchy for {len(existing_node_ids)} source nodes...")
    print("Pattern: Source -> Stage -> persistentStage -> Stage")
    print("-" * 50)
    
    for i, source_node_id in enumerate(existing_node_ids, 1):
        print(f"\nProcessing source node {i}/{len(existing_node_ids)}: {source_node_id}")
        
        # Level 1: Create Stage node (child of source)
        print(f"  Creating Stage 1 for source {source_node_id}")
        stage1_config = {
            "nodeType": "Stage",
            "predecessorNodeIDs": [source_node_id],
            "name": f"Stage1 for {source_node_id}"
        }
        
        stage1_result = create_node(base_url, workspace_id, stage1_config, auth_token)
        results.append(stage1_result)
        
        if stage1_result['status_code'] not in [200, 201]:
            print(f"  ✗ Failed to create Stage 1 for source {source_node_id}")
            continue
            
        stage1_id = stage1_result.get('node_id')
        if not stage1_id:
            print(f"  ✗ No node ID returned for Stage 1")
            continue
            
        print(f"  ✓ Created Stage 1: {stage1_id}")
        
        # Level 2: Create persistentStage node (child of Stage 1)
        print(f"  Creating persistentStage for Stage 1 {stage1_id}")
        persistent_stage_config = {
            "nodeType": "persistentStage",
            "predecessorNodeIDs": [stage1_id],
            "name": f"PersistentStage for {stage1_id}"
        }
        
        persistent_stage_result = create_node(base_url, workspace_id, persistent_stage_config, auth_token)
        results.append(persistent_stage_result)
        
        if persistent_stage_result['status_code'] not in [200, 201]:
            print(f"  ✗ Failed to create persistentStage for Stage 1 {stage1_id}")
            continue
            
        persistent_stage_id = persistent_stage_result.get('node_id')
        if not persistent_stage_id:
            print(f"  ✗ No node ID returned for persistentStage")
            continue
            
        print(f"  ✓ Created persistentStage: {persistent_stage_id}")
        
        # Level 3: Create Stage 2 node (child of persistentStage)
        print(f"  Creating Stage 2 for persistentStage {persistent_stage_id}")
        stage2_config = {
            "nodeType": "Stage",
            "predecessorNodeIDs": [persistent_stage_id],
            "name": f"Stage2 for {persistent_stage_id}"
        }
        
        stage2_result = create_node(base_url, workspace_id, stage2_config, auth_token)
        results.append(stage2_result)
        
        if stage2_result['status_code'] in [200, 201]:
            stage2_id = stage2_result.get('node_id')
            print(f"  ✓ Created Stage 2: {stage2_id}")
            print(f"  ✓ Completed hierarchy for source {source_node_id}")
        else:
            print(f"  ✗ Failed to create Stage 2 for persistentStage {persistent_stage_id}")
        
        print("-" * 30)
    
    # Step 4: Display final summary
    print("-" * 50)
    successful = sum(1 for r in results if r['status_code'] in [200, 201])
    failed = len(results) - successful
    
    print(f"Summary: {successful} successful, {failed} failed out of {len(results)} total nodes")

# Entry point: Run the main function when script is executed directly
if __name__ == "__main__":
    main()