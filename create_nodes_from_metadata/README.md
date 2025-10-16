# Coalesce Node Creation Script

This script allows you to create multiple Coalesce nodes from a JSON configuration file.

## Setup

1. **Set your authentication token:**
   ```bash
   export COALESCE_AUTH_TOKEN='your-coalesce-api-token-here'
   ```

2. **Configure your nodes in `nodes_config.json`:**
   ```json
   {
     "workspaceID": "your-workspace-id",
     "nodes": [
       {
         "nodeType": "SnowflakeTable",
         "predecessorNodeIDs": ["parent-node-1", "parent-node-2"],
         "name": "My Snowflake Table Node"
       },
       {
         "nodeType": "SnowflakeStage",
         "predecessorNodeIDs": ["parent-node-3"],
         "name": "My Snowflake Stage Node"
       },
       {
         "nodeType": "Python",
         "predecessorNodeIDs": [],
         "name": "My Python Node"
       }
     ]
   }
   ```

## Configuration File Format

- **workspaceID**: Your Coalesce workspace ID
- **nodes**: Array of node configurations
  - **nodeType**: The type of node to create (e.g., "SnowflakeTable", "SnowflakeStage", "Python")
  - **predecessorNodeIDs**: Array of parent node IDs (can be empty array for root nodes)
  - **name**: Optional name for the node (used for logging purposes)

## Usage

Run the script:
```bash
python create_nodes.py
```

The script will:
1. Load the configuration from `nodes_config.json`
2. Create each node in sequence
3. Display progress and results
4. Show a summary of successful/failed creations

## Error Handling

The script includes comprehensive error handling for:
- Missing configuration file
- Invalid JSON format
- Missing workspace ID
- Missing authentication token
- API request failures
- Network errors

## Example Output

```
Creating 3 nodes in workspace abc123...
--------------------------------------------------
Creating node 1/3: My Snowflake Table Node
✓ Successfully created node: My Snowflake Table Node
Creating node 2/3: My Snowflake Stage Node
✓ Successfully created node: My Snowflake Stage Node
Creating node 3/3: My Python Node
✓ Successfully created node: My Python Node
--------------------------------------------------
Summary: 3 successful, 0 failed out of 3 total nodes
```
