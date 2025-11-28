# Obric MCP Server MVP

An MCP (Model Context Protocol) server for runtime agents that provides graph analysis, computation, and detection capabilities on top of a Neo4j database.

## Overview

This MCP server is designed to be deployed on cloud infrastructure and serves enterprise LLMs and custom agents. It exposes various tools for analyzing, finding, computing, and detecting information from a Neo4j graph database.

## Project Structure

```
obric-mcp-server-mvp/
├── src/
│   └── obric_mcp_server/
│       ├── __init__.py
│       ├── mcp_server.py     # Main MCP server implementation
│       ├── config.py          # Configuration (Neo4j credentials, etc.)
│       ├── neo4j/             # Neo4j database client
│       │   ├── __init__.py
│       │   └── client.py      # Neo4j connection and query execution
│       └── tools/             # MCP tools (graph analysis functions)
│           └── __init__.py
├── scripts/                   # Deployment scripts
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Features

- MCP protocol implementation for runtime agents
- Neo4j graph database integration
- Graph analysis tools
- Cloud-ready deployment
- Enterprise-grade configuration management

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables (see `.env.example`)

## License

[Add your license here]

