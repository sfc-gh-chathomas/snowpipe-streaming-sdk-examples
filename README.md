# Snowpipe Streaming SDK Example

This repository contains examples demonstrating how to use the Snowpipe Streaming SDK to ingest data into Snowflake in real-time.

## Overview

The Snowpipe Streaming SDK enables applications to stream data directly into Snowflake tables with low latency and high throughput. This repository provides practical examples to help you get started with the SDK quickly.

The language examples use **Elastic Channels**: one implicit, Snowflake-managed
channel per pipe, with concurrent producers and durable acknowledgements.

## Three Learning Levels

1. **Quickstart:** ten pipelined single-row appends, acknowledgement, and cleanup.
2. **Continuous ingestion:** bounded Futures/Promises, backpressure, in-process client recreation, and shutdown drain.
3. **Callback integration:** the same operational scope using lightweight callback handoff. This is an alternative API style, not a stronger delivery guarantee.

Each SDK file is self-contained. Start with Level 1, then adapt Level 2 for a realistic PoC.
Retain events outside SDK memory until confirmed. Persistent source checkpoints and crash replay
are not supplied; that advanced Level 4 recipe is deferred. See each language README for limits.

## Choosing SDK vs. REST

- **SDK (Java, Python, Node.js)** — recommended for most applications. Higher throughput and simpler error handling than calling the REST API directly.
- **REST API** — use for lightweight, language-agnostic, or infrastructure-constrained integrations where adding the SDK isn't practical.

## Examples

This repository contains complete, runnable examples in multiple languages:

### [Java Example](./java-example)
A complete Maven project demonstrating the Snowpipe Streaming SDK in Java. Includes:
- Quickstart, continuous Futures ingestion, and callback integration
- Maven build configuration with all required dependencies
- Full example code with proper error handling
- Comprehensive setup instructions
- Sample configuration files
- **[Monitoring & Abort](./java-example/monitoring)** — Monitor channel status, track offset lag, inject errors, and abort on error increase

### [Python Example](./python-example)
A complete Python project demonstrating the Snowpipe Streaming SDK in Python. Includes:
- Quickstart, continuous Futures ingestion, and callback integration
- Requirements file with all necessary packages
- Clean, well-documented example code
- Setup instructions with virtual environment
- Sample configuration files
- **[Monitoring & Abort](./python-example/monitoring)** — Monitor channel status, track offset lag, inject errors, abort on error increase, and optional live matplotlib plotting

### [Node.js Example](./nodejs-example)
A complete Node.js project demonstrating the Snowpipe Streaming SDK in Node.js. Includes:
- Quickstart, continuous Promise ingestion, and callback integration
- npm package configuration with all required dependencies
- Clean, well-documented example code
- Setup instructions
- Sample configuration files

## Getting Started

1. Choose your preferred language (Java, Python, or Node.js)
2. Navigate to the respective example directory
3. Follow the README instructions in that directory to:
   - Set up your Snowflake table and pipe
   - Configure authentication
   - Install dependencies
   - Run the example

## Important Notes

**SDK version**: All examples require `snowpipe-streaming` **1.8.0** or later. The version numbers in the dependency files (`pom.xml`, `package.json`, `requirements.txt`) reflect the minimum tested version. Pin to the latest published SDK version in production deployments.

## License

This project is licensed under the CC BY 4.0 license.
