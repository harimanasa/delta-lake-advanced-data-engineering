# Advanced Data Engineering with Delta Lake

This project demonstrates advanced data engineering techniques using Delta Lake, showcasing data partitioning strategies, structured streaming, Change Data Feed (CDF), Medallion Architecture, handling invalid records, and managing Slowly Changing Dimensions (SCDs) with Spark.

## Project Structure

- **Partitioning Strategies**: 
  - Traditional partitioning (by date)
  - Liquid partitioning with Z-order and Bloom filters for optimized querying.
  
- **Structured Streaming with CDF**:
  - Uses Delta Lake's Change Data Feed (CDF) for handling real-time data changes, allowing downstream consumers to process updates.

- **Medallion Architecture**:
  - Implements Bronze, Silver, and Gold layers for data ingestion and processing, ensuring data quality and efficient query performance.
  
- **Handling Invalid Records**:
  - Validates records and marks invalid entries, either processing them separately or using partitioned storage.

- **Slowly Changing Dimensions (SCDs)**:
  - Manages historical data changes using SCD Type 2, ensuring historical accuracy.

## Getting Started

### Prerequisites

- Java (JDK 8 or above)
- Apache Spark with Delta Lake support
- Git

### Running the Project

1. **Clone the Repository**:
   ```bash
   git clone <repo-url>
   cd delta-lake-advanced-data-engineering

2. **Build and Run:**

Compile the project and run AdvancedDataEngineeringWithDeltaLake class from your IDE or command line.

3. **Directory Setup:**

Ensure that paths such as /tmp/delta/partitioned_data exist or update them to your preferred location.

**Key Code Modules**
partitioningStrategies: Implements various partitioning techniques.
structuredStreamingWithCDF: Demonstrates real-time data ingestion with CDF.
medallionArchitecture: Configures Bronze, Silver, and Gold layers.
handleInvalidRecords: Provides methods for handling and validating invalid records.
manageSCDs: Manages SCD Type 2 for maintaining historical data accuracy.

**Project Setup**
Spark Session: Configures Spark with Delta Lake extensions.
Data Sources: Ingests data from sample JSON and Delta Lake tables.
Checkpointing: Uses checkpoints for streaming operations to ensure fault tolerance.
