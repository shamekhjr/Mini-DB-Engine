# Database Engine

This is a database engine that supports various operations for table management and data manipulation. It was built with a focus on performance, ensuring fast processing and retrieval of data. The engine provides the following features:

## Features

- Table creation with a primary key column
- Sorted inserts based on the primary key
- Updates of existing records
- Select queries for retrieving data
- Deletion of records

## Indexing

The engine supports two types of indices:

1. **BRIN Index** (Default): The Block Range Index (BRIN) is the default index used by the engine. It provides efficient indexing based on block ranges, suitable for large datasets.

2. **Octree Index** (Optional): The Octree Index is an optional indexing mechanism available upon request. It offers specialized indexing capabilities for specific use cases.

## Storage

The tables within the database engine are stored as pages, which correspond to serializable files on secondary storage. This storage design allows for efficient data management and retrieval.

## Optimization

Every aspect of the codebase has been optimized for performance. Careful consideration has been given to optimizing various operations and algorithms to ensure speedy processing and retrieval of data.
