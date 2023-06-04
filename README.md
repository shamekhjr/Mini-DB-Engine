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

## Multi-threading

The database engine has been designed to support multithreading. All the data structures used within the engine are thread-safe, allowing users to leverage multithreading capabilities when interacting with the database. This enables concurrent access to the database, enhancing performance in scenarios where multiple threads or processes are involved.

## Optimization

Every aspect of the codebase has been optimized for performance. Careful consideration has been given to optimizing various operations and algorithms to ensure speedy processing and retrieval of data.

## Contributors 
This project was the issue of a whole semester of tireless work, sleepless nights, dedication and passion of:
- [Mohamed Shamekh](https://github.com/shamekhjr)
- [Omar Nour](https://github.com/Omar-Nour)
- [Ali Abdalwahaab](https://github.com/AliAbdalwahaab)
- [Youssef El-Sharkawy](https://github.com/Shark23p4)
- [Mazen Soliman](https://github.com/MazenS0liman)
