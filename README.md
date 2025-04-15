# SQLBridge

> 🚀 **A distributed SQL query engine for unified access to all your data**

**SQLBridge** integrates relational databases, NoSQL, external APIs, and more into a **standard SQL** interface, providing a **flexible and high-performance** data processing environment. It allows seamless data integration, enabling tasks like building analytical pipelines, performing cross-system joins, and exposing internal APIs as SQL queries.

## 🔥 Key Features

- **Distributed SQL Execution**: Executes SQL queries across multiple data sources in parallel, offering **scalability and reliability**.
- **Universal Data Integration**: Connects to various sources (PostgreSQL, MongoDB, REST APIs, etc.).
- **Standard SQL Driver**: Available as a standard `database/sql` driver, easily integrable with existing applications.
- **Plugin Architecture**: Easily add new backends or customize query logic.
- **Data Virtualization**: Abstracts the complexity of data sources to provide unified virtual views.

## 🧩 Use Cases

- Join PostgreSQL and MongoDB data with a single SQL query
- Combine REST API and database data to build analytical dashboards
- Explore and transform internal data via SQL
- Execute data workflows within Go applications

## 🚀 Quick Start

Register the driver with the standard `sql` package for easy usage:

```go
drv := driver.New(driver.WithRegistry(registry))
sql.Register("sqlbridge", drv)
```

Then, run SQL queries like this:

```go
conn, _ := sql.Open("sqlbridge", "source")
rows, _ := conn.QueryContext(ctx, "SELECT * FROM `users` WHERE id = ?", id)
```

## 🔗 Integration

To integrate various systems into SQL, implement the following interfaces:

```go
type Registry interface {
    Catalog(name string) (Catalog, error)
}

type Catalog interface {
    Table(name string) (Table, error)
}

type Table interface {
    Scan(ctx context.Context) (Cursor, error)
}
```