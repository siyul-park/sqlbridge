# SQLBridge

> 🚀 **모든 데이터를 하나로 연결하는 분산 SQL 쿼리 엔진**

**SQLBridge**는 관계형 DB, NoSQL, 외부 API 등을 **표준 SQL**로 통합하여 **유연하고 고성능**의 데이터 처리 환경을 제공합니다. 다양한 데이터 소스를 하나의 SQL 인터페이스로 연결해 분석 파이프라인 구축, 시스템 간 조인, 내부 API SQL 노출 등을 간편하게 처리할 수 있습니다.

## 🔥 주요 기능

- **분산 SQL 실행**: 여러 데이터 소스에서 SQL 쿼리를 병렬로 실행하여 **확장성과 안정성** 제공
- **범용 데이터 통합**: 다양한 소스(PostgreSQL, MongoDB, REST API 등)와 연결
- **표준 SQL 드라이버**: 표준 `database/sql` 드라이버 형식으로 기존 애플리케이션과 쉽게 통합
- **플러그인 아키텍처**: 새로운 백엔드 추가 및 쿼리 로직 커스터마이징 가능
- **데이터 가상화**: 데이터 소스 복잡성을 추상화하여 통합된 뷰 제공

## 🧩 활용 사례

- PostgreSQL과 MongoDB를 하나의 SQL 쿼리로 조인
- REST API와 DB 데이터를 결합하여 분석 대시보드 구축
- 내부 데이터를 SQL로 탐색 및 변환
- Go 애플리케이션 내 데이터 워크플로우 실행

## 🚀 빠른 시작

드라이버를 표준 `sql` 패키지에 등록하여 간편하게 사용할 수 있습니다:

```go
drv := driver.New(driver.WithRegistry(registry))
sql.Register("sqlbridge", drv)
```

이후, SQL 쿼리를 실행합니다:

```go
conn, _ := sql.Open("sqlbridge", "source")
rows, _ := conn.QueryContext(ctx, "SELECT * FROM `users` WHERE id = ?", id)
```

## 🔗 통합

다양한 시스템을 SQL로 통합하려면 아래 인터페이스를 구현합니다:

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