# mongomery

MongoDB 전체 컬렉션(컬렉션 생성 옵션, 인덱스, 데이터)을 다른 MongoDB로 1회 스냅샷 복제하는 CLI입니다.

## 동작 방식

아래 순서로 실행합니다.

1. 소스 DB의 컬렉션 목록/옵션/문서 수를 조회
2. 타겟 DB에 컬렉션 생성(없을 때만)
3. 타겟 DB에 인덱스 생성
4. `_id` 오름차순 커서 기반으로 배치 복제

CDC는 지원하지 않으며, 한 번 전체를 돌고 종료합니다.

## 재시작(Resume)

진행 상태는 타겟 DB의 메타 컬렉션에 저장됩니다.

- `<meta-prefix>_jobs`: 작업 단위 상태
- `<meta-prefix>_collections`: 컬렉션 단위 체크포인트

체크포인트에는 다음이 저장됩니다.

- `status` (`pending`, `copying`, `done`)
- `initialized`, `indexes_cloned`
- `last_id` (`_id` 커서)
- `copied_docs`, `total_docs`
- `last_logged_percent`

같은 `job_id`로 다시 실행하면 중단 지점부터 이어서 진행합니다.

## 설정 파일(JSON)

실행 설정은 JSON 파일로 전달합니다.

```json
{
  "source": {
    "host": "source-host-1:27017",
    "username": "myuser",
    "password": "mypassword",
    "database": "app",
    "kind": "replica_set",
    "read_preference": "primary"
  },
  "target": {
    "host": "target-host:27017",
    "username": "myuser",
    "password": "target-password",
    "database": "app_migrated",
    "kind": "standalone",
    "direct_connection": true
  },
  "job_id": "snapshot-20260306",
  "meta_prefix": "__mongomery",
  "batch_size": 1000,
  "log_percent_step": 5
}
```

### 공통 필드

- `source`, `target`: 각 Mongo 연결 설정
- `job_id`: 재시작 기준 작업 ID (기본 `default`)
- `meta_prefix`: 메타 컬렉션 prefix (기본 `__mongomery`)
- `batch_size`: 데이터 복제 배치 크기 (기본 `1000`)
- `log_percent_step`: 퍼센트 로그 간격 (기본 `5`)

### source/target 연결 필드

- `full_uri` (선택): 전체 MongoDB 연결 URI를 그대로 사용할 때 사용 (`mongodb://...`, `mongodb+srv://...`)
- `host` (선택): 단일 엔드포인트 또는 CSV 형태 다중 엔드포인트 (`host:27017` 또는 `host1:27017,host2:27017`)  
  `full_uri`가 비어 있으면 필수
- `database` (필수): DB 이름
- `username`: 사용자명
- `password`: 비밀번호
- `kind`: `auto` | `standalone` | `replica_set` | `documentdb` (기본 `auto`)
- `replica_set`: replica set 이름 오버라이드
- `direct_connection`: 단일 노드 직접 연결 여부
- `retry_writes`: retryWrites 설정 오버라이드
- `read_preference`: `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred`, `nearest`
- `app_name`: Mongo app name
- `auth_source`: auth DB 오버라이드
- `tls`: TLS 사용 여부
- `tls_ca_file`: CA PEM 파일 경로
- `tls_insecure_skip_verify`: TLS 인증서 검증 skip (운영 비권장)
- `connect_timeout_ms`, `server_selection_timeout_ms`, `socket_timeout_ms`
- `max_pool_size`, `min_pool_size`

`kind`별 자동 기본값(명시값 우선):

- `standalone`: `direct_connection=true`
- `replica_set`: `direct_connection=false`
- `documentdb`: `direct_connection=false`, `tls=true`, `retry_writes=false`, `replica_set=rs0`, `read_preference=secondaryPreferred`

### DocumentDB 기본값

`kind: "documentdb"`를 지정하면 아래 기본값이 자동 적용됩니다(명시값 우선).

- `tls = true`
- `retry_writes = false`
- `replica_set = "rs0"`
- `read_preference = "secondaryPreferred"`

### 주석 달린 템플릿(JSONC)

JSON 표준은 주석을 지원하지 않기 때문에, 필드별 상세 코멘트는 별도 JSONC 파일에 제공합니다.

- 상세 코멘트 템플릿: `config.annotated.example.jsonc`
- 실행용 순수 JSON 예시: `config.example.json`, `config.documentdb.example.json`

예시 파일:

- 일반/replica+standalone: `config.example.json`
- DocumentDB 포함 예시: `config.documentdb.example.json`

## 실행

```bash
go run . --config ./config.json
```

## 로그

컬렉션별로 `N%` 단위(`log_percent_step`) 진행률을 출력합니다.

예시:

- `collection=users progress=25% (250000/1000000)`
- `collection=orders progress=100% (503221/503221)`

## 제한 사항

- `_id` 기준 정렬/필터가 가능한 컬렉션을 전제로 합니다.
- 타겟에 같은 `_id`가 이미 있으면 중복키 오류(11000)는 무시하고 계속 진행합니다.
- 소스와 타겟이 완전히 같은 DB(host+database)는 허용하지 않습니다.
