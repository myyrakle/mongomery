# mongomery

MongoDB 전체 컬렉션(컬렉션 생성 옵션, 인덱스, 데이터)을 다른 MongoDB로 1회 스냅샷 복제하는 CLI입니다.

## 동작 방식

실행 모드는 3개로 분리되어 있으며, 원하는 단계만 개별 실행할 수 있습니다.

1. `verify`: 연결 체크 + 버전 확인
2. `schema`: 컬렉션 목록/옵션/인덱스만 복제
3. `copy`: `_id` 커서 기반 문서 복제 + 중단 지점 재개

`copy`는 스키마 단계가 완료되어 있는 대상 DB를 기준으로 동작합니다. 독립 실행할 경우 먼저 `schema`를 완료하세요.

명령을 지정하지 않으면 기본 모드로 `verify → schema → copy`를 한 번에 수행합니다.

CDC는 지원하지 않으며, 한 번 전체를 돌고 종료합니다.

## 재시작(Resume)

진행 상태는 타겟 DB의 메타 컬렉션에 저장됩니다.

- `<meta-prefix>_jobs`: 작업 단위 상태
- `<meta-prefix>_collections`: 컬렉션 단위 체크포인트

체크포인트에는 다음이 저장됩니다.

- `status` (`pending`, `copying`, `done`)
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
    "read_preference": "primary",
    "compressors": ["zstd", "snappy", "zlib"],
    "zlib_compression_level": 6
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
- `srv` (선택): host가 Atlas-style SRV 클러스터명인 경우 `mongodb+srv://`로 연결 (`false` 기본)
- `database` (필수): DB 이름. `full_uri`에 DB path가 있으면 생략해도 자동 추출됨.
- `username`: 사용자명
- `password`: 비밀번호
- `kind`: `auto` | `standalone` | `replica_set` | `documentdb` (기본 `auto`)
- `replica_set`: replica set 이름 오버라이드
- `direct_connection`: 단일 노드 직접 연결 여부  
  `full_uri`가 `mongodb+srv://`인 경우에는 `false`여야 하며, `standalone`에서 기본값은 `false`로 보정됩니다.
- `srv`: `true`면 `host`를 `mongodb+srv://`로 해석
- `retry_writes`: retryWrites 설정 오버라이드
- `read_preference`: `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred`, `nearest`
- `compressors`: 네트워크 압축 우선순위 목록 (`snappy`, `zlib`, `zstd`)
- `zlib_compression_level`: zlib 레벨 (`-1`~`9`, `compressors`에 `zlib` 필요)
- `zstd_compression_level`: zstd 레벨 (`-131072`~`22`, `compressors`에 `zstd` 필요)
- `app_name`: Mongo app name
- `auth_source`: auth DB 오버라이드
- `tls`: TLS 사용 여부
- `tls_ca_file`: CA PEM 파일 경로
- `tls_insecure_skip_verify`: TLS 인증서 검증 skip (운영 비권장)
- `connect_timeout_ms`, `server_selection_timeout_ms`, `socket_timeout_ms`
- `max_pool_size`, `min_pool_size`

`kind`별 자동 기본값(명시값 우선):

- `standalone`: 기본적으로 `direct_connection=true` (단, `mongodb+srv://` 사용 시 `false`로 보정)
- `replica_set`: `direct_connection=false`
- `documentdb`: `direct_connection=false`, `tls=true`, `retry_writes=false`, `replica_set=rs0`, `read_preference=secondaryPreferred`

`full_uri`에 `mongodb+srv://`를 사용할 때 `direct_connection=true`는 지원되지 않습니다.

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
# 기본: verify → schema → copy
go run . --config ./config.json

# 단계별 실행(위치 인자)
go run . --config ./config.json verify
go run . --config ./config.json schema
go run . --config ./config.json copy

# 단계별 실행(명시 플래그)
go run . --config ./config.json --command verify
go run . --config ./config.json --command schema
go run . --config ./config.json --command copy
```

### 모드 정리

- `verify`: 소스/타겟 ping + 버전 출력
- `schema`: 현재 소스 DB의 컬렉션/옵션/인덱스를 대상 DB에 복제
- `copy`: 데이터 복사 실행 (재개 가능, `_id` 기반)

## 테스트용 Target Mongo 실행

- 대상 DB만 빠르게 띄우기:

```bash
docker compose -f docker-compose.target-test.yml up -d
```

- 종료:

```bash
docker compose -f docker-compose.target-test.yml down -v
```

- 테스트용 타겟 URI 예시:

`mongodb://admin:admin-pass@127.0.0.1:27018/app_migrated?authSource=admin`

## 로그

컬렉션별로 `N%` 단위(`log_percent_step`) 진행률을 출력합니다.

예시:

- `collection=users progress=25% (250000/1000000)`
- `collection=orders progress=100% (503221/503221)`

## 제한 사항

- `_id` 기준 정렬/필터가 가능한 컬렉션을 전제로 합니다.
- 타겟에 같은 `_id`가 이미 있으면 중복키 오류(11000)는 무시하고 계속 진행합니다.
- 소스와 타겟이 완전히 같은 DB(host+database)는 허용하지 않습니다.
