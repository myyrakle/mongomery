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

같은 `--job-id`로 다시 실행하면 중단 지점부터 이어서 진행합니다.

## 설정 파일(JSON)

실행 설정은 JSON 파일로 전달합니다.

```json
{
  "source_uri": "mongodb://source-host:27017",
  "target_uri": "mongodb://target-host:27017",
  "source_db": "app",
  "target_db": "app_migrated",
  "job_id": "snapshot-20260306",
  "meta_prefix": "__mongomery",
  "batch_size": 1000,
  "log_percent_step": 5
}
```

필드 설명:

- `source_uri`: 소스 MongoDB URI (필수)
- `target_uri`: 타겟 MongoDB URI (필수)
- `source_db`: 소스 DB 이름 (필수)
- `target_db`: 타겟 DB 이름 (필수)
- `job_id`: 재시작 기준 작업 ID (기본 `default`)
- `meta_prefix`: 메타 컬렉션 prefix (기본 `__mongomery`)
- `batch_size`: 데이터 복제 배치 크기 (기본 `1000`)
- `log_percent_step`: 퍼센트 로그 간격 (기본 `5`)

## 실행

```bash
go run . --config ./config.json
```

## 로그

컬렉션별로 `N%` 단위(`--log-percent-step`) 진행률을 출력합니다.

예시:

- `collection=users progress=25% (250000/1000000)`
- `collection=orders progress=100% (503221/503221)`

## 제한 사항

- `_id` 기준 정렬/필터가 가능한 컬렉션을 전제로 합니다.
- 타겟에 같은 `_id`가 이미 있으면 중복키 오류(11000)는 무시하고 계속 진행합니다.
- 소스와 타겟이 완전히 같은 DB(URI+DB)는 허용하지 않습니다.
