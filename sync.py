"""PostgreSQL → Notion 주기적 동기화 스크립트.

마지막 동기화 시점 이후에 생성된 행만 Notion 데이터베이스에 추가한다.
마지막 동기화 시각은 last_synced.txt 파일에 저장한다.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from notion_client import Client

load_dotenv()

# ── 환경변수 ──────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]
NOTION_API_KEY = os.environ["NOTION_API_KEY"]
NOTION_PARENT_PAGE_ID = os.environ["NOTION_PARENT_PAGE_ID"]
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "")
SYNC_TABLE_NAME = os.environ["SYNC_TABLE_NAME"]
SYNC_TIMESTAMP_COLUMN = os.environ.get("SYNC_TIMESTAMP_COLUMN", "created_at")
SYNC_BATCH_LIMIT = int(os.environ.get("SYNC_BATCH_LIMIT", "100"))

LAST_SYNCED_FILE = Path(__file__).parent / "last_synced.txt"

notion = Client(auth=NOTION_API_KEY)

# ── PostgreSQL 컬럼 → Notion 프로퍼티 타입 매핑 ──────────
PG_TYPE_MAP = {
    "integer": "number",
    "bigint": "number",
    "smallint": "number",
    "numeric": "number",
    "real": "number",
    "double precision": "number",
    "boolean": "checkbox",
    "timestamp without time zone": "date",
    "timestamp with time zone": "date",
    "date": "date",
    "text": "rich_text",
    "character varying": "rich_text",
    "character": "rich_text",
    "uuid": "rich_text",
    "json": "rich_text",
    "jsonb": "rich_text",
}


def get_pg_columns(conn, table_name: str) -> list[dict]:
    """information_schema에서 테이블 컬럼 정보를 조회한다."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return cur.fetchall()


def notion_prop_type(pg_data_type: str) -> str:
    return PG_TYPE_MAP.get(pg_data_type, "rich_text")


def build_notion_properties(columns: list[dict]) -> dict:
    """PostgreSQL 컬럼 목록으로부터 Notion DB 프로퍼티 스키마를 생성한다."""
    properties = {}
    for col in columns:
        name = col["column_name"]
        ntype = notion_prop_type(col["data_type"])
        if ntype == "number":
            properties[name] = {"number": {}}
        elif ntype == "checkbox":
            properties[name] = {"checkbox": {}}
        elif ntype == "date":
            properties[name] = {"date": {}}
        else:
            properties[name] = {"rich_text": {}}
    return properties


def create_notion_database(columns: list[dict]) -> str:
    """Notion 데이터베이스를 생성하고 ID를 반환한다."""
    properties = build_notion_properties(columns)
    title_col = columns[0]["column_name"]
    properties[title_col] = {"title": {}}

    # 2025-09-03 API: properties는 initial_data_source 안에 넣어야 함
    db = notion.databases.create(
        parent={"type": "page_id", "page_id": NOTION_PARENT_PAGE_ID},
        title=[{"type": "text", "text": {"content": f"sync: {SYNC_TABLE_NAME}"}}],
        initial_data_source={"properties": properties},
    )
    db_id = db["id"]
    print(f"Notion DB 생성 완료: {db_id}")
    return db_id


def get_title_property_name(database_id: str) -> str:
    """기존 Notion DB의 title 프로퍼티명을 찾는다."""
    db = notion.databases.retrieve(database_id=database_id)
    ds_id = db["data_sources"][0]["id"]
    ds = notion.data_sources.retrieve(data_source_id=ds_id)
    for name, prop in ds["properties"].items():
        if prop["type"] == "title":
            return name
    return "Name"


def ensure_notion_properties(database_id: str, columns: list[dict]) -> None:
    """기존 Notion DB에 누락된 프로퍼티를 추가한다."""
    db = notion.databases.retrieve(database_id=database_id)
    ds_id = db["data_sources"][0]["id"]
    ds = notion.data_sources.retrieve(data_source_id=ds_id)
    existing = set(ds["properties"].keys())
    title_name = get_title_property_name(database_id)

    missing = {}
    for col in columns:
        name = col["column_name"]
        # title 프로퍼티와 같은 이름이거나 이미 존재하면 스킵
        if name in existing or name == title_name:
            continue
        ntype = notion_prop_type(col["data_type"])
        if ntype == "number":
            missing[name] = {"number": {}}
        elif ntype == "checkbox":
            missing[name] = {"checkbox": {}}
        elif ntype == "date":
            missing[name] = {"date": {}}
        else:
            missing[name] = {"rich_text": {}}

    if missing:
        notion.data_sources.update(data_source_id=ds_id, properties=missing)
        print(f"프로퍼티 추가: {list(missing.keys())}")


def get_or_create_database(columns: list[dict]) -> str:
    """NOTION_DATABASE_ID가 있으면 그대로 사용, 없으면 새로 생성한다."""
    if NOTION_DATABASE_ID:
        print(f"기존 Notion DB 사용: {NOTION_DATABASE_ID}")
        ensure_notion_properties(NOTION_DATABASE_ID, columns)
        return NOTION_DATABASE_ID
    return create_notion_database(columns)


# ── last_synced.txt 파일 관리 ─────────────────────────────
def get_last_synced_at() -> datetime | None:
    """파일에서 마지막 동기화 시각을 읽는다."""
    if not LAST_SYNCED_FILE.exists():
        return None
    text = LAST_SYNCED_FILE.read_text().strip()
    if not text:
        return None
    return datetime.fromisoformat(text)


def save_last_synced_at(ts: datetime) -> None:
    """마지막 동기화 시각을 파일에 저장한다."""
    LAST_SYNCED_FILE.write_text(ts.isoformat())
    print(f"last_synced_at 저장: {ts.isoformat()}")


# ── PostgreSQL → Notion 행 변환 ───────────────────────────
def row_to_notion_properties(
    row: dict, columns: list[dict], title_col: str, title_source_col: str
) -> dict:
    """PostgreSQL 행 1개를 Notion 프로퍼티 dict로 변환한다."""
    props: dict = {}
    # title 프로퍼티에 첫 번째 PG 컬럼 값 매핑
    title_value = row.get(title_source_col)
    props[title_col] = {
        "title": [{"text": {"content": str(title_value) if title_value is not None else ""}}]
    }
    for col in columns:
        name = col["column_name"]
        if name == title_source_col:
            continue  # title로 이미 처리됨
        ntype = notion_prop_type(col["data_type"])
        value = row.get(name)

        if ntype == "number":
            props[name] = {"number": float(value) if value is not None else None}
        elif ntype == "checkbox":
            props[name] = {"checkbox": bool(value) if value is not None else False}
        elif ntype == "date":
            if value is not None:
                if isinstance(value, datetime):
                    props[name] = {"date": {"start": value.isoformat()}}
                else:
                    props[name] = {"date": {"start": str(value)}}
            else:
                props[name] = {"date": None}
        else:
            text = str(value) if value is not None else ""
            # Notion rich_text 최대 2000자
            if len(text) > 2000:
                text = text[:2000]
            props[name] = {"rich_text": [{"text": {"content": text}}]}
    return props


# ── 메인 동기화 ───────────────────────────────────────────
def sync() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        columns = get_pg_columns(conn, SYNC_TABLE_NAME)
        if not columns:
            print(f"테이블 '{SYNC_TABLE_NAME}'의 컬럼 정보를 찾을 수 없습니다.")
            sys.exit(1)

        database_id = get_or_create_database(columns)
        # 기존 DB면 title 프로퍼티명 감지, 새 DB면 첫 컬럼
        if NOTION_DATABASE_ID:
            title_col = get_title_property_name(database_id)
        else:
            title_col = columns[0]["column_name"]
        last_synced = get_last_synced_at()

        if last_synced:
            print(f"마지막 동기화 시각: {last_synced.isoformat()}")
        else:
            print("첫 실행 — 전체 데이터 동기화")

        # PostgreSQL에서 신규 행 조회
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if last_synced:
                cur.execute(
                    f'SELECT * FROM "{SYNC_TABLE_NAME}" '
                    f'WHERE "{SYNC_TIMESTAMP_COLUMN}" > %s '
                    f'ORDER BY "{SYNC_TIMESTAMP_COLUMN}" ASC '
                    f'LIMIT %s',
                    (last_synced, SYNC_BATCH_LIMIT),
                )
            else:
                cur.execute(
                    f'SELECT * FROM "{SYNC_TABLE_NAME}" '
                    f'ORDER BY "{SYNC_TIMESTAMP_COLUMN}" ASC '
                    f'LIMIT %s',
                    (SYNC_BATCH_LIMIT,),
                )
            rows = cur.fetchall()

        print(f"동기화 대상: {len(rows)}행")

        if not rows:
            print("새로운 데이터 없음. 종료.")
            return

        # Notion에 행 추가
        for i, row in enumerate(rows, 1):
            props = row_to_notion_properties(row, columns, title_col, columns[0]["column_name"])
            notion.pages.create(
                parent={"database_id": database_id},
                properties=props,
            )
            if i % 10 == 0:
                print(f"  {i}/{len(rows)} 완료")
            time.sleep(0.35)  # Notion API rate limit 대응

        now = datetime.now(timezone.utc)
        save_last_synced_at(now)
        print(f"동기화 완료: {len(rows)}행 추가됨")
    finally:
        conn.close()


if __name__ == "__main__":
    sync()
