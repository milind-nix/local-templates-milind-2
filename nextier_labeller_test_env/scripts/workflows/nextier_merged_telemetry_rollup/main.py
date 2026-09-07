from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import (
    get_state,
    query_store,
    set_state,
    write_datastore,
)


SOURCE_DATASTORE_KEY = "merged_fleet_stream_customer_full_v2"
ROLLUP_DATASTORE_KEY = "merged_fleet_stream_customer_rollup_v1"
CHECKPOINT_KEY = "__checkpoint__merged_telemetry_rollup_end_ts"
BACKFILL_CURSOR_PREFIX = "__checkpoint__merged_telemetry_rollup_backfill_cursor"


def _parse_time(value: str | None) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return None
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.to_pydatetime()


def _format_time(value: datetime | pd.Timestamp | Any) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")


def _state_key_part(value: str | None) -> str:
    if value is None or str(value).strip() == "":
        return "ALL"
    return "".join(char if char.isalnum() else "_" for char in str(value).strip())[:80]


def _backfill_cursor_key(
    bucket_seconds: int,
    fleet_name: str | None,
    pad_name: str | None,
    well_name: str | None,
) -> str:
    return "__".join(
        [
            BACKFILL_CURSOR_PREFIX,
            f"bucket_{int(bucket_seconds)}",
            f"fleet_{_state_key_part(fleet_name)}",
            f"pad_{_state_key_part(pad_name)}",
            f"well_{_state_key_part(well_name)}",
        ],
    )


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _window_chunks(
    start: datetime,
    end: datetime,
    chunk_hours: int,
    max_chunks: int,
) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    step = timedelta(hours=max(int(chunk_hours), 1))
    while cursor < end and len(chunks) < max(int(max_chunks), 1):
        next_end = min(cursor + step, end)
        chunks.append((cursor, next_end))
        cursor = next_end
    return chunks


async def _compute_rollup_chunk(
    workspace_id: int,
    start_ts: datetime,
    end_ts: datetime,
    bucket_seconds: int,
    rollup_ts: str,
    fleet_name: str | None,
    pad_name: str | None,
    well_name: str | None,
):
    params: dict[str, Any] = {
        "start_ts": _format_time(start_ts),
        "end_ts": _format_time(end_ts),
        "bucket_seconds": int(bucket_seconds),
        "rollup_ts": rollup_ts,
        "fleet_name": fleet_name or "",
        "pad_name": pad_name or "",
        "well_name": well_name or "",
    }

    return await query_store(
        sql=f"""
            WITH filtered AS (
              SELECT
                COALESCE(NULLIF(fleet_name, ''), 'Unknown') AS fleet_name,
                COALESCE(NULLIF(pad_name, ''), 'Unknown') AS pad_name,
                name,
                id,
                api_num,
                record_ts,
                created_ts,
                rate_slurry,
                press_mainline,
                prop_conc_blend_denso,
                prop_conc_inline,
                prop_conc_target,
                prop_conc_blend_auger,
                DATE_BIN(
                  (:bucket_seconds || ' seconds')::interval,
                  CAST(record_ts AS timestamp),
                  TIMESTAMP '1970-01-01 00:00:00'
                ) AS bucket_ts
              FROM datastore:{SOURCE_DATASTORE_KEY}
              WHERE
                record_ts IS NOT NULL
                AND name IS NOT NULL
                AND CAST(record_ts AS timestamp) >= CAST(:start_ts AS timestamp)
                AND CAST(record_ts AS timestamp) < CAST(:end_ts AS timestamp)
                AND (:fleet_name = '' OR COALESCE(NULLIF(fleet_name, ''), 'Unknown') = :fleet_name)
                AND (:pad_name = '' OR COALESCE(NULLIF(pad_name, ''), 'Unknown') = :pad_name)
                AND (:well_name = '' OR name = :well_name)
            ),
            ranked AS (
              SELECT
                *,
                ROW_NUMBER() OVER (
                  PARTITION BY fleet_name, pad_name, name, bucket_ts
                  ORDER BY CAST(record_ts AS timestamp) ASC, CAST(created_ts AS timestamp) ASC NULLS LAST
                ) AS rn_first,
                ROW_NUMBER() OVER (
                  PARTITION BY fleet_name, pad_name, name, bucket_ts
                  ORDER BY CAST(record_ts AS timestamp) DESC, CAST(created_ts AS timestamp) DESC NULLS LAST
                ) AS rn_last
              FROM filtered
            )
            SELECT
              :bucket_seconds AS bucket_seconds,
              bucket_ts,
              fleet_name,
              pad_name,
              name,
              MAX(id) FILTER (WHERE rn_last = 1) AS id,
              MAX(api_num) FILTER (WHERE rn_last = 1) AS api_num,
              MIN(CAST(record_ts AS timestamp)) AS first_record_ts,
              MAX(CAST(record_ts AS timestamp)) AS last_record_ts,
              MIN(CAST(created_ts AS timestamp)) AS first_created_ts,
              MAX(CAST(created_ts AS timestamp)) AS last_created_ts,
              COUNT(*) AS records_in_bucket,
              AVG(rate_slurry) AS rate_slurry_avg,
              MIN(rate_slurry) AS rate_slurry_min,
              MAX(rate_slurry) AS rate_slurry_max,
              MAX(rate_slurry) FILTER (WHERE rn_first = 1) AS rate_slurry_first,
              MAX(rate_slurry) FILTER (WHERE rn_last = 1) AS rate_slurry_last,
              AVG(press_mainline) AS press_mainline_avg,
              MIN(press_mainline) AS press_mainline_min,
              MAX(press_mainline) AS press_mainline_max,
              MAX(press_mainline) FILTER (WHERE rn_first = 1) AS press_mainline_first,
              MAX(press_mainline) FILTER (WHERE rn_last = 1) AS press_mainline_last,
              AVG(prop_conc_blend_denso) AS prop_conc_blend_denso_avg,
              MIN(prop_conc_blend_denso) AS prop_conc_blend_denso_min,
              MAX(prop_conc_blend_denso) AS prop_conc_blend_denso_max,
              MAX(prop_conc_blend_denso) FILTER (WHERE rn_first = 1) AS prop_conc_blend_denso_first,
              MAX(prop_conc_blend_denso) FILTER (WHERE rn_last = 1) AS prop_conc_blend_denso_last,
              AVG(prop_conc_inline) AS prop_conc_inline_avg,
              MIN(prop_conc_inline) AS prop_conc_inline_min,
              MAX(prop_conc_inline) AS prop_conc_inline_max,
              MAX(prop_conc_inline) FILTER (WHERE rn_first = 1) AS prop_conc_inline_first,
              MAX(prop_conc_inline) FILTER (WHERE rn_last = 1) AS prop_conc_inline_last,
              AVG(prop_conc_target) AS prop_conc_target_avg,
              MIN(prop_conc_target) AS prop_conc_target_min,
              MAX(prop_conc_target) AS prop_conc_target_max,
              MAX(prop_conc_target) FILTER (WHERE rn_first = 1) AS prop_conc_target_first,
              MAX(prop_conc_target) FILTER (WHERE rn_last = 1) AS prop_conc_target_last,
              AVG(prop_conc_blend_auger) AS prop_conc_blend_auger_avg,
              MIN(prop_conc_blend_auger) AS prop_conc_blend_auger_min,
              MAX(prop_conc_blend_auger) AS prop_conc_blend_auger_max,
              MAX(prop_conc_blend_auger) FILTER (WHERE rn_first = 1) AS prop_conc_blend_auger_first,
              MAX(prop_conc_blend_auger) FILTER (WHERE rn_last = 1) AS prop_conc_blend_auger_last,
              CAST(:rollup_ts AS timestamp) AS rollup_created_at,
              CAST(:rollup_ts AS timestamp) AS rollup_updated_at
            FROM ranked
            GROUP BY
              bucket_ts,
              fleet_name,
              pad_name,
              name
            ORDER BY
              fleet_name,
              pad_name,
              name,
              bucket_ts
        """,
        workspace_id=workspace_id,
        params=params,
    )


async def _get_source_end(
    workspace_id: int,
    fleet_name: str | None,
    pad_name: str | None,
    well_name: str | None,
) -> datetime | None:
    bounds_df = await query_store(
        sql=f"""
            SELECT
              MAX(CAST(record_ts AS timestamp)) AS end_ts
            FROM datastore:{SOURCE_DATASTORE_KEY}
            WHERE
              record_ts IS NOT NULL
              AND name IS NOT NULL
              AND (:fleet_name = '' OR COALESCE(NULLIF(fleet_name, ''), 'Unknown') = :fleet_name)
              AND (:pad_name = '' OR COALESCE(NULLIF(pad_name, ''), 'Unknown') = :pad_name)
              AND (:well_name = '' OR name = :well_name)
        """,
        workspace_id=workspace_id,
        params={
            "fleet_name": fleet_name or "",
            "pad_name": pad_name or "",
            "well_name": well_name or "",
        },
    )
    if bounds_df.is_empty():
        return None
    bounds = bounds_df.to_pandas()
    if pd.isna(bounds.iloc[0]["end_ts"]):
        return None
    return _parse_time(str(bounds.iloc[0]["end_ts"]))


async def _get_rollup_checkpoint(
    workspace_id: int,
    bucket_seconds: int,
    fleet_name: str | None,
    pad_name: str | None,
    well_name: str | None,
) -> datetime | None:
    checkpoint_df = await query_store(
        sql=f"""
            SELECT
              MAX(CAST(bucket_ts AS timestamp)) AS checkpoint_ts
            FROM datastore:{ROLLUP_DATASTORE_KEY}
            WHERE
              CAST(bucket_seconds AS integer) = :bucket_seconds
              AND bucket_ts IS NOT NULL
              AND (:fleet_name = '' OR COALESCE(NULLIF(fleet_name, ''), 'Unknown') = :fleet_name)
              AND (:pad_name = '' OR COALESCE(NULLIF(pad_name, ''), 'Unknown') = :pad_name)
              AND (:well_name = '' OR name = :well_name)
        """,
        workspace_id=workspace_id,
        params={
            "bucket_seconds": int(bucket_seconds),
            "fleet_name": fleet_name or "",
            "pad_name": pad_name or "",
            "well_name": well_name or "",
        },
    )
    if checkpoint_df.is_empty():
        return None
    checkpoint = checkpoint_df.to_pandas()
    if pd.isna(checkpoint.iloc[0]["checkpoint_ts"]):
        return None
    return _parse_time(str(checkpoint.iloc[0]["checkpoint_ts"]))


async def _get_source_bounds(
    workspace_id: int,
    fleet_name: str | None,
    pad_name: str | None,
    well_name: str | None,
) -> tuple[datetime, datetime]:
    bounds_df = await query_store(
        sql=f"""
            SELECT
              MIN(CAST(record_ts AS timestamp)) AS start_ts,
              MAX(CAST(record_ts AS timestamp)) AS end_ts
            FROM datastore:{SOURCE_DATASTORE_KEY}
            WHERE
              record_ts IS NOT NULL
              AND name IS NOT NULL
              AND (:fleet_name = '' OR COALESCE(NULLIF(fleet_name, ''), 'Unknown') = :fleet_name)
              AND (:pad_name = '' OR COALESCE(NULLIF(pad_name, ''), 'Unknown') = :pad_name)
              AND (:well_name = '' OR name = :well_name)
        """,
        workspace_id=workspace_id,
        params={
            "fleet_name": fleet_name or "",
            "pad_name": pad_name or "",
            "well_name": well_name or "",
        },
    )
    if bounds_df.is_empty():
        raise ValueError("No source records found for historical rollup bounds")
    bounds = bounds_df.to_pandas()
    if pd.isna(bounds.iloc[0]["start_ts"]) or pd.isna(bounds.iloc[0]["end_ts"]):
        raise ValueError("No source records found for historical rollup bounds")
    return _parse_time(str(bounds.iloc[0]["start_ts"])), _parse_time(str(bounds.iloc[0]["end_ts"]))


@flow(name="nextier-merged-telemetry-rollup")
async def nextier_merged_telemetry_rollup_flow(
    workspace_id: int,
    workflow_id: int,
    process_historical: bool = False,
    start_time: str | None = None,
    end_time: str | None = None,
    backfill_start_time: str | None = None,
    backfill_end_time: str | None = None,
    reset_backfill_checkpoint: bool = False,
    reset_backfill_cursor: bool = False,
    lookback_minutes: int = 15,
    bucket_seconds: int = 60,
    chunk_hours: int = 24,
    max_chunks: int = 1,
    fleet_name: str | None = None,
    pad_name: str | None = None,
    well_name: str | None = None,
):
    logger = get_run_logger()

    bucket_seconds = max(int(bucket_seconds), 1)
    state = await get_state(workflow_id=workflow_id, workspace_id=workspace_id)
    backfill_start = _parse_time(backfill_start_time)
    backfill_end = _parse_time(backfill_end_time)
    explicit_start = _parse_time(start_time)
    explicit_end = _parse_time(end_time)

    backfill_cursor_key = _backfill_cursor_key(
        bucket_seconds=bucket_seconds,
        fleet_name=fleet_name,
        pad_name=pad_name,
        well_name=well_name,
    )
    mode = "scheduled"
    should_backfill = process_historical or backfill_start is not None or backfill_end is not None
    should_reset_backfill = reset_backfill_checkpoint or reset_backfill_cursor

    if should_backfill:
        if backfill_start is None or backfill_end is None:
            source_start, source_end = await _get_source_bounds(
                workspace_id=workspace_id,
                fleet_name=fleet_name,
                pad_name=pad_name,
                well_name=well_name,
            )
            backfill_start = backfill_start or source_start
            backfill_end = backfill_end or source_end
        mode = "backfill"
        persisted_cursor = None
        if not should_reset_backfill:
            persisted_cursor = _parse_time(state.get(backfill_cursor_key))
        start_ts = max(persisted_cursor or backfill_start, backfill_start)
        end_ts = backfill_end
    else:
        mode = "explicit" if explicit_start is not None or explicit_end is not None else "scheduled"
        source_end = await _get_source_end(
            workspace_id=workspace_id,
            fleet_name=fleet_name,
            pad_name=pad_name,
            well_name=well_name,
        )
        if source_end is None:
            result = {
                "rollup_datastore": ROLLUP_DATASTORE_KEY,
                "bucket_seconds": bucket_seconds,
                "mode": mode,
                "rows_upserted": 0,
                "chunks": [],
                "reason": "no_source_records",
            }
            logger.info("Merged telemetry rollup result: %s", result)
            return result

        if explicit_start is not None or explicit_end is not None:
            end_ts = explicit_end or source_end
            start_ts = explicit_start or (end_ts - timedelta(minutes=max(int(lookback_minutes), 1)))
        else:
            rollup_checkpoint = await _get_rollup_checkpoint(
                workspace_id=workspace_id,
                bucket_seconds=bucket_seconds,
                fleet_name=fleet_name,
                pad_name=pad_name,
                well_name=well_name,
            )
            persisted_checkpoint = _parse_time(state.get(CHECKPOINT_KEY))
            checkpoint = rollup_checkpoint or persisted_checkpoint or source_end
            end_ts = min(source_end, _utc_now_naive())
            start_ts = checkpoint - timedelta(
                minutes=max(int(lookback_minutes), 1),
            )
            logger.info(
                "Merged telemetry rollup checkpoint bucket_seconds=%s rollup_checkpoint=%s state_checkpoint=%s source_end=%s",
                bucket_seconds,
                _format_time(rollup_checkpoint) if rollup_checkpoint else None,
                _format_time(persisted_checkpoint) if persisted_checkpoint else None,
                _format_time(source_end),
            )

    if start_ts >= end_ts:
        result = {
            "rollup_datastore": ROLLUP_DATASTORE_KEY,
            "bucket_seconds": bucket_seconds,
            "mode": mode,
            "start_time": _format_time(start_ts),
            "end_time": _format_time(end_ts),
            "rows_upserted": 0,
            "chunks": [],
        }
        if mode == "backfill":
            result["backfill_cursor_key"] = backfill_cursor_key
            result["backfill_complete"] = True
        logger.info("Merged telemetry rollup result: %s", result)
        return result

    rollup_ts = _format_time(_utc_now_naive())
    total_rows = 0
    processed_chunks: list[dict[str, str | int]] = []
    last_processed_end = start_ts

    for chunk_start, chunk_end in _window_chunks(
        start=start_ts,
        end=end_ts,
        chunk_hours=int(chunk_hours),
        max_chunks=int(max_chunks),
    ):
        logger.info(
            "Computing merged telemetry rollup: start=%s end=%s bucket_seconds=%s",
            _format_time(chunk_start),
            _format_time(chunk_end),
            bucket_seconds,
        )
        rollup_df = await _compute_rollup_chunk(
            workspace_id=workspace_id,
            start_ts=chunk_start,
            end_ts=chunk_end,
            bucket_seconds=bucket_seconds,
            rollup_ts=rollup_ts,
            fleet_name=fleet_name,
            pad_name=pad_name,
            well_name=well_name,
        )
        row_count = len(rollup_df)
        if row_count:
            await write_datastore(
                datastore_key=ROLLUP_DATASTORE_KEY,
                workspace_id=workspace_id,
                df=rollup_df,
                upsert=True,
            )
        total_rows += row_count
        last_processed_end = chunk_end
        processed_chunks.append(
            {
                "start_time": _format_time(chunk_start),
                "end_time": _format_time(chunk_end),
                "rows": row_count,
            },
        )
        if mode == "backfill":
            await set_state(
                workflow_id=workflow_id,
                workspace_id=workspace_id,
                key=backfill_cursor_key,
                value=_format_time(chunk_end),
            )

    await set_state(
        workflow_id=workflow_id,
        workspace_id=workspace_id,
        key=CHECKPOINT_KEY,
        value=_format_time(last_processed_end),
    )

    result = {
        "rollup_datastore": ROLLUP_DATASTORE_KEY,
        "bucket_seconds": bucket_seconds,
        "mode": mode,
        "start_time": _format_time(start_ts),
        "end_time": _format_time(last_processed_end),
        "requested_end_time": _format_time(end_ts),
        "rows_upserted": total_rows,
        "chunks": processed_chunks,
    }
    if mode == "backfill":
        result["backfill_cursor_key"] = backfill_cursor_key
        result["backfill_complete"] = last_processed_end >= end_ts
    logger.info("Merged telemetry rollup result: %s", result)
    return result
