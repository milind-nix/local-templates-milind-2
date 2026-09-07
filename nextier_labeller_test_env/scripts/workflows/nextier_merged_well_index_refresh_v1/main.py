from __future__ import annotations

from prefect import flow, get_run_logger

from nixdlt.workflow_sdk.platform_tasks import (
    get_state,
    query_store,
    set_state,
    write_featurestore,
)


SOURCE_DATASTORE_KEY = "merged_fleet_stream_customer_full_v2"
WELL_INDEX_FEATURESTORE_KEY = "merged_well_index_v1"
CHECKPOINT_KEY = "__checkpoint__merged_well_index_created_ts"


@flow(name="nextier-merged-well-index-refresh-v1")
async def nextier_merged_well_index_refresh_v1_flow(
    workspace_id: int,
    workflow_id: int,
    batch_limit: int = 200000,
):
    logger = get_run_logger()
    state = await get_state(workflow_id=workflow_id, workspace_id=workspace_id)

    logger.info(
        "Merged well index changed-well scan start checkpoint=%s batch_limit=%s",
        state.get(CHECKPOINT_KEY),
        batch_limit,
    )
    changed_rows_df = await query_store(
        sql=f"""
            WITH changed_rows AS (
              SELECT
                CAST(created_ts AS timestamp) AS created_ts,
                name
              FROM datastore:{SOURCE_DATASTORE_KEY}
              WHERE
                created_ts IS NOT NULL
                AND record_ts IS NOT NULL
                AND name IS NOT NULL
                AND (CAST(:{CHECKPOINT_KEY} AS timestamp) IS NULL OR CAST(created_ts AS timestamp) > CAST(:{CHECKPOINT_KEY} AS timestamp))
              ORDER BY
                CAST(created_ts AS timestamp) ASC,
                name ASC
              LIMIT {int(batch_limit)}
            )
            SELECT
              name,
              MAX(created_ts) AS created_ts
            FROM changed_rows
            GROUP BY name
            ORDER BY name ASC
        """,
        workspace_id=workspace_id,
        params=state,
    )
    logger.info(
        "Merged well index changed-well scan complete changed_wells=%s",
        len(changed_rows_df),
    )
    if changed_rows_df.is_empty():
        logger.info("No changed merged wells for well-index refresh")
        return {"changed_wells": 0, "well_index_rows_upserted": 0}

    well_names = [
        str(name) for name in changed_rows_df["name"].drop_nulls().to_list()
    ]
    logger.info("Refreshing merged well index changed_wells=%s", len(well_names))

    params = {f"well_name_{idx}": well for idx, well in enumerate(well_names)}
    values_sql = " UNION ALL ".join(
        f"SELECT :well_name_{idx} AS name" for idx in range(len(well_names))
    )
    logger.info(
        "Merged well index aggregate query start changed_wells=%s",
        len(well_names),
    )
    well_index = await query_store(
        sql=f"""
            WITH changed_wells AS (
              {values_sql}
            ),
            source_rows AS (
              SELECT
                t.name,
                t.fleet_name,
                t.pad_name,
                t.id,
                t.api_num,
                CAST(t.record_ts AS timestamp) AS record_ts,
                CAST(t.created_ts AS timestamp) AS created_ts,
                ROW_NUMBER() OVER (
                  PARTITION BY t.name
                  ORDER BY CAST(t.record_ts AS timestamp) DESC, CAST(t.created_ts AS timestamp) DESC
                ) AS row_rank
              FROM datastore:{SOURCE_DATASTORE_KEY} t
              JOIN changed_wells w
                ON t.name = w.name
              WHERE
                t.created_ts IS NOT NULL
                AND t.record_ts IS NOT NULL
                AND t.name IS NOT NULL
            ),
            aggregated AS (
              SELECT
                name,
                MIN(record_ts) AS first_record_ts,
                MAX(record_ts) AS last_record_ts,
                MIN(created_ts) AS first_created_ts,
                MAX(created_ts) AS last_created_ts,
                COUNT(*) AS sample_count
              FROM source_rows
              GROUP BY name
            ),
            latest AS (
              SELECT
                name,
                fleet_name,
                pad_name,
                id AS well_id,
                api_num
              FROM source_rows
              WHERE row_rank = 1
            )
            SELECT
              a.name,
              CASE
                WHEN l.pad_name IS NOT NULL AND trim(l.pad_name) <> '' THEN l.pad_name
                WHEN trim(a.name) = '' THEN 'Unknown'
                WHEN split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 2) = '' THEN split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 1)
                WHEN split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 3) ~ '^[0-9]+$' THEN split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 1) || ' ' || split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 2) || ' ' || split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 3)
                ELSE split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 1) || ' ' || split_part(regexp_replace(trim(a.name), '\\s+', ' ', 'g'), ' ', 2)
              END AS well_family,
              l.fleet_name,
              l.pad_name,
              l.well_id,
              l.api_num,
              CAST(a.first_record_ts AS TEXT) AS first_record_ts,
              CAST(a.last_record_ts AS TEXT) AS last_record_ts,
              CAST(a.first_created_ts AS TEXT) AS first_created_ts,
              CAST(a.last_created_ts AS TEXT) AS last_created_ts,
              a.sample_count
            FROM aggregated a
            LEFT JOIN latest l
              ON l.name = a.name
            ORDER BY well_family ASC, a.name ASC
        """,
        workspace_id=workspace_id,
        params=params,
    )
    logger.info(
        "Merged well index aggregate query complete rows=%s",
        len(well_index),
    )
    if well_index.is_empty():
        logger.info("No merged raw rows found for changed wells")
        return {"changed_wells": len(well_names), "well_index_rows_upserted": 0}

    if not well_index.is_empty():
        await write_featurestore(
            featurestore_key=WELL_INDEX_FEATURESTORE_KEY,
            workspace_id=workspace_id,
            df=well_index,
            upsert=True,
        )

    checkpoint = changed_rows_df["created_ts"].max()
    if checkpoint is not None:
        await set_state(
            workflow_id=workflow_id,
            workspace_id=workspace_id,
            key=CHECKPOINT_KEY,
            value=str(checkpoint),
        )

    logger.info(
        "Merged well index refresh complete changed_wells=%s rows=%s checkpoint=%s",
        len(well_names),
        len(well_index),
        checkpoint,
    )
    return {
        "changed_wells": len(well_names),
        "well_index_rows_upserted": len(well_index),
        CHECKPOINT_KEY: str(checkpoint) if checkpoint is not None else None,
    }
