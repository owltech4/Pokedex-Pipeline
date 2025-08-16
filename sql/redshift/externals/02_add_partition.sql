-- Registra a partição do batch que acabou de cair no BRONZE
ALTER TABLE {EXTERNAL_SCHEMA}.pokemon_bronze
ADD IF NOT EXISTS PARTITION (
  ingestion_date='{INGESTION_DATE}',
  batch_id='{BATCH_ID}'
)
LOCATION '{S3_BRONZE}ingestion_date={INGESTION_DATE}/batch_id={BATCH_ID}/';
