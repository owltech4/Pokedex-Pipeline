-- Tabela externa apontando para o BRONZE no S3 (Parquet)
CREATE EXTERNAL TABLE IF NOT EXISTS {EXTERNAL_SCHEMA}.pokemon (
  pokedex_number           int,
  name                     varchar(200),
  japanese_name            varchar(200),
  classfication            varchar(200),
  type1                    varchar(50),
  type2                    varchar(50),
  abilities                varchar(1000),
  height_m                 double precision,
  weight_kg                double precision,
  capture_rate             int,
  base_egg_steps           int,
  experience_growth        int,
  percentage_male          double precision,
  generation               int,
  is_legendary             boolean,

  against_bug              double precision,
  against_dark             double precision,
  against_dragon           double precision,
  against_electric         double precision,
  against_fairy            double precision,
  against_fighting         double precision,
  against_fire             double precision,
  against_flying           double precision,
  against_ghost            double precision,
  against_grass            double precision,
  against_ground           double precision,
  against_ice              double precision,
  against_normal           double precision,
  against_poison           double precision,
  against_psychic          double precision,
  against_rock             double precision,
  against_steel            double precision,
  against_water            double precision,

  attack                   int,
  defense                  int,
  sp_attack                int,
  sp_defense               int,
  speed                    int,
  hp                       int,

  _ingestion_ts_utc        varchar(40),
  _source_file             varchar(200),
  _source_sha256           varchar(64),
  _run_id                  varchar(40),
  _batch_id                varchar(20)
)
PARTITIONED BY (
  ingestion_date           varchar(10),   -- YYYY-MM-DD (vindo do path)
  batch_id                 varchar(13)    -- YYYY_MM_DD_HH (vindo do path)
)
STORED AS PARQUET
LOCATION '{S3_BRONZE}';
