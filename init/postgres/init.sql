CREATE DATABASE youtube;
\c youtube;
\i /mnt/sources/init/enum_iso3166.sql;
CREATE TABLE channel (
    uuid UUID default gen_random_uuid() NOT NULL,
    etag CHAR(27) NOT NULL,
    id CHAR(24) NOT NULL,
    title VARCHAR(100) NOT NULL,
    "description" VARCHAR(5000),
    custom_url VARCHAR(31),
    published_at Timestamp,
    country country_alpha2,
    uploads CHAR(24),
    view_count INTEGER,
    subscriber INTEGER,
    video_count INTEGER,
    topic_category TEXT[],
    updated_at TIMESTAMP NOT NULL default CURRENT_TIMESTAMP
);


