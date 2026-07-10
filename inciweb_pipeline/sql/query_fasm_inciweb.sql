SELECT a.id AS inciweb_id,
       round(ST_X(ST_Transform(a.geom, 4326))::numeric, 4) AS lon,
       round(ST_Y(ST_Transform(a.geom, 4326))::numeric, 4) AS lat,
       round(a.acres) AS acres,
       fasm_fire_id IS NOT NULL AS fasm_tracking,
       fasm_fire_last_updated,
       a.type AS itype,
       COALESCE(nearby.nearby_wildfire, '[]'::json) AS nearby_wildfire,
       COALESCE(nearby.nearby_rx, '[]'::json) AS nearby_rx
FROM fire_summary.fasm_inciweb a
LEFT JOIN LATERAL (
    SELECT
        json_agg(json_build_object(
            'link', b.link,
            'name', b.incident,
            'lon', round(ST_X(ST_Transform(b.geom, 4326))::numeric, 4),
            'lat', round(ST_Y(ST_Transform(b.geom, 4326))::numeric, 4)
        )) FILTER (WHERE LOWER(b.type) LIKE '%wildfire%') AS nearby_wildfire,
        json_agg(json_build_object(
            'link', b.link,
            'name', b.incident,
            'lon', round(ST_X(ST_Transform(b.geom, 4326))::numeric, 4),
            'lat', round(ST_Y(ST_Transform(b.geom, 4326))::numeric, 4)
        )) FILTER (WHERE LOWER(b.type) LIKE '%prescribed%') AS nearby_rx
    FROM fire_summary.fasm_inciweb b
    WHERE ST_DWithin(a.geom::geography, b.geom::geography, 100000)
      AND a.id != b.id
) nearby ON true;
