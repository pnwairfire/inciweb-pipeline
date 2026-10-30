CREATE OR REPLACE FUNCTION inciweb_widget.population_in_box(latitude numeric,longitude numeric,width_in_km numeric)
RETURNS INTEGER AS
$$
DECLARE box_population INTEGER;
BEGIN
	SELECT 
		(sum(l.population))::int 
	INTO 
		box_population 
	FROM 
		common.landscan_population_usa l
	WHERE 
		public.st_contains(public.ST_ENVELOPE ( public.ST_TRANSFORM (
			public.ST_BUFFER(public.ST_TRANSFORM ( 
				public.ST_SetSRID(public.ST_MakePoint(
					population_in_box.longitude, population_in_box.latitude), 4326) , 5070) , (500*population_in_box.width_in_km)) , 4326)),l.geom);
RETURN box_population;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION inciweb_widget.aqi_cats_in_box(latitude numeric, longitude numeric, width_in_km numeric, hours_ago integer DEFAULT 72)
 RETURNS TABLE(measurement_hr timestamp without time zone, aqi_level_1 integer, aqi_level_2 integer, 
 			aqi_level_3 integer, aqi_level_4 integer, aqi_level_5 integer, aqi_level_6 integer, 
 			total_monitors integer)
 LANGUAGE plpgsql
AS $function$
BEGIN
	RETURN QUERY
	SELECT 
		d.measurement_time::timestamp without time zone as measurement_hr,
		count(case when d.aqi between 0 and 50 then 1 end)::int aqi_level_1,
		count(case when d.aqi between 51 and 100 then 1 end)::int aqi_level_2,
		count(case when d.aqi between 101 and 150 then 1 end)::int aqi_level_3,
		count(case when d.aqi between 151 and 200 then 1 end)::int aqi_level_4,
		count(case when d.aqi between 201 and 300 then 1 end)::int aqi_level_5,
		count(case when d.aqi > 300 then 1 end)::int aqi_level_6,
		count(*)::int total_monitors
	FROM 
		inciweb_widget.devices_last_80_hourly_measurements d
	WHERE
	    public.st_contains(public.ST_ENVELOPE ( public.ST_TRANSFORM (
	    	public.ST_BUFFER(public.ST_TRANSFORM ( 
	    	public.ST_SetSRID(public.ST_MakePoint(longitude, latitude), 4326) , 5070) 
	    	, (500*width_in_km)) , 4326)),d.geopoint)
	    and d.measurement_time > now() - (hours_ago::text || ' ' || 'hour')::INTERVAL
	    and d.aqi is not null
	GROUP BY 1
	ORDER BY 1;
END;
$function$


CREATE OR REPLACE FUNCTION inciweb_widget.refresh_device_last_80_hourly_measurements()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
REFRESH MATERIALIZED VIEW inciweb_widget.devices_last_80_hourly_measurements;
END;
$function$






