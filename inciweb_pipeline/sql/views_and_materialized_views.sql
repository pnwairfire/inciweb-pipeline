CREATE MATERIALIZED VIEW inciweb_widget.devices_last_80_hourly_measurements AS 
	SELECT 
		measurement_time, 
		public.ST_SetSRID(public.ST_MakePoint(longitude, latitude),4326) as geopoint, 
		aqi as aqi 
	FROM 
		purple_air_data.purple_air_hourly_data 
	WHERE 
		is_excluded is false 
		and is_outlier is false
		and aqi is not null
		and created_at > now() - interval '80 hour'
	UNION ALL
	SELECT 
		measurement_time, 
		public.ST_SetSRID(public.ST_MakePoint(longitude, latitude),4326) as geopoint, 
		aqi as aqi 
	FROM 
		airquality.pm25_hourly 
	WHERE 
		aqi is not null
		and created_at > now() - interval '80 hour';
