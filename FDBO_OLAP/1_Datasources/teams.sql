DROP TABLE IF EXISTS public.teams CASCADE;

CREATE TABLE public.teams( 
	id BIGSERIAL PRIMARY KEY, 
	team_api_id BIGINT UNIQUE NOT NULL, 
	team_fifa_api_id BIGINT, 
	team_long_name VARCHAR(150), 
	team_short_name VARCHAR(50) );