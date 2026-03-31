DROP TABLE IF EXISTS public.matches CASCADE;

CREATE TABLE public.matches ( 
	id BIGSERIAL PRIMARY KEY, 
	match_api_id BIGINT UNIQUE, 
	season VARCHAR(20), 
	stage BIGINT, 
	match_date TIMESTAMP, 
	home_team_id BIGINT, 
	away_team_id BIGINT, 
	home_team VARCHAR(150), 
	away_team VARCHAR(150), 
	home_team_goal BIGINT, 
	away_team_goal BIGINT, 
	result VARCHAR(20) );