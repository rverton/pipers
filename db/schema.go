package db

const SQL_CREATE_DATA_TBL = `
CREATE TABLE IF NOT EXISTS %v (
	id text primary key,
	asset text not null,
	target text not null,
	pipe text not null,
	exclude boolean default false,
	data jsonb,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS %v_asset_idx ON %v (asset);
CREATE INDEX IF NOT EXISTS %v_target_idx ON %v (target);
`

const SQL_CREATE_ESSENTIALS = `
CREATE TABLE IF NOT EXISTS pipers_tasks (
	id serial primary key,
	pipe text not null,
	ident text not null,
	note text,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tasks_pipe_idx ON pipers_tasks (pipe);
CREATE INDEX IF NOT EXISTS tasks_ident_idx ON pipers_tasks (ident);

CREATE TABLE IF NOT EXISTS pipers_alerts (
	id serial primary key,
	type text not null,
	pipe text not null,
	ident text not null,
	message text,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS alerts_pipe_idx ON pipers_alerts (pipe);
CREATE INDEX IF NOT EXISTS alerts_ident_idx ON pipers_alerts (ident);
`
