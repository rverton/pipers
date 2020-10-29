package db

const SQL_CREATE_DATA_TBL = `
CREATE TABLE IF NOT EXISTS %v (
	id text primary key,
	hostname text not null,
	target text not null,
	pipe text not null,
	data jsonb,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS %v_hostname_idx ON %v (hostname);
CREATE INDEX IF NOT EXISTS %v_target_idx ON %v (target);
`

const SQL_CREATE_ESSENTIALS = `
CREATE TABLE IF NOT EXISTS tasks (
	id serial primary key,
	pipe text not null,
	ident text not null,
	note text,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tasks_pipe_idx ON tasks (pipe);
CREATE INDEX IF NOT EXISTS tasks_ident_idx ON tasks (ident);

CREATE TABLE IF NOT EXISTS alerts (
	id serial primary key,
	type text not null,
	pipe text not null,
	ident text not null,
	message text,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS alerts_pipe_idx ON alerts (pipe);
CREATE INDEX IF NOT EXISTS alerts_ident_idx ON alerts (ident);
`
