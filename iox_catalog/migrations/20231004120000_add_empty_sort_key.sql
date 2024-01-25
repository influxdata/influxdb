-- We no longer use sort_key but to avoid phase deployments for Clustered customers,
-- we do not need to drop it. However, since it was already dropped in the previous migration,
-- let us add it back as a NULL column 
ALTER TABLE partition ADD COLUMN sort_key TEXT[];