DO $$
BEGIN
    RAISE NOTICE 'Starting enhanced FTS setup...';
END $$;

DO $$ BEGIN RAISE NOTICE 'Enabling extensions (unaccent, pg_trgm)...'; END $$;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DO $$ BEGIN RAISE NOTICE 'Ensuring tsvector columns exist...'; END $$;
ALTER TABLE extracted_files ADD COLUMN IF NOT EXISTS content_tsv_en tsvector;
ALTER TABLE extracted_files ADD COLUMN IF NOT EXISTS content_tsv_ru tsvector;
ALTER TABLE extracted_files ADD COLUMN IF NOT EXISTS content_tsv_simple tsvector;

DO $$ BEGIN RAISE NOTICE 'Creating custom text search configurations with unaccent...'; END $$;

CREATE TEXT SEARCH CONFIGURATION public.fts_english_unaccent (COPY = pg_catalog.english);
ALTER TEXT SEARCH CONFIGURATION public.fts_english_unaccent
    ALTER MAPPING FOR hword, hword_part, word
    WITH unaccent, english_stem;

CREATE TEXT SEARCH CONFIGURATION public.fts_russian_unaccent (COPY = pg_catalog.russian);
ALTER TEXT SEARCH CONFIGURATION public.fts_russian_unaccent
    ALTER MAPPING FOR hword, hword_part, word
    WITH unaccent, russian_stem;

DO $$ BEGIN RAISE NOTICE 'Creating/Replacing trigger function update_fts_vectors_enhanced()...'; END $$;
CREATE OR REPLACE FUNCTION update_fts_vectors_enhanced() RETURNS TRIGGER AS $$
BEGIN
    NEW.content_tsv_en := to_tsvector('public.fts_english_unaccent', COALESCE(NEW.content, ''));
    NEW.content_tsv_ru := to_tsvector('public.fts_russian_unaccent', COALESCE(NEW.content, ''));
    NEW.content_tsv_simple := to_tsvector('pg_catalog.simple', COALESCE(NEW.content, ''));

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN RAISE NOTICE 'Creating/Replacing trigger fts_vector_update_enhanced...'; END $$;
DROP TRIGGER IF EXISTS fts_vector_update_enhanced ON extracted_files;
DROP TRIGGER IF EXISTS fts_vector_update ON extracted_files;

CREATE TRIGGER fts_vector_update_enhanced
BEFORE INSERT OR UPDATE ON extracted_files
FOR EACH ROW EXECUTE FUNCTION update_fts_vectors_enhanced();

DO $$
BEGIN
    RAISE NOTICE 'Starting initial population/re-population of tsvector columns for existing data using enhanced configurations...';
    RAISE NOTICE 'NOTE: This may take time on large tables as all rows are processed.';
END $$;

UPDATE extracted_files
SET
    content_tsv_en = to_tsvector('public.fts_english_unaccent', COALESCE(content, '')),
    content_tsv_ru = to_tsvector('public.fts_russian_unaccent', COALESCE(content, '')),
    content_tsv_simple = to_tsvector('pg_catalog.simple', COALESCE(content, ''));

DO $$
BEGIN
    RAISE NOTICE 'Finished initial population/re-population of tsvector columns.';
END $$;

DO $$ BEGIN RAISE NOTICE 'Creating GIN indexes on tsvector columns...'; END $$;
DROP INDEX IF EXISTS idx_fts_en;
DROP INDEX IF EXISTS idx_fts_ru;
DROP INDEX IF EXISTS idx_fts_simple;

CREATE INDEX idx_fts_en ON extracted_files USING GIN(content_tsv_en);
CREATE INDEX idx_fts_ru ON extracted_files USING GIN(content_tsv_ru);
CREATE INDEX idx_fts_simple ON extracted_files USING GIN(content_tsv_simple);

DO $$ BEGIN RAISE NOTICE 'Creating GIN index on content column using gin_trgm_ops for typo tolerance...'; END $$;
DROP INDEX IF EXISTS idx_trgm_content;
CREATE INDEX idx_trgm_content ON extracted_files USING GIN (content gin_trgm_ops);

DO $$
BEGIN
    RAISE NOTICE 'Enhanced FTS setup complete. Extensions, custom configurations, trigger, tsvector indexes, and trigram index are ready.';
    RAISE NOTICE '---';
    RAISE NOTICE 'Optional Next Steps / Query Considerations:';
    RAISE NOTICE '1. Weighted Ranking: Use ts_rank_cd(tsvector_col, query) at query time. If you had separate title/body columns, you could use setweight(to_tsvector(...), ''A'') || setweight(...) and adjust ts_rank_cd weights.';
    RAISE NOTICE '2. Typo-Tolerant Search: Use the similarity operator: SELECT file_path, similarity(content, %s) as sim FROM extracted_files WHERE content %% %s ORDER BY sim DESC LIMIT N;', 'your_query', 'your_query_with_typo';
    RAISE NOTICE '3. Thesaurus/Synonyms: Add a thesaurus dictionary to the custom configurations using ALTER TEXT SEARCH CONFIGURATION.';
    RAISE NOTICE '4. Custom Stop Words: Modify the stop word list used by the stemmers within the custom configurations.';
    RAISE NOTICE '---';
END $$;
