-- ============================================================
-- Schema dedicado "galpao" para o ERP Reforma Galpão 380
-- Isola as tabelas do projeto para não conflitar com outros
-- projetos que compartilham este Supabase.
--
-- APÓS rodar este arquivo, adicionar "galpao" em PGRST_DB_SCHEMAS
-- no container supabase-rest do Coolify e reiniciar o serviço.
-- ============================================================

BEGIN;

-- 1. Schema + permissões de uso
CREATE SCHEMA IF NOT EXISTS galpao;

GRANT USAGE ON SCHEMA galpao TO anon, authenticated, service_role;
GRANT CREATE ON SCHEMA galpao TO service_role;

-- Privilégios default para tabelas futuras criadas pelo service_role
ALTER DEFAULT PRIVILEGES IN SCHEMA galpao
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA galpao
  GRANT USAGE, SELECT ON SEQUENCES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA galpao
  GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;

-- 2. Tabelas
CREATE TABLE IF NOT EXISTS galpao.despesas (
  id          BIGSERIAL PRIMARY KEY,
  descricao   TEXT NOT NULL DEFAULT '',
  obs         TEXT NOT NULL DEFAULT '',
  data        DATE,
  pago        TEXT NOT NULL DEFAULT '',
  valor       NUMERIC(14,2) NOT NULL DEFAULT 0,
  source      TEXT NOT NULL DEFAULT 'excel',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS galpao.outros (
  id          BIGSERIAL PRIMARY KEY,
  cat         TEXT NOT NULL DEFAULT '',
  data        DATE,
  valor       NUMERIC(14,2) NOT NULL DEFAULT 0,
  source      TEXT NOT NULL DEFAULT 'excel',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS galpao.financeiro (
  id          BIGSERIAL PRIMARY KEY,
  empresa     TEXT NOT NULL DEFAULT '',
  descricao   TEXT NOT NULL DEFAULT '',
  valor       NUMERIC(14,2) NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Garantir privilégios em tabelas já criadas
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA galpao TO anon, authenticated, service_role;
GRANT USAGE, SELECT                 ON ALL SEQUENCES IN SCHEMA galpao TO anon, authenticated, service_role;

-- Índices úteis
CREATE INDEX IF NOT EXISTS despesas_source_idx ON galpao.despesas (source);
CREATE INDEX IF NOT EXISTS despesas_data_idx   ON galpao.despesas (data);
CREATE INDEX IF NOT EXISTS outros_source_idx   ON galpao.outros   (source);
CREATE INDEX IF NOT EXISTS outros_cat_idx      ON galpao.outros   (cat);

-- 3. RPCs qualificadas com schema galpao
CREATE OR REPLACE FUNCTION galpao.sync_despesas(payload JSONB)
RETURNS void AS $$
DECLARE
  item JSONB;
  matched_id BIGINT;
BEGIN
  FOR item IN SELECT * FROM jsonb_array_elements(payload)
  LOOP
    SELECT id INTO matched_id
    FROM galpao.despesas
    WHERE data = (item->>'data')::DATE
      AND valor = (item->>'valor')::NUMERIC
      AND UPPER(TRIM(pago)) = UPPER(TRIM(item->>'pago'))
      AND UPPER(TRIM(descricao)) = UPPER(TRIM(item->>'descricao'))
    LIMIT 1;

    IF matched_id IS NULL THEN
      SELECT id INTO matched_id
      FROM galpao.despesas
      WHERE data = (item->>'data')::DATE
        AND valor = (item->>'valor')::NUMERIC
        AND UPPER(TRIM(pago)) = UPPER(TRIM(item->>'pago'))
      LIMIT 1;
    END IF;

    IF matched_id IS NOT NULL THEN
      UPDATE galpao.despesas
      SET descricao = UPPER(TRIM(item->>'descricao')),
          obs       = TRIM(COALESCE(item->>'obs','')),
          pago      = TRIM(item->>'pago'),
          source    = 'excel'
      WHERE id = matched_id
        AND source != 'erp';
    ELSE
      INSERT INTO galpao.despesas (descricao, obs, data, pago, valor, source)
      VALUES (
        UPPER(TRIM(item->>'descricao')),
        TRIM(COALESCE(item->>'obs','')),
        (item->>'data')::DATE,
        TRIM(item->>'pago'),
        (item->>'valor')::NUMERIC,
        'excel'
      );
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION galpao.sync_outros(payload JSONB)
RETURNS void AS $$
DECLARE
  item JSONB;
  matched_id BIGINT;
BEGIN
  FOR item IN SELECT * FROM jsonb_array_elements(payload)
  LOOP
    SELECT id INTO matched_id
    FROM galpao.outros
    WHERE UPPER(TRIM(cat)) = UPPER(TRIM(item->>'cat'))
      AND data = (item->>'data')::DATE
      AND valor = (item->>'valor')::NUMERIC
    LIMIT 1;

    IF matched_id IS NOT NULL THEN
      UPDATE galpao.outros
      SET source = 'excel'
      WHERE id = matched_id
        AND source != 'erp';
    ELSE
      INSERT INTO galpao.outros (cat, data, valor, source)
      VALUES (
        TRIM(item->>'cat'),
        (item->>'data')::DATE,
        (item->>'valor')::NUMERIC,
        'excel'
      );
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Placeholder (igual ao do schema public) — mantido para compatibilidade
-- com o frontend que chama sb.rpc('sync_financeiro_data', {})
CREATE OR REPLACE FUNCTION galpao.sync_financeiro_data()
RETURNS void AS $$
BEGIN
  NULL;
END;
$$ LANGUAGE plpgsql;

-- 4. Garantir grants nas funções
GRANT EXECUTE ON FUNCTION galpao.sync_despesas(JSONB)      TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION galpao.sync_outros(JSONB)        TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION galpao.sync_financeiro_data()    TO anon, authenticated, service_role;

COMMIT;
