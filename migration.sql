-- Migration: Adicionar coluna source para sync bidirecional ERP ↔ Excel
-- Executar no Supabase SQL Editor

-- 1. Adicionar coluna source nas tabelas
ALTER TABLE despesas ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'excel';
ALTER TABLE outros ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'excel';

-- 2. Marcar registros existentes como vindos do Excel
UPDATE despesas SET source = 'excel' WHERE source IS NULL;
UPDATE outros SET source = 'excel' WHERE source IS NULL;

-- 3. Atualizar RPC sync_despesas para não sobrescrever itens do ERP
-- Match usa UPPER(TRIM()) para evitar duplicatas por variação de capitalização
CREATE OR REPLACE FUNCTION sync_despesas(payload JSONB)
RETURNS void AS $$
DECLARE
  item JSONB;
  matched_id BIGINT;
BEGIN
  FOR item IN SELECT * FROM jsonb_array_elements(payload)
  LOOP
    -- Match por data + valor + pago (case-insensitive) — identifica a transação
    -- Depois tenta refinar por descricao normalizado
    SELECT id INTO matched_id
    FROM despesas
    WHERE data = item->>'data'
      AND valor = (item->>'valor')::NUMERIC
      AND UPPER(TRIM(pago)) = UPPER(TRIM(item->>'pago'))
      AND UPPER(TRIM(descricao)) = UPPER(TRIM(item->>'descricao'))
    LIMIT 1;

    IF matched_id IS NULL THEN
      -- Fallback: mesma transação (data+valor+pago) mas nome diferente
      SELECT id INTO matched_id
      FROM despesas
      WHERE data = item->>'data'
        AND valor = (item->>'valor')::NUMERIC
        AND UPPER(TRIM(pago)) = UPPER(TRIM(item->>'pago'))
      LIMIT 1;
    END IF;

    IF matched_id IS NOT NULL THEN
      -- Atualizar registro existente (nome, obs, pago) se não é do ERP
      UPDATE despesas
      SET descricao = UPPER(TRIM(item->>'descricao')),
          obs = TRIM(item->>'obs'),
          pago = TRIM(item->>'pago'),
          source = 'excel'
      WHERE id = matched_id
        AND source != 'erp';
    ELSE
      -- Inserir novo registro vindo do Excel (já normalizado)
      INSERT INTO despesas (descricao, obs, data, pago, valor, source)
      VALUES (
        UPPER(TRIM(item->>'descricao')),
        TRIM(item->>'obs'),
        item->>'data',
        TRIM(item->>'pago'),
        (item->>'valor')::NUMERIC,
        'excel'
      );
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 4. Atualizar RPC sync_outros para não sobrescrever itens do ERP
CREATE OR REPLACE FUNCTION sync_outros(payload JSONB)
RETURNS void AS $$
DECLARE
  item JSONB;
  matched_id BIGINT;
BEGIN
  FOR item IN SELECT * FROM jsonb_array_elements(payload)
  LOOP
    SELECT id INTO matched_id
    FROM outros
    WHERE UPPER(TRIM(cat)) = UPPER(TRIM(item->>'cat'))
      AND data = item->>'data'
      AND valor = (item->>'valor')::NUMERIC
    LIMIT 1;

    IF matched_id IS NOT NULL THEN
      UPDATE outros
      SET source = 'excel'
      WHERE id = matched_id
        AND source != 'erp';
    ELSE
      INSERT INTO outros (cat, data, valor, source)
      VALUES (
        TRIM(item->>'cat'),
        item->>'data',
        (item->>'valor')::NUMERIC,
        'excel'
      );
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
