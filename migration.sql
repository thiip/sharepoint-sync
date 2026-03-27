-- Migration: Adicionar coluna source para sync bidirecional ERP ↔ Excel
-- Executar no Supabase SQL Editor

-- 1. Adicionar coluna source nas tabelas
ALTER TABLE despesas ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'excel';
ALTER TABLE outros ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'excel';

-- 2. Marcar registros existentes como vindos do Excel
UPDATE despesas SET source = 'excel' WHERE source IS NULL;
UPDATE outros SET source = 'excel' WHERE source IS NULL;

-- 3. Atualizar RPC sync_despesas para não sobrescrever itens do ERP
CREATE OR REPLACE FUNCTION sync_despesas(payload JSONB)
RETURNS void AS $$
DECLARE
  item JSONB;
  matched_id BIGINT;
BEGIN
  FOR item IN SELECT * FROM jsonb_array_elements(payload)
  LOOP
    -- Tentar encontrar registro existente por descricao + data + valor
    SELECT id INTO matched_id
    FROM despesas
    WHERE descricao = item->>'descricao'
      AND data = item->>'data'
      AND valor = (item->>'valor')::NUMERIC
    LIMIT 1;

    IF matched_id IS NOT NULL THEN
      -- Atualizar apenas se não foi modificado pelo ERP
      UPDATE despesas
      SET obs = item->>'obs',
          pago = item->>'pago',
          source = 'excel'
      WHERE id = matched_id
        AND source != 'erp';
    ELSE
      -- Inserir novo registro vindo do Excel
      INSERT INTO despesas (descricao, obs, data, pago, valor, source)
      VALUES (
        item->>'descricao',
        item->>'obs',
        item->>'data',
        item->>'pago',
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
    WHERE cat = item->>'cat'
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
        item->>'cat',
        item->>'data',
        (item->>'valor')::NUMERIC,
        'excel'
      );
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
