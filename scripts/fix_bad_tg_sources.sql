UPDATE sources
SET tg_channel = '@fgupnami',
    updated_at = NOW()
WHERE id = 'tg_ru_nami_russia';

UPDATE sources
SET tg_channel = '@sberbank_auto',
    updated_at = NOW()
WHERE id = 'tg_ru_sber_auto';
