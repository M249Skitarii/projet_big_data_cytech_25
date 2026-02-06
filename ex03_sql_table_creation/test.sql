-- 1. Voir les 5 premières lignes avec les noms de zones et arrondissements
-- Cela permet de vérifier que les jointures (le flocon) fonctionnent bien
-- 2. Obtenir le nombre total de lignes
SELECT COUNT(*) AS total_lignes_inserees FROM FACT_TRIPS;
SELECT * FROM dim_date Limit 51;

SELECT *
FROM pg_stat_activity
WHERE state <> 'idle';
SELECT usename, application_name, client_addr
FROM pg_stat_activity;
DROP DATABASE postgres;


-- Générer les IDs de 1 à 256
WITH all_ids AS (
    SELECT generate_series(1, 256) AS location_id
)
-- Garder seulement ceux qui ne sont pas dans dim_zones
SELECT a.location_id
FROM all_ids a
LEFT JOIN dim_zones d
ON a.location_id = d.location_id
WHERE d.location_id IS NULL
ORDER BY a.location_id;
