-- Query per trovare i 15 film più redditizi (maggior profitto netto),
-- mostrando per ciascuno la dimensione del cast, i dati finanziari e il voto medio,
-- ed escludendo i record con budget o incassi pari a zero.
SELECT 
    f.title AS Titolo_Film,
    COUNT(c.person_id) AS Dimensione_Cast,
    f.budget AS Budget_Investito,
    f.revenue AS Incasso_Totale,
    (f.revenue - f.budget) AS Profitto_Netto,
    f.voteAverage AS Voto_Medio
FROM 
    films f
JOIN 
    cast c ON f.filmId = c.filmId
WHERE 
    f.budget > 0 AND f.revenue > 0 -- Escludiamo i film con dati finanziari mancanti
GROUP BY 
    f.filmId, 
    f.title, 
    f.budget, 
    f.revenue, 
    f.voteAverage
ORDER BY 
    Profitto_Netto DESC
LIMIT 15;
