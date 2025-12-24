SELECT  IdCliente,
        count(DISTINCT substr(DtCriacao, 0, 11)) as qtdeFrequencia,
        sum(CASE WHEN QtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPos,
        sum(abs(QtdePontos)) as QtdePontosAbs

FROM transacoes

WHERE date(DtCriacao) < '2025-09-01'
AND DtCriacao >= date('2025-09-01',  '-28 days') 

GROUP BY IdCliente
ORDER BY qtdeFrequencia desc