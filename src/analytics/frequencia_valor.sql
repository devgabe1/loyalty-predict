WITH tb_freq_valor AS (

    SELECT  IdCliente,
            count(DISTINCT substr(DtCriacao, 0, 11)) as qtdeFrequencia,
            sum(CASE WHEN QtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPos
            -- sum(abs(QtdePontos)) as QtdePontosAbs

    FROM transacoes

    WHERE date(DtCriacao) < '2025-09-01'
    AND DtCriacao >= date('2025-09-01',  '-28 days') 

    GROUP BY IdCliente
    ORDER BY qtdeFrequencia DESC
),

tb_cluster AS(
    SELECT  *,
            CASE
                WHEN qtdeFrequencia <= 10 AND qtdePontosPos >= 1500 THEN 'HYPER'
                WHEN qtdeFrequencia > 10 AND qtdePontosPos >= 1500 THEN 'EFICIENTE'
                WHEN qtdeFrequencia <= 10 AND qtdePontosPos >= 750 THEN 'INDECISO'
                WHEN qtdeFrequencia > 10 AND qtdePontosPos >= 750 THEN 'ESFORÇADO'
                WHEN qtdeFrequencia < 5 THEN 'LURKER'
                WHEN qtdeFrequencia <= 10 THEN 'PREGUIÇOSO'
                WHEN qtdeFrequencia > 10 THEN 'POTENCIAL' 

            END AS cluster

    FROM tb_freq_valor
)

SELECT * FROM tb_cluster