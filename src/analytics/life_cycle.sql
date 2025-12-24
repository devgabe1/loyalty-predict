-- curiosa -> idade < 7
-- fiel recencia > 7 e recencia anterios < 15 
-- turista -> recencia <= 14
-- desencantado -> recencia <= 28
-- zumbi -> recencia >= 28
-- reconquistado -> recencia < 7 e 14 <= recencia anterior <=28
-- reborn -> recencia < 7 e recencia anterior > 28
WITH tb_daily as (
    SELECT
        DISTINCT
            IdCliente,
            date(substr(DtCriacao, 0, 11)) as DtDia
    FROM transacoes
    WHERE DtCriacao < "{date}"
),


tb_idade as (
    SELECT
        IdCliente,
        -- min(dtDia) as dtPrimeiraTransacao,
        cast(max(julianday('{date}') - julianday(DtDia)) as int) as qtdeDiasPrimeiraTransacao,

        -- max(dtDia) as dtUltimaTransacao,
        cast(min(julianday('{date}') - julianday(DtDia)) as int) as qtdeDiasUltimaTransacao
    FROM tb_daily
    GROUP BY idCliente
),

tb_rn as(
    SELECT  *,
            row_number() OVER (PARTITION BY IdCliente ORDER BY DtDia DESC) as rnDia
    FROM tb_daily
),

tb_penultima_ativacao as(   
    SELECT  * ,
            CAST(julianday('{date}') - julianday(DtDia) AS INT) AS qtdeDiasPenultimaTransacao
    FROM    tb_rn 
    WHERE   rnDia = 2
),

tb_life_cycle as(
    SELECT  t1.*,
            t2.qtdeDiasPenultimaTransacao,
            CASE
                WHEN qtdeDiasPrimeiraTransacao <= 7 THEN '01-CURIOSO'
                WHEN qtdeDiasUltimaTransacao <= 7 AND qtdeDiasPenultimaTransacao - qtdeDiasUltimaTransacao <=  14 THEN '02-FIEL'
                WHEN qtdeDiasUltimaTransacao BETWEEN 8 AND 14 THEN '03-TURISTA'
                WHEN qtdeDiasUltimaTransacao BETWEEN 15 AND 27 THEN '04-DESENCANTADA'
                WHEN qtdeDiasUltimaTransacao >= 28 THEN '05-ZUMBI'
                WHEN qtdeDiasUltimaTransacao <= 7 AND qtdeDiasPenultimaTransacao - qtdeDiasUltimaTransacao BETWEEN 15 AND 27 THEN '02-RECONQUISTADO'
                WHEN qtdeDiasUltimaTransacao <= 7 AND qtdeDiasPenultimaTransacao - qtdeDiasUltimaTransacao > 28 THEN '02-REBORN'


            END AS descLifeCycle
            
    FROM    tb_idade AS t1
    LEFT JOIN tb_penultima_ativacao AS t2
    ON t1.idCliente = t2.idCliente
)

SELECT  date("{date}", "-1 day") as DtRef,
        *
FROM    tb_life_cycle

