WITH tb_transacao AS (

    SELECT  *,
            substr(DtCriacao, 0, 11) AS dtDia,
            cast(substr(DtCriacao, 12, 2) AS int) AS dtHora
    FROM    transacoes
    WHERE   DtCriacao < '2025-10-01'
),

tb_agg_transacao AS(
    SELECT  IdCliente,
            
            max(julianday(date('2025-10-01', '-1 day')) - julianday(DtCriacao)) AS idadeDias, 

            count(DISTINCT dtDia) AS qtdeAtivacaoVida,
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-7 days') THEN dtDia END) AS qtdeAtivacaoD7, 
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-14 days') THEN dtDia END) AS qtdeAtivacaoD14, 
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-28 days') THEN dtDia END) AS qtdeAtivacaoD28,
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-56 days') THEN dtDia END) AS qtdeAtivacaoD56,

            count(DISTINCT IdTransacao) AS qtdeTransacaoVida,
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-7 days') THEN IdTransacao END) AS qtdeTransacaoD7, 
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-14 days') THEN IdTransacao END) AS qtdeTransacaoD14, 
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-28 days') THEN IdTransacao END) AS qtdeTransacaoD28,
            count(DISTINCT CASE WHEN dtDia >= date('2025-10-01', '-56 days') THEN IdTransacao END) AS qtdeTransacaoD56,

            sum(qtdePontos) AS saldoVida,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-7 days') THEN qtdePontos ELSE 0 END) AS saldoD7,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-14 days') THEN qtdePontos ELSE 0  END) AS saldoD14,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-28 days') THEN qtdePontos ELSE 0  END) AS saldoD28,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-56 days') THEN qtdePontos ELSE 0  END) AS saldoD56,

            sum(CASE WHEN qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPosVida,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-7 days') AND qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPosD7,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-14 days') AND qtdePontos > 0 THEN qtdePontos ELSE 0  END) AS qtdePontosPosD14,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-28 days') AND qtdePontos > 0 THEN qtdePontos ELSE 0  END) AS qtdePontosPosD28,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-56 days') AND qtdePontos > 0 THEN qtdePontos ELSE 0  END) AS qtdePontosPosD56,

            sum(CASE WHEN qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegVida,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-7 days') AND qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegD7,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-14 days') AND qtdePontos < 0 THEN qtdePontos ELSE 0  END) AS qtdePontosNegD14,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-28 days') AND qtdePontos < 0 THEN qtdePontos ELSE 0  END) AS qtdePontosNegD28,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-56 days') AND qtdePontos < 0 THEN qtdePontos ELSE 0  END) AS qtdePontosNegD56,

            count(CASE WHEN dtHora BETWEEN 10 AND 14 THEN IdTransacao END) AS qtdeTransacaoManha,
            count(CASE WHEN dtHora BETWEEN 15 AND 21 THEN IdTransacao END) AS qtdeTransacaoTarde,
            count(CASE WHEN dtHora > 21 OR dtHora < 10 THEN IdTransacao END) AS qtdeTransacaoNoite,

            1. * count(CASE WHEN dtHora BETWEEN 10 AND 14 THEN IdTransacao END) / count(idTransacao) AS pctTransacaoManha,
            1. * count(CASE WHEN dtHora BETWEEN 15 AND 21 THEN IdTransacao END) / count(idTransacao) AS pctTransacaoTarde,
            1. * count(CASE WHEN dtHora > 21 OR dtHora < 10 THEN IdTransacao END) / count(idTransacao) AS pctTransacaoNoite

    FROM    tb_transacao
    GROUP BY idCliente
),

tb_agg_calc AS(
    SELECT  *,
        COALESCE(1. * qtdeTransacaoVida / qtdeAtivacaoVida, 0) AS qtdeTransacaoDiaVida,
        COALESCE(1. * qtdeTransacaoD7 / qtdeAtivacaoD7, 0) AS qtdeTransacaoDiaD7,
        COALESCE(1. * qtdeTransacaoD14 / qtdeAtivacaoD14, 0) AS qtdeTransacaoDiaD14,
        COALESCE(1. * qtdeTransacaoD28 / qtdeAtivacaoD28, 0) AS qtdeTransacaoDiaD28,
        COALESCE(1. * qtdeTransacaoD56 / qtdeAtivacaoD56, 0) AS qtdeTransacaoDiaD56,

        COALESCE(1. * qtdeAtivacaoD28 / 28, 0) AS pctAtivacaoMAU

    FROM tb_agg_transacao 
),

tb_horas_dia AS (

    SELECT  IdCliente,
            DtDia,
            (max(julianday(DtCriacao)) - min(julianday(DtCriacao))) * 24 AS duracao
    FROM tb_transacao
    GROUP BY IdCliente, dtDia
),

tb_hora_cliente AS (
    SELECT  idCliente,
            sum(duracao) as qtdeHorasVida,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-7 days') THEN duracao ELSE 0 END) AS qtdeHorasD7,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-14 days') THEN duracao ELSE 0 END) AS qtdeHorasD14,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-28 days') THEN duracao ELSE 0 END) AS qtdeHorasD28,
            sum(CASE WHEN dtDia >= date('2025-10-01', '-56 days') THEN duracao ELSE 0 END) AS qtdeHorasD56
    FROM tb_horas_dia

    GROUP BY IdCliente
),

tb_lag_day AS (

    SELECT  idCliente,
            DtDia,
            LAG(DtDia) OVER (PARTITION BY idCliente ORDER BY DtDia) AS lagDia

    FROM tb_horas_dia
),

tb_intervalo_dias AS(

    SELECT  IdCliente,
            avg(julianday(DtDia) - julianday(lagDia)) AS avgIntervaloDiasVida,
            avg(CASE WHEN DtDia >= date('2025-10-01', '-28 day') THEN julianday(DtDia) - julianday(lagDia) END) AS avgIntervaloDiasD28
    FROM tb_lag_day
    GROUP BY idCliente
),

tb_share_produtos AS (

    SELECT  
        IdCliente,
        1. * COUNT(CASE WHEN DescNomeProduto = 'ChatMessage' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteChatMessage,
        1. * COUNT(CASE WHEN DescNomeProduto = 'Airflow Lover' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteAirflowLover,
        1. * COUNT(CASE WHEN DescNomeProduto = 'R Lover' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteRLover,
        1. * COUNT(CASE WHEN DescNomeProduto = 'Resgatar Ponei' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteResgatarPonei,
        1. * COUNT(CASE WHEN DescNomeProduto = 'Lista de presença' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteListadepresenca,
        1. * COUNT(CASE WHEN DescNomeProduto = 'Presença Streak' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qtePresencaStreak,
        1. * COUNT(CASE WHEN DescNomeProduto = 'Troca de Pontos StreamElements' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteTrocaStreamElements,
        1. * COUNT(CASE WHEN DescNomeProduto = 'Reembolso: Troca de Pontos StreamElements' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteReembolsoStreamElements,
        1. * COUNT(CASE WHEN DescCategoriaProduto = 'rpg' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteRPG,
        1. * COUNT(CASE WHEN DescCategoriaProduto = 'churn_model' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteChurnModel

    FROM tb_transacao AS t1

    LEFT JOIN transacao_produto AS t2
    ON t1.IdTransacao = t2.IdTransacao

    LEFT JOIN produtos AS t3
    ON t2.IdProduto = t3.IdProduto

    GROUP BY IdCliente
),

tb_join AS (
    SELECT  t1.*,
            t2.qtdeHorasVida,
            t2.qtdeHorasD7,
            t2.qtdeHorasD14,
            t2.qtdeHorasD28,
            t2.qtdeHorasD56,
            t3.avgIntervaloDiasVida,
            t3.avgIntervaloDiasD28,
            t4.qteChatMessage,
            t4.qteAirflowLover,
            t4.qteRLover,
            t4.qteResgatarPonei,
            t4.qteListadepresenca,
            t4.qtePresencaStreak,
            t4.qteTrocaStreamElements,
            t4.qteReembolsoStreamElements,
            t4.qteRPG,
            t4.qteChurnModel
    FROM tb_agg_calc AS t1
    
    LEFT JOIN tb_hora_cliente AS t2
    ON t1.idCliente = t2.idCliente

    LEFT JOIN tb_intervalo_dias as t3
    ON t1.idCliente = t3.idCliente

    LEFT JOIN tb_share_produtos as t4
    ON t1.idCliente = t4.IdCliente
)

SELECT  date('2025-10-01', '-1 day') AS dtRef,
        *
FROM tb_join