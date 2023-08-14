EXECUTE BLOCK RETURNS (
                        EMPRESA VARCHAR(30),
                         INDICADOR VARCHAR(30),
                          PEDIDO INT,
                           EMISSAO DATE,   
                            MATERIAL VARCHAR(100),
                             NOME VARCHAR(100),
                              UN VARCHAR(30), 
                               SEQ VARCHAR(30),
                                CHAVE VARCHAR(30),
                                 DESCRICAO_CHAVE VARCHAR(100),
                                  QTD DECIMAL(15,2))
  
  AS DECLARE VARIABLE ID_PEDIDO INT;
  
  BEGIN
  FOR
  
  /* FILTRO PEDIDOS NAO ATENDIDOS CONTROL MATRIZ */ 
  
WITH 
RESULT11 AS (SELECT DISTINCT P.ID_PEDIDO,PEDDTEMIS,AP.APCODIGO,LP.LPCODIGO FROM
PEDID P

/* FILTRA BLOCOS */  

INNER JOIN (SELECT
DISTINCT
PCP.ID_PEDIDO
FROM PEDCELPDCAO PCP
INNER JOIN (SELECT ID_PEDIDO FROM PEDID WHERE PEDDTEMIS >=DATEADD(-7 DAY TO CURRENT_DATE) AND PEDDTEMIS < 'TODAY') PDT ON PCP.ID_PEDIDO=PDT.ID_PEDIDO
LEFT JOIN PDCAO PDC ON PCP.PDCCODIGO = PDC.PDCCODIGO AND PCP.EMPPDCCODIGO = PDC.EMPCODIGO
LEFT JOIN REQUI REQ ON PDC.PDCCODIGO = REQ.PDCCODIGO AND PDC.EMPCODIGO = REQ.EMPCODIGO
LEFT JOIN REQPRO RP ON RP.REQCODIGO = REQ.REQCODIGO AND RP.EMPCODIGO = REQ.EMPCODIGO
INNER JOIN (SELECT PROCODIGO FROM PRODU WHERE PROCODIGO2 IN 
('MF0317','MF0322','MF0323','MF0385','MF0391','MF0396','MF0397','MF0398','MF0399',
'MF0404','MF0405','MF0417','MF0418','MF0419','MF0426'))PR ON RP.PROCODIGO=PR.PROCODIGO) PRD ON P.ID_PEDIDO=PRD.ID_PEDIDO 

LEFT JOIN ACOPED AP ON AP.ID_PEDIDO= P.ID_PEDIDO
LEFT JOIN LOCALPED LP ON LP.LPCODIGO  = AP.LPCODIGO 
WHERE 

/* DATES */ 

PEDDTEMIS >=DATEADD(-7 DAY TO CURRENT_DATE) AND PEDDTEMIS < 'TODAY'
--------------------------------------

AND
P.EMPCODIGO=1
AND TPCODIGO IN (10,11)
AND PEDSITPED IN ('A','F'))
 
 SELECT DISTINCT R.ID_PEDIDO
   FROM RESULT11 R
   WHERE
      R.ID_PEDIDO > 0
      AND R.APCODIGO = (SELECT MAX(APCODIGO) FROM ACOPED APO 
      LEFT JOIN LOCALPED LP2 ON APO.LPCODIGO = LP2.LPCODIGO
      WHERE APO.ID_PEDIDO = R.ID_PEDIDO )
      AND EXISTS (SELECT 1 FROM ACOPED ACP WHERE ACP.ID_PEDIDO= R.ID_PEDIDO AND ACP.LPCODIGO = 69)
      AND NOT EXISTS (SELECT 1 FROM ACOPED ACP WHERE ACP.ID_PEDIDO= R.ID_PEDIDO AND ACP.LPCODIGO = 1805 ) 
      AND NOT EXISTS (SELECT 1 FROM ACOPED ACP WHERE ACP.ID_PEDIDO= R.ID_PEDIDO AND ACP.LPCODIGO = 1847 )

  INTO :ID_PEDIDO 
  DO
  BEGIN
  FOR

  /* FILTRO PEDIDOS NAO ATENDIDOS MATRIZ */ 
  
  WITH PED AS (SELECT ID_PEDIDO,PEDDTEMIS FROM PEDID WHERE ID_PEDIDO=:ID_PEDIDO),
  
      PROD AS (SELECT PROCODIGO,PRODESCRICAO,IIF(PROCODIGO2 IS NULL, PROCODIGO,PROCODIGO2)CHAVE FROM PRODU WHERE PROSITUACAO='A' AND PROTIPO NOT IN ('T','M','S','C'))

  
  SELECT
   'MATRIZ' EMPRESA,
   'PEDIDOS CONTROL NAO ATENDIDOS' INDICADOR,
   ID_PEDIDO,
   (SELECT PEDDTEMIS FROM PEDID WHERE ID_PEDIDO=:ID_PEDIDO) EMISSAO ,
   RP.PROCODIGO MATERIAL , 
    RP.RQPDESCRICAO NOME , 
     PD.PROUN UN , 
      RQPSEQ,
      PD.PROCODIGO2 CHAVE,
      (SELECT DISTINCT PRODESCRICAO FROM PRODU WHERE PROCODIGO=PD.PROCODIGO2) DESCRICAO_CHAVE,
      RP.RQPQTDADE QTD    
      FROM PEDCELPDCAO PCP
LEFT JOIN PDCAO PDC ON PCP.PDCCODIGO = PDC.PDCCODIGO AND PCP.EMPPDCCODIGO = PDC.EMPCODIGO
LEFT JOIN REQUI REQ ON PDC.PDCCODIGO = REQ.PDCCODIGO AND PDC.EMPCODIGO = REQ.EMPCODIGO
LEFT JOIN REQPRO RP ON RP.REQCODIGO = REQ.REQCODIGO AND RP.EMPCODIGO = REQ.EMPCODIGO
INNER JOIN (SELECT PROCODIGO,PROCODIGO2,PRODESCRICAO,PROUN FROM PRODU WHERE PROTIPO NOT IN ('T','S')) PD ON PD.PROCODIGO = RP.PROCODIGO

WHERE
PCP.ID_PEDIDO = :ID_PEDIDO AND
RP.PROCODIGO IS NOT NULL UNION

SELECT
  'MATRIZ' EMPRESA,
  'PEDIDOS CONTROL NAO ATENDIDOS' INDICADOR,
   PRD.ID_PEDIDO,
   (SELECT PEDDTEMIS FROM PEDID WHERE ID_PEDIDO=:ID_PEDIDO) EMISSAO ,
   PRD.PROCODIGO MATERIAL , 
    PDPDESCRICAO NOME , 
      (SELECT DISTINCT PROUN FROM PRODU WHERE PROCODIGO=CHAVE ) UN,
      '' RQPSEQ,
      CHAVE CHAVE,
      (SELECT PRODESCRICAO FROM PRODU WHERE PROCODIGO=CHAVE ) DESCRICAO_CHAVE,
      PRD.PDPQTDADE QTD    
      FROM PDPRD PRD
      INNER JOIN PED ON PRD.ID_PEDIDO=PED.ID_PEDIDO
      INNER JOIN PROD P ON PRD.PROCODIGO=P.PROCODIGO
  
  INTO :EMPRESA,:INDICADOR,:PEDIDO,:EMISSAO, :MATERIAL,:NOME, :UN, :SEQ,:CHAVE, :DESCRICAO_CHAVE,:QTD
  
  DO BEGIN
  
  SUSPEND;
  
  END
  END
  END