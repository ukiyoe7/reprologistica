EXECUTE BLOCK RETURNS (EMPRESA VARCHAR(30),TIPO VARCHAR(30),INDICADOR VARCHAR(30),TOTAL DECIMAL(15,2))
  
  AS DECLARE VARIABLE CLIENTE INT;
  
  BEGIN
  FOR
  
  SELECT DISTINCT CLICODIGO
                   FROM CLIEN
                    WHERE CLICODIGO IN (495,1065,81,5)

                     INTO :CLIENTE
                      DO
                       BEGIN
                        FOR
                        
  
                         SELECT 
                           CASE 
                            WHEN CLICODIGO=495 THEN 'JOINVILLE'
                             WHEN CLICODIGO=1065 THEN 'CRICIUMA'
                              WHEN CLICODIGO=81 THEN 'CHAPECO'
                               WHEN CLICODIGO=5 THEN 'BALNEARIO C'
                                END FILIAL,
                                 TPCODIGO,
                                  COUNT (DISTINCT (P.ID_PEDIDO)) TOTAL,'CONTROL' INDICADOR
                                   FROM PEDID P
                                   
                                   INNER JOIN (SELECT ID_PEDIDO FROM PEDID WHERE 
/* DATES */ 

PEDDTEMIS >=DATEADD(-7 DAY TO CURRENT_DATE) AND PEDDTEMIS < 'TODAY') PDT ON P.ID_PEDIDO=PDT.ID_PEDIDO
                                   
                                    /* FILTER LENS */  
/* FILTRA BLOCOS */  

INNER JOIN (SELECT
DISTINCT
PCP.ID_PEDIDO
FROM PEDCELPDCAO PCP
INNER JOIN (SELECT ID_PEDIDO FROM PEDID WHERE 
PEDDTEMIS >=DATEADD(-7 DAY TO CURRENT_DATE) AND PEDDTEMIS < 'TODAY')PDT ON PCP.ID_PEDIDO=PDT.ID_PEDIDO
LEFT JOIN PDCAO PDC ON PCP.PDCCODIGO = PDC.PDCCODIGO AND PCP.EMPPDCCODIGO = PDC.EMPCODIGO
LEFT JOIN REQUI REQ ON PDC.PDCCODIGO = REQ.PDCCODIGO AND PDC.EMPCODIGO = REQ.EMPCODIGO
LEFT JOIN REQPRO RP ON RP.REQCODIGO = REQ.REQCODIGO AND RP.EMPCODIGO = REQ.EMPCODIGO
INNER JOIN (SELECT PROCODIGO FROM PRODU WHERE PROCODIGO2 IN 
('MF0317','MF0322','MF0323','MF0385','MF0391','MF0396','MF0397','MF0398','MF0399',
'MF0404','MF0405','MF0417','MF0418','MF0419','MF0426'))PR ON RP.PROCODIGO=PR.PROCODIGO) PRD ON P.ID_PEDIDO=PRD.ID_PEDIDO 
                  
                                    WHERE P.PEDSITPED IN ('A','F')
                                     AND P.CLICODIGO = :CLIENTE AND P.TPCODIGO IN ('10','11')  


AND EXISTS (SELECT 1 FROM PEDROTEIRO PR1 WHERE PR1.ALXCODIGO = 1 AND PR1.ID_PEDIDO = P.ID_PEDIDO)
AND NOT EXISTS (SELECT 1 FROM PEDROTEIRO PR2 WHERE PR2.ALXCODIGO IN ('15','16','10','9','38','40','6') 
AND PR2.ID_PEDIDO = P.ID_PEDIDO) GROUP BY 1,2
  
  INTO :EMPRESA,:TIPO, :TOTAL,:INDICADOR
  
  DO BEGIN
  
  SUSPEND;
  
  END
  END
  END 