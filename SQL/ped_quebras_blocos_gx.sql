WITH  
PED_DATES AS (SELECT ID_PEDIDO FROM PEDID WHERE 
/* DATES */ 

PEDDTEMIS >=DATEADD(-7 DAY TO CURRENT_DATE) AND PEDDTEMIS < 'TODAY')


SELECT 
      CASE 
       WHEN EMPCODIGO=1 THEN 'MATRIZ' 
        WHEN EMPCODIGO=3 THEN 'JOINVILLE'
         WHEN EMPCODIGO=4 THEN 'CRICIUMA'
          WHEN EMPCODIGO=5  THEN 'CHAPECO'
           WHEN EMPCODIGO=8  THEN 'B.CAMBORIU' 
            ELSE '' END EMPRESA,
      
        TPCODIGO TIPO,
         'QUEBRAS' INDICADOR,
           COUNT(P.ID_PEDIDO) TOTAL
            FROM PEDID P
            
            INNER JOIN PED_DATES PDT ON P.ID_PEDIDO=PDT.ID_PEDIDO
             
/* FILTRA BLOCOS */  

INNER JOIN (SELECT
DISTINCT
PCP.ID_PEDIDO
FROM PEDCELPDCAO PCP
INNER JOIN PED_DATES PDT ON PCP.ID_PEDIDO=PDT.ID_PEDIDO
LEFT JOIN PDCAO PDC ON PCP.PDCCODIGO = PDC.PDCCODIGO AND PCP.EMPPDCCODIGO = PDC.EMPCODIGO
LEFT JOIN REQUI REQ ON PDC.PDCCODIGO = REQ.PDCCODIGO AND PDC.EMPCODIGO = REQ.EMPCODIGO
LEFT JOIN REQPRO RP ON RP.REQCODIGO = REQ.REQCODIGO AND RP.EMPCODIGO = REQ.EMPCODIGO
INNER JOIN (SELECT PROCODIGO FROM PRODU WHERE PROCODIGO2 IN 
('MF0317','MF0322','MF0323','MF0385','MF0391','MF0396','MF0397','MF0398','MF0399',
'MF0404','MF0405','MF0417','MF0418','MF0419','MF0426'))PR ON RP.PROCODIGO=PR.PROCODIGO) PRD ON P.ID_PEDIDO=PRD.ID_PEDIDO
                  
             WHERE FISCODIGO1='5.927'
              AND CLICODIGO=579

               GROUP BY 1,2,3
--------------------------------------