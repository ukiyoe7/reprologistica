## FILL RATE BLOCOS GX
## MF0317; MF0322; MF0323; MF0385; MF0391; MF0396; MF0397; MF0398; MF0399; MF0404; MF0405; MF0417; MF0418; MF0419; MF0426; 


library(tidyverse)
library(reshape2)
library(DBI)
library(googlesheets4)
library(readr)
con2 <- dbConnect(odbc::odbc(), "reproreplica")

blocos_gx <- dbGetQuery(con2,"SELECT PROCODIGO FROM PRODU WHERE PROCODIGO2='MF0317'")

View(blocos_gx)

blocos_gx_compo <- dbGetQuery(con2,"
WITH 
PEDID_DATE AS (SELECT ID_PEDIDO FROM PEDID WHERE PEDDTBAIXA >=DATEADD(-90 DAY TO CURRENT_DATE))

SELECT
DISTINCT
PCP.ID_PEDIDO
FROM PEDCELPDCAO PCP
INNER JOIN PEDID_DATE P ON PCP.ID_PEDIDO=P.ID_PEDIDO
LEFT JOIN PDCAO PDC ON PCP.PDCCODIGO = PDC.PDCCODIGO AND PCP.EMPPDCCODIGO = PDC.EMPCODIGO
LEFT JOIN REQUI REQ ON PDC.PDCCODIGO = REQ.PDCCODIGO AND PDC.EMPCODIGO = REQ.EMPCODIGO
LEFT JOIN REQPRO RP ON RP.REQCODIGO = REQ.REQCODIGO AND RP.EMPCODIGO = REQ.EMPCODIGO
INNER JOIN (SELECT PROCODIGO FROM PRODU WHERE PROCODIGO2 IN('MF0317','MF0322','MF0323','MF0385','MF0391','MF0396','MF0397','MF0398','MF0399','MF0404','MF0405','MF0417','MF0418','MF0419','MF0426'))PR ON RP.PROCODIGO=PR.PROCODIGO")

View(blocos_gx_compo)

### TOTAL PEDIDOS

tot_ped_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/tot_ped_blocos_gx.sql')) 

View(tot_ped_blocos_gx)

tot_ped_natd_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/tot_ped_natd_blocos_gx.sql')) 

View(tot_ped_natd_blocos_gx)

ped_control_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/ped_control_blocos_gx.sql')) 

View(ped_control_blocos_gx)

ped_quebras_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/ped_quebras_blocos_gx.sql')) 

View(ped_quebras_blocos_gx)


## RESUMO PEDIDOS POR TIPO

fillrate_resumo_1_blocos_gx <- rbind(tot_ped_blocos_gx,tot_ped_natd_blocos_gx,ped_control_blocos_gx,ped_quebras_blocos_gx) %>% 
  mutate(TIPO=as.numeric(TIPO))

fillrate_resumo_2_blocos_gx <- fillrate_resumo_1_blocos_gx %>% 
  dcast(EMPRESA + INDICADOR ~ TIPO,value.var = "TOTAL") %>% 
  arrange(desc(EMPRESA)) %>% 
  as.data.frame()  %>% 
  rowwise() %>% 
  mutate(TOTAL= rowSums(across(where(is.numeric)),na.rm = TRUE)) %>% 
  mutate(DATA=Sys.Date())

View(fillrate_resumo_2_blocos_gx)

## SAVE SUMMARY 

filewd_fillrate_resumo_2_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_resumo_2_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")

save(fillrate_resumo_2_blocos_gx,file =filewd_fillrate_resumo_2_blocos_gx)

## CALCULO FILL RATE =================================================================


## CALCULO FILIAIS 

fillrate_calc_filiais_blocos_gx <- fillrate_resumo_1_blocos_gx %>% 
  filter(!str_detect(EMPRESA,"MATRIZ")) %>% 
  group_by(EMPRESA) %>% 
  mutate(INDICADOR=trimws(INDICADOR)) %>% 
  summarize(VALOR=(1-((sum(TOTAL[INDICADOR=='CONTROL'])+sum(TOTAL[INDICADOR=='PEDIDOS NAO ATENDIDOS']))/
                        sum(TOTAL[INDICADOR=='TOTAL PEDIDOS'])))*100) %>%
  mutate(VALOR=round(VALOR,2)) %>% mutate(INDICADOR="FILL RATE: CONTROL + NAO ATENDIDOS x TOTAL PEDIDOS") %>% 
  mutate(DATA=Sys.Date()) %>% as.data.frame() %>% .[,c(1,3,2,4)] 


## CALCULO MATRIZ

fillrate_calc_matriz_1_blocos_gx <- fillrate_resumo_1_blocos_gx %>%  
  filter(str_detect(EMPRESA,"MATRIZ"))%>% 
  group_by(EMPRESA) %>% 
  mutate(INDICADOR=trimws(INDICADOR)) %>% 
  summarize(VALOR=(1-((sum(TOTAL[INDICADOR=='PEDIDOS NAO ATENDIDOS']))/sum(TOTAL[INDICADOR=='TOTAL PEDIDOS'])))*100) %>%
  mutate(VALOR=round(VALOR,2)) %>% mutate(INDICADOR="FILL RATE: NAO ATENDIDOS x TOTAL") %>% 
  mutate(DATA=Sys.Date()) %>% as.data.frame() %>% .[,c(1,3,2,4)] 

fillrate_calc_matriz_2_blocos_gx <- fillrate_resumo_1_blocos_gx %>%  
  mutate(INDICADOR=trimws(INDICADOR)) %>% 
  filter(str_detect(EMPRESA,"MATRIZ"))%>% 
  group_by(EMPRESA) %>%
  summarize(VALOR=(1-((sum(TOTAL[INDICADOR=='PEDIDOS CONTROL NAO ATENDIDOS']))/sum(TOTAL[INDICADOR=='TOTAL PEDIDOS CONTROL'])))*100) %>%
  mutate(VALOR=round(VALOR,2)) %>% mutate(INDICADOR="FILL RATE: CONTROL NAO ATENDIDOS x CONTROL TOTAL") %>% 
  mutate(DATA=Sys.Date()) %>% as.data.frame() %>% .[,c(1,3,2,4)] 

fillrate_calc_matriz_3_blocos_gx <- fillrate_resumo_1_blocos_gx %>%  
  mutate(INDICADOR=trimws(INDICADOR)) %>% 
  filter(str_detect(EMPRESA,"MATRIZ"))%>% 
  group_by(EMPRESA) %>%
  summarize(VALOR=(1-((sum(TOTAL[INDICADOR=='PEDIDOS CONTROL NAO ATENDIDOS']))/sum(TOTAL[INDICADOR=='TOTAL PEDIDOS'])))*100) %>%
  mutate(VALOR=round(VALOR,2)) %>% mutate(INDICADOR="FILL RATE: CONTROL NAO ATENDIDOS x TOTAL PEDIDOS") %>% 
  mutate(DATA=Sys.Date()) %>% as.data.frame() %>% .[,c(1,3,2,4)] 


fillrate_calc_blocos_gx <- union_all(fillrate_calc_filiais_blocos_gx,fillrate_calc_matriz_1_blocos_gx) %>% 
  union_all(.,fillrate_calc_matriz_2_blocos_gx) %>% 
  union_all(.,fillrate_calc_matriz_3_blocos_gx) %>% rename(TOTAL=VALOR)



## SAVE CALC SUMMARY 

filewd_fillrate_calc_blocos_gx<-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_calc_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")

save(fillrate_calc_blocos_gx,file =filewd_fillrate_calc_blocos_gx)


## START ORDER DETAILS =========================================================================================

# MATRIZ

fillrate_mp_matriz_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_matriz_blocos_gx.sql')) 

View(fillrate_mp_matriz_blocos_gx)

fillrate_mp_joinville_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_joinville_blocos_gx.sql')) 

View(fillrate_mp_joinville_blocos_gx)

fillrate_mp_criciuma_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_criciuma_blocos_gx.sql')) 

View(fillrate_mp_criciuma_blocos_gx)

fillrate_mp_chapeco_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_chapeco_blocos_gx.sql')) 

View(fillrate_mp_chapeco_blocos_gx)

fillrate_mp_bc_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_bc_blocos_gx.sql')) 

View(fillrate_mp_bc_blocos_gx)


