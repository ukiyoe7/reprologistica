## FILL RATE BLOCOS GX
## MF0317; MF0322; MF0323; MF0385; MF0391; MF0396; MF0397; MF0398; MF0399; MF0404; MF0405; MF0417; MF0418; MF0419; MF0426; 


library(tidyverse)
library(reshape2)
library(DBI)
library(googlesheets4)
library(readr)
con2 <- dbConnect(odbc::odbc(), "reproreplica")


### TOTAL PEDIDOS

tot_ped_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/tot_ped_blocos_gx.sql')) 


### TOTAL PEDIDOS NAO ATENDIDOS

tot_ped_natd_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/tot_ped_natd_blocos_gx.sql')) 


### PEDIDOS CONTROL

ped_control_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/ped_control_blocos_gx.sql')) 


### PEDIDOS QUEBRAS

ped_quebras_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/ped_quebras_blocos_gx.sql')) 


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


fillrate_mp_joinville_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_joinville_blocos_gx.sql')) 


fillrate_mp_criciuma_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_criciuma_blocos_gx.sql')) 


fillrate_mp_chapeco_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_chapeco_blocos_gx.sql')) 


fillrate_mp_bc_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_bc_blocos_gx.sql')) 


fillrate_mp_control_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_control_blocos_gx.sql')) 


fillrate_mp_control_matriz_blocos_gx <- dbGetQuery(con2,statement = read_file('C:/Users/Repro/Documents/R/LOGISTICA/LOGISTICA/SQL/fillrate_mp_control_matriz_blocos_gx.sql')) 



# ADD ALL BLOCKS

fillrate_mp_emp_blocos_gx <- union_all(fillrate_mp_matriz_blocos_gx,fillrate_mp_joinville_blocos_gx) %>% 
  union_all(.,fillrate_mp_criciuma_blocos_gx) %>% 
  union_all(.,fillrate_mp_chapeco_blocos_gx) %>% 
  union_all(.,fillrate_mp_bc_blocos_gx) %>% 
  union_all(.,fillrate_mp_control_blocos_gx) %>% 
  union_all(.,fillrate_mp_control_matriz_blocos_gx) %>% 
  filter(substr(CHAVE,1,2)=="MF") %>% 
  mutate(DATA=Sys.Date()) %>% distinct() %>% as.data.frame()


# SAVE CURRENT DAY

filewd_emp_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_mp_emp_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")

save(fillrate_mp_emp_blocos_gx,file =filewd_emp_blocos_gx)


# AGGREG MP AND SUM

fillrate_mp_emp_resumo_blocos_gx <- fillrate_mp_emp_blocos_gx %>% group_by(INDICADOR,CHAVE,DESCRICAO_CHAVE) %>% 
  summarize(QTD=sum(QTD)) %>% 
  arrange(desc(QTD)) %>% 
  mutate(DATA=Sys.Date()) %>%  as.data.frame()

filewd_emp_resumo_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_mp_emp_resumo_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")


save(fillrate_mp_emp_resumo_blocos_gx,file =filewd_emp_resumo_blocos_gx)


##  SEND EMAIL  ==============================================================================================

library(gmailr)
library(xlsx)

gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\sendmail.json")


#RESUMO
filewd_fillrate_resumo_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_resumo_2_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")

fillrate_resumo_blocos_gx <- get(load(filewd_fillrate_resumo_blocos_gx)) %>% as.data.frame()

#CALCULO
filewd_fillrate_calc_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_calc_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")

fillrate_calc_blocos_gx <- get(load(filewd_fillrate_calc_blocos_gx))

#DADOS MP
filewd_emp_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_mp_emp_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")

fillrate_mp_emp_blocos_gx <- get(load(filewd_emp_blocos_gx))

## RESUMO MP
filewd_emp_resumo_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_mp_emp_resumo_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".RData")

fillrate_mp_emp_resumo_blocos_gx <- get(load(filewd_emp_resumo_blocos_gx))


filewd_emp_mail_blocos_gx <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_mp_emp_blocos_gx","_",format(Sys.Date(),"%d_%m_%y"),".xlsx")

write.xlsx(fillrate_resumo_blocos_gx, file = filewd_emp_mail_blocos_gx,row.names=FALSE,sheetName = "RESUMO",showNA=FALSE)
write.xlsx(fillrate_calc_blocos_gx, file = filewd_emp_mail_blocos_gx,row.names=FALSE,sheetName = "CALCULO_FILLRATE",showNA=FALSE,append = TRUE)
write.xlsx(fillrate_mp_emp_blocos_gx, file = filewd_emp_mail_blocos_gx,row.names=FALSE,sheetName = "DADOS", append = TRUE)
write.xlsx(fillrate_mp_emp_resumo_blocos_gx, file = filewd_emp_mail_blocos_gx,row.names=FALSE,sheetName = "DADOS2", append = TRUE)

filewd_emp_mail_blocos_gx2 <-  paste0("C:\\Users\\Repro\\Documents\\R\\LOGISTICA\\LOGISTICA\\BASES\\fillrate_mp_emp_blocos_gx2","_",format(Sys.Date(),"%d_%m_%y"),".csv")

write.csv2(fillrate_mp_emp_blocos_gx, file = filewd_emp_mail_blocos_gx2,row.names=FALSE)


mymail_fillrate_blocos_gx <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,debora.rocha@repro.com.br,silvano.silva@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATORIO FILL RATE BLOCOS GX") %>%
  gm_text_body("Segue Anexo relatorio dos últimos 7 dias .Esse e um email automatico.") %>% 
  gm_attach_file(filewd_emp_mail_blocos_gx) 

gm_send_message(mymail_fillrate_blocos_gx)



