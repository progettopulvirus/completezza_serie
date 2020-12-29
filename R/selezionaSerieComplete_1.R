rm(list=objects())
library("tidyverse")
library("RPostgreSQL")
library("sf")
library("furrr")
library("visdat")
library("lubridate")
##plan(multicore,workers=20)
options(error=browser)

#serie valide ma non in raf-170. Verificato chesono valide
#IT1121A;PIEMONTE
#IT1826A;LOMBARDIA
#IT1193A;BASILICATA


### parametri
PARAM<-"pm10"

annoI<-2013
annoF<-2020

#mettere TRUE per invalidare i dati in modo di testare la routine di selezione delle serie valide
INVALIDA<-FALSE
PERCENTUALE_DA_INVALIDARE<-0.25 

#Quanti mesi validi debbono esserci nel 2020? Tutti i mesi debbono essere validi? C'è un margine di tolleranza?
NUMERO_MESI_VALIDI_ULTIMO_ANNO<-6
###


numeroAnni<-(annoF)-annoI+1
SOGLIA_ANNI_VALIDI<-floor((numeroAnni-1)*0.75)

numeroGiorni<-31
SOGLIA_GIORNI_VALIDI<-floor(numeroGiorni*0.75)

if(file.exists(glue::glue("stazioniNonValide_{PARAM}.csv"))) file.remove(glue::glue("stazioniNonValide_{PARAM}.csv"))
if(file.exists(glue::glue("stazioniValide_{PARAM}.csv"))) file.remove(glue::glue("stazioniValide_{PARAM}.csv"))
if(file.exists(glue::glue("numeroStazioniValidePerRegione_{PARAM}.csv"))) file.remove(glue::glue("numeroStazioniValidePerRegione_{PARAM}.csv"))
  
if(file.exists(glue::glue("{PARAM}.csv"))){ 
    
  read_delim(glue::glue("{PARAM}.csv"),delim=";",col_names = TRUE,col_types = cols(value=col_double()))->datiTutti
  read_delim(glue::glue("ana.csv"),delim=";",col_names = TRUE)->ana
  
  left_join(datiTutti,ana[,c("station_eu_code","regione")])->datiTutti
  
}else{ 
  
  dbDriver("PostgreSQL")->mydrv
  dbConnect(drv=mydrv,dbname="pulvirus",host="10.158.102.164",port=5432,user="srv-pulvirus",password="pulvirus#20")->myconn
  dbReadTable(conn=myconn,name = c(PARAM),)->datiTutti
  suppressWarnings({dbReadTable(conn=myconn,name = c("stazioni_aria"))->ana})
  dbDisconnect(myconn)
  
  #le stringhe NA vengono lette come carattere
  suppressWarnings(mutate(datiTutti,value=as.double(value))->datiTutti)
  
  datiTutti %>%
    mutate(mm=lubridate::month(date)) %>%
    mutate(yy=lubridate::year(date))->datiTutti
  
  write_delim(datiTutti,glue::glue("{PARAM}.csv"),delim=";",col_names = TRUE)
  write_delim(ana,glue::glue("ana.csv"),delim=";",col_names = TRUE)
  
  
}#fine if 

###########################
#
#  Scrittura output
#
###################

nonValida<-function(codice,regione,param,error=""){
  sink(glue::glue("stazioniNonValide_{param}.csv"),append=TRUE)
  cat(paste0(glue::glue("{codice};{error};{regione}"),"\n")) 
  sink()
}#fine nonValida 

valida<-function(codice,regione,param){
  sink(glue::glue("stazioniValide_{param}.csv"),append=TRUE)
  cat(paste0(glue::glue("{codice};{regione}"),"\n"))
  sink()
}#fine nonValida 

purrr::partial(nonValida,param=PARAM)->nonValida
purrr::partial(valida,param=PARAM)->valida

###########################

### Inizio programma

left_join(datiTutti,ana[,c("station_eu_code","regione")])->datiTutti

purrr::walk(unique(ana$regione),.f=function(nomeRegione){ 
  
    print(nomeRegione)

    datiTutti %>%
      filter(regione==nomeRegione)->dati

    #La regione ha dati? 
    if(!nrow(dati)) return()
  
    #Ciclo su codici delle stazioni della regione 
    purrr::map(unique(dati$station_eu_code),.f=function(codice){ 
      
    
    #if(codice=="IT1193A") browser()
      
      dati %>%
        filter(station_eu_code==codice)->subDati
      
      if(!nrow(subDati)){ 
        nonValida(codice,regione =nomeRegione)
        return()
      }
      
      
      subDati %>%
        filter(!is.na(value)) %>%
        group_by(yy,mm) %>%
        summarise(numeroDati=n()) %>%
        ungroup()->ndati
      
     
    
      #elimino mesi con meno del 75% di dati disponibili  
      ndati %>%
        mutate(meseValido=case_when(numeroDati>=SOGLIA_GIORNI_VALIDI~1,
                                    TRUE~0))->ndati
      
      #aggiungo stagione  
      ndati %>%
        mutate(seas=case_when(mm %in% c(1,2,12)~1,
                              mm %in% c(3,4,5)~2,
                              mm %in% c(6,7,8)~3,
                              TRUE~4)) %>%
        group_by(yy,seas) %>%
        summarise(stagioneValida=sum(meseValido)) %>%
        ungroup()->ndati2
      
      #elimino le stagioni con meno di due mesi validi
      ndati2 %>%
        filter(stagioneValida>=2)->ndati2
      
      #nessuna stagione valida: serie sfigata (esiste?)  
      if(!nrow(ndati2)){ 
        nonValida(codice,regione=nomeRegione,error="NessunaStagioneValida")
        return()
      }
      
      #cerchiamo gli anni validi
      ndati2 %>%
        group_by(yy) %>%
        summarise(annoValido=n()) %>%
        ungroup()->ndati3
    
      #un anno e' valido se ha le 4 stagioni valide  
      ndati3 %>%
        filter((annoValido==4) & (yy<annoF))->ndati3
      
      nrow(ndati3)->numeroAnniValidi
      
      annoFP<-annoF-1
      
      if(!numeroAnniValidi){ 
        nonValida(codice,regione=nomeRegione,error=glue::glue("Nessun_Anno_Valida_{annoI}_{annoFP}"))
        return()  
      } 
      
      if(!(all((2016:2019) %in%  ndati3$yy)) ){ 
        nonValida(codice,regione=nomeRegione,error=paste0("Pochi_anni_validi (",numeroAnniValidi,")" ))
        return()  
      }
      
      #verifico il 2020
      ndati %>%
        filter(yy==annoF)->annoFinale
      
      #nessun anno valido per il 2020  
      if(!nrow(annoFinale)){ 
        nonValida(codice,regione=nomeRegione,error="2020 non disponibile")
        return()
      }
    
      sum(annoFinale[annoFinale$mm %in% (1:NUMERO_MESI_VALIDI_ULTIMO_ANNO),]$meseValido)->somma 
    
      #2020 non suff. completo  
      if(somma<NUMERO_MESI_VALIDI_ULTIMO_ANNO){
        nonValida(codice,regione=nomeRegione,error ="2020 non completo")
        return()
      }  
      
      ndati3[nrow(ndati3)+1,]<-as.list(c(2020,4))
      names(ndati3)<-c("yy",codice)
    
      valida(codice,regione=nomeRegione)
      
      ndati3
      
    })->listaOut
    

    
    purrr::compact(listaOut)->listaOut
    length(listaOut)->numeroStazioniValide
    
    sink(glue::glue("numeroStazioniValidePerRegione_{PARAM}.csv"),append=TRUE)
    cat(paste0(nomeRegione,";",numeroStazioniValide,"\n"))
    sink()
    
    if(!numeroStazioniValide) return()
      
    ##### Fine mappa stazioni
    purrr::reduce(listaOut,full_join)->dfFinale
    

    
    names(dfFinale)[!grepl("^yy",names(dfFinale))]->codiciStazioniSelezionate
    
    dati %>%
      filter(station_eu_code %in% codiciStazioniSelezionate)->daScrivere
    
    if(nrow(daScrivere)) write_delim(daScrivere,file=glue::glue("{PARAM}_{nomeRegione}.csv"),delim=";",col_names = TRUE)
    
    
    #i nomi del detaframe sono le stazioni valide
    write_delim(dfFinale,glue::glue("completezzaAnni_{PARAM}_{nomeRegione}.csv"),delim=";",col_names = TRUE)

    
})#fine walk su regione 
