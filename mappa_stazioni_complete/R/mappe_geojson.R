rm(list=objects())
library("tidyverse")
library("geojsonio")
library("sf")

PARAM<-"pm10"

read_delim("ana.csv",delim=";",col_names = TRUE)->ana


list.files(pattern="^completezza.+csv$")->ffile

purrr::map_dfr(ffile,.f=function(nomeFile){ 
  
  read_delim(nomeFile,delim=";",col_names=TRUE)->dati
  
  ana %>%
    filter(station_eu_code %in% names(dati))

  
})->anagraficaStazioniComplete

st_as_sf(anagraficaStazioniComplete,coords = c("st_x","st_y"),crs=4326)->sfAna

geojsonio::geojson_write(sfAna,file=glue::glue("stazioniComplete_{PARAM}.geojson"))
