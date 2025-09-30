# Lees 1CHO in en maak cohortbestand (één regel per student)

library(tidyverse)
jaar = 2025


##### Data inlezen #####

#Basisbestand inlezen
Basisbestand1CHO <- readRDS(paste0(here::here(), "/", jaar, "/Basisbestand_1CHO_", jaar, ".RDS"))

## Maak bestand instroomcohort (1 regel per student)
# Selecteer op hoofdinschrijving
# Selecteer op inschrijvingen actief op 1 oktober -> automatisch bij gebruik variabele verblijfsjaar, want deze filtert op actief op 1 okt. 
# Selecteren op verblijfsjaar==1 (nieuwe instromers) 


Cohorten_InstroomAvans = Basisbestand1CHO %>% 
  ## TODO Ik moest hbo in wo wijzigen omdat mijn demo bestand alleen wo heeft
  filter(soortHO %in% c("hoger beroepsonderwijs", "wo")) %>%  
  
  filter(soortinschrijvingactinstelling == "hoofdinschrijving binnen het domein actuele instelling",
         verblijfsjaaractueleinstelling == 1) %>% 
  
  mutate(eerstejaar_Avans = inschrijvingsjaar)
    
 
# Foutmelding als er dubbele studentnummers in het instroomcohortbestand zitten
if(any(duplicated(Cohorten_InstroomAvans$persnr))){
  stop("Dubbele studentnummers in het instroomcohortbestand gevonden!")
}



#Instroombestand opslaan
saveRDS(Cohorten_InstroomAvans, file = paste0(here::here(), "/", jaar, "/Instroom_cohorten_", jaar, ".RDS"))

