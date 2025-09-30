# Lees 1CHO in, maak diplomabestand, koppel aan instroombestand en maak rendementsbestand 

library(tidyverse)
jaar = 2025


##### Data inlezen #####

# Basisbestand inlezen (als het nog niet is ingelezen)
if(!"Basisbestand1CHO" %in% ls()){
  Basisbestand1CHO <- readRDS(paste0(here::here(), "/", jaar, "/Basisbestand_1CHO_", jaar, ".RDS"))
}


## Lijst van mogelijke diploma's excl. propedeusediploma's
diplomas = c("Hoofd-bachelor-diploma binnen de actuele instelling",
             "Neven-bachelor-diploma binnen de actuele instelling",
             "Hoofd-master-diploma binnen de actuele instelling",
             "Neven-master-diploma binnen de actuele instelling",
             "Hoofd-doctoraal-diploma binnen de actuele instelling",
             "Neven-doctoraal-diploma binnen de actuele instelling",
             "Hoofddiploma beroepsfase/voortgezet binnen de actuele instelling",
             "Nevendiploma beroepsfase/voortgezet binnen de actuele instelling",
             "Hoofddiploma associate degree binnen de actuele instelling",
             "Nevendiploma associate degree binnen de actuele instelling",
             "Hoofddiploma postinitiele master binnen de actuele instelling",
             "Nevendiploma postinitiele master binnen de actuele instelling")


# Selecteren op eerste diploma behaald bij Avans, m.u.v. propedeusediploma
Diploma_Avans = Basisbestand1CHO %>% 
  
  filter(soortdiplomainstelling %in% diplomas) %>% 
  
  group_by(persnr) %>% 
  
  arrange(diplomajaar, .by_group = TRUE) %>% 
  
  distinct(persnr, .keep_all = TRUE) %>% 
  
  ungroup() %>% 
  
  mutate(firstdiplomajaar1 = diplomajaar,
         verblijfsjaar_diploma = verblijfsjaaractueleinstelling,
         diploma = "Diploma behaald (excl. propedeuse)") %>% 
  
  select(persnr, firstdiplomajaar1, verblijfsjaar_diploma, soortdiplomainstelling, diploma)


# Foutmelding als er dubbele studentnummers in het diplomabestand zitten
if(any(duplicated(Diploma_Avans$persnr))){
  stop("Dubbele studentnummers in het diplomabestand gevonden!")
}

# Opmerking:
# Diplomabestand bevat studenten die in verblijfsjaar 0 al hun diploma halen. Reden: als 
# de inschrijving niet actief is op 1 okt en het diploma in een nainschrijvingsjaar behaald wordt, 
# is verblijfsjaar=0.

# Diplomabestand opslaan, omdat dit bestand bij bepalen van uitval nodig is
saveRDS(Diploma_Avans, file = paste0(here::here(), "/", jaar, "/Diploma_Avans_", jaar, ".RDS"))



###### Koppeling met instroomcohorten ###### 

#Instroombestand inlezen (als het nog niet ingelezen is)
if(!"Cohorten_InstroomAvans" %in% ls()){
  Cohorten_InstroomAvans <- readRDS(file = paste0(here::here(), "/", jaar, "/Instroom_cohorten_", jaar, ".RDS"))
}

# Rendementsbestand aanmaken: combineer cohortbestand en diplomabestand
# Standaard diploma binnen 5 en 8 jaar, maar binnen 3 jaar toegevoegd voor Ad opleidingen
Rendementen_ahv_instroomcohort_Avans = Cohorten_InstroomAvans %>% 
  
  left_join(Diploma_Avans, by="persnr") %>% 
  
  mutate(studieduur = firstdiplomajaar1 - eerstejaar_Avans + 1,
         rendement3jr = case_when(
      firstdiplomajaar1 >= eerstejaar_Avans & 
        firstdiplomajaar1 <= eerstejaar_Avans + 2 ~ "Diploma binnen 3 jaar",
      
      firstdiplomajaar1 >= eerstejaar_Avans & 
        firstdiplomajaar1 >  eerstejaar_Avans + 2 ~ "Diploma na 3 jaar",
      
      is.na(firstdiplomajaar1) ~ "Geen diploma",
      
      firstdiplomajaar1 < eerstejaar_Avans ~ "Onbekend, want diplomajaar ligt voor eerste jaar bij Avans"),
      
        rendement5jr = case_when(
      firstdiplomajaar1 >= eerstejaar_Avans & 
        firstdiplomajaar1 <= eerstejaar_Avans + 4 ~ "Diploma binnen 5 jaar",
      
      firstdiplomajaar1 >= eerstejaar_Avans & 
        firstdiplomajaar1 > eerstejaar_Avans + 4 ~ "Diploma na 5 jaar",
      
      is.na(firstdiplomajaar1) ~ "Geen diploma", 
      
      firstdiplomajaar1 < eerstejaar_Avans ~ "Onbekend, want diplomajaar ligt voor eerste jaar bij Avans"),
    
    
    # Bereken rendement8jr: diploma binnen 8 jaar, na 8 jaar of geen diploma
    rendement8jr = case_when(
      firstdiplomajaar1 >= eerstejaar_Avans & 
        firstdiplomajaar1 <= eerstejaar_Avans + 7 ~ "Diploma binnen 8 jaar",
      
      firstdiplomajaar1 >= eerstejaar_Avans & 
        firstdiplomajaar1 > eerstejaar_Avans + 7 ~ "Diploma na 8 jaar",
      
      is.na(firstdiplomajaar1) ~ "Geen diploma", 
      
      firstdiplomajaar1 < eerstejaar_Avans ~ "Onbekend, want diplomajaar ligt voor eerste jaar bij Avans")) %>% 
  
  mutate(across(starts_with("rendement"), as.factor))

#Opmerkingen:
# eerstejaar_Avans is gebaseerd op verblijfsjaaractueleinstelling, dus voor na-inschrijvers is dit het eerste gehele studiejaar.
# Bij de berekende variabele verblijfsjaar_diploma worden alleen de jaren geteld met een actieve inschrijving
# Bij de berekende variabele studieduur worden alle tussenliggende jaren geteld (ook tussenjaren meegeteld)

# Ook naar het gebruik van verblijfsjaar_diploma gekeken voor rendementsbepalingen.
# Dan blijkt dat als de inschrijving niet actief is op 1 okt, het verblijfsjaar_diploma = 0,
# omdat alleen actieve jaren worden meegenomen bij de berekening van het verblijfsjaar. 
# We kiezen ervoor om rendementen te bepalen op de oude manier (zelfde methode als bij studieduur),
# waarbij we tussenliggende jaren meenemen. Voor studenten die jaren niet hebben gestudeerd lijkt het dan alsof ze 
# langstudeerders zijn, maar in werkelijkheid zijn ze lang gestopt.

# berekende variabele studieduur kan er eventueel uit, want wordt niet gebruikt in scripts indicatoren


# Rendementbestand opslaan
saveRDS(Rendementen_ahv_instroomcohort_Avans, file = paste0(here::here(), "/", jaar, "/Rendementen_", jaar, ".RDS"))



