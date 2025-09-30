# Lees 1CHO in, maak uitstroombestand, koppel diplomabestand en maak uitvalbestand (standaard: uitval binnen 1 en binnen 3 jaar) 

library(tidyverse)
jaar = 2025

##### Data inlezen #####

# Basisbestand inlezen
if(!"Basisbestand1CHO" %in% ls()){
  Basisbestand1CHO <- readRDS(paste0(here::here(), "/", jaar, "/Basisbestand_1CHO_", jaar, ".RDS"))
}


# Uitstroombestand aanmaken (1 regel per student)
Uitstroom_Avans <- Basisbestand1CHO %>% 
  
  # Selecteer per student het laatste jaar van inschrijving bij hoofdinschrijving door 1ste rij te selecteren (distinct) na sorteren
  # van hoog naar laag op inschrijvingsjaar. Daarnaast filteren op hoofdinschrijving door sorteren op 
  # soortinschrijvingactinstelling, want dan wordt de volgorde van de levels aangehouden, dus 1= hoofdinschrijving, 2 = neveninschrijving etc. 
  group_by(persnr) %>% 
  
  arrange(desc(inschrijvingsjaar), soortinschrijvingactinstelling, .by_group = TRUE) %>% 
  
  distinct(persnr, .keep_all = TRUE) %>% 
  
  ungroup()  



# Foutmelding als er dubbele studentnummers in het diplomabestand zitten
if(any(duplicated(Uitstroom_Avans$persnr))){
  stop("Dubbele studentnummers in het uitvalbestand gevonden!")
}

# Nieuwe variabele aanmaken om laatste jaar van inschrijving aan te geven
  # Let op: laatste_jaar_inschrijving = NA voor studenten die nog studeren en als inschrijvingsjaar niet bekend is. 
  # Anders: laatste_jaar_inschrijving is het inschrijvingsjaar
Uitstroom_Avans_bewerkt = Uitstroom_Avans %>% 
  
  mutate(laatste_jaar_inschrijving = case_when(
    inschrijvingsjaar == jaar-1 ~ NA_real_,
    is.na(inschrijvingsjaar) ~ NA_real_,
    TRUE ~ as.numeric(inschrijvingsjaar)))


# Uitvalbestand aanmaken, waarbij uitstroom door uitval onderscheiden wordt van uitstroom door diploma behaald 

# Uitval bepalen door variabele status aan te maken: Net als VH onderscheid maken tussen 
# studenten die diploma hebben behaald, studenten die uitgevallen zijn en 
# studenten die nog student zijn en dus nog ingeschreven staan (zittende studenten). Dit zijn elkaar uitsluitende categorieën. 
# Studenten die een diploma behalen in het lopend studiejaar en daarna verder studeren, worden in de kolom status 
# op Diploma behaald gezet. Zij kunnen daarna niet meer uitvallen en worden niet meer meegeteld als zittende student. 
# Om de uitval te bepalen dus het diplomabestand koppelen aan het uitstroombestand
# en uitvalbestand aanmaken.


if(!"Diploma_Avans" %in% ls()){
  Diploma_Avans <- readRDS(paste0("../", jaar, "/Diploma_Avans_", jaar, ".RDS"))
}

if(!"Cohorten_InstroomAvans" %in% ls()){
  # Om het uitvalbestand te kunnen koppelen aan het instroombestand, eerst het instroombestand inlezen en dan koppelen
  Cohorten_InstroomAvans <- readRDS(file = paste0("../", jaar, "/Instroom_cohorten_", jaar, ".RDS"))
}

# Aanmaken bestand voor eerste jaar bij Avans om variabele voor uitval na x jaar te kunnen berekenen
Eerstejaar = Cohorten_InstroomAvans %>% 
  select(persnr, eerstejaar_Avans)

# Opmerking:
# Waarom gebruik maken van eerstejaar_Avans en niet eerstejaardezeactinstelling? Deze hoeven niet gelijk te zijn, want
# voor eerstejaardezeactinstelling kan het eerste inschrijvingsrecord ook een na-inschrijving betreffen. Het eerstejaar_Avans
# is gebaseerd op verblijfsjaar == 1, dus is het eerste volledige studiejaar bij na-inschrijving. Keuze is gemaakt bij het
# aanmaken van het instroombestand om variabele verblijfsjaar = 1 te gebruiken. Dit houdt in dat we voor na-inschrijvers het 
# eerstvolgende studiejaar gebruiken als jaar van instroom. Dus hiermee is tevens bepaald dat we eerstejaar_Avans gebruiken als jaar van instroom.


Uitval_Avans <- Uitstroom_Avans_bewerkt %>% 
  
  left_join(Diploma_Avans, by="persnr") %>% 
  
  left_join(Eerstejaar, by="persnr") %>% 
  
  mutate(status = case_when(diploma == "Diploma behaald (excl. propedeuse)" ~ "Diploma behaald",
                            
                            inschrijvingsjaar == jaar-1 ~ "Zittend",
                            
                            TRUE ~ "Uitgevallen")) %>% 
  
  mutate(status = as.factor(status)) %>% 
  
  mutate(uitvalxjr = case_when(status == "Uitgevallen" ~ laatste_jaar_inschrijving + 1 - eerstejaar_Avans),
         
         uitval1jr = case_when(is.na(uitvalxjr) ~ NA_character_,
                               uitvalxjr == 1 ~ "Uitgevallen binnen 1 jaar",
                               TRUE ~ "Na 1 jaar nog ingeschreven of diploma behaald"),
         
         uitval3jr = case_when(is.na(uitvalxjr) ~ NA_character_,
                               uitvalxjr <= 3 ~ "Uitgevallen binnen 3 jaar", 
                               TRUE ~ "Na 3 jaar nog ingeschreven of diploma behaald")) %>% 
  # Omzetten naar factors
  mutate(uitval1jr = factor(uitval1jr),
         uitval3jr = factor(uitval3jr)) %>% 
  
  select(persnr, laatste_jaar_inschrijving, diploma, status, starts_with("uitval"))

# Opmerkingen:
# Bestand Eerstejaar heeft minder regels dan Uitstroom bestand, omdat er bij eerstejaar via de variabele verblijfsjaaractinstelling == 1
# geselecteerd wordt op actief op peildatum 1 okt. Dit doen we niet bij uitval, dus studenten die wel een uitvalregel hebben, 
# maar die geen actieve inschrijving bij instroom hebben verschijnen wel in het bestand, maar met NA voor eerstejaar_Avans (bv na-inschrijvers die in hun eerste jaar meteen uitvallen).

# Studenten die geen eerstejaar Avans hebben, omdat ze toen geen actieve inschrijving hadden, hebben in
# het uitvalbestand voor uitvalxjr = NA, dus moeten ook NA voor uitval1jr en uitval3jr krijgen.
# Bij de koppeling met het instroombestand vallen deze studenten er sowieso uit, want instroombestand 
# is gebaseerd op verblijfsjaaractinst == 1 en dat gaat uit van actief op 1 okt.


###### Koppeling met instroomcohorten ###### 


# Uitvalbestand: combineer informatie uit cohortbestand en diplomabestand
Uitval_ahv_instroomcohort_Avans = Cohorten_InstroomAvans %>% 
  left_join(Uitval_Avans, by="persnr") 

# Check of alle statussen zijn ingevuld
if(any(is.na(Uitval_ahv_instroomcohort_Avans$status))){
  stop("Niet alle statussen zijn gevuld")
}



# Uitvalbestand opslaan
saveRDS(Uitval_ahv_instroomcohort_Avans, file = paste0(here::here(), "/", jaar, "/Uitval_", jaar, ".RDS"))


