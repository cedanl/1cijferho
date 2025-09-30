# Lees de bestanden voor rendement, uitval en studiewissel in en combineer in één bestand 
# Veerle van Son & Damiëtte Bakx-van den Brink
# mei 2021



library(tidyverse)
jaar = 2025

##### Data inlezen #####

# Bestand Rendement, Uitval en Switch inlezen (in eerdere scripts al gekoppeld aan het instroombestand)
Rendement <- readRDS(paste0(here::here(), "/", jaar, "/Rendementen_", jaar, ".RDS"))
Uitval <- readRDS(paste0(here::here(), "/", jaar, "/Uitval_", jaar, ".RDS"))
Switch <- readRDS(paste0(here::here(), "/", jaar, "/Switch_", jaar, ".RDS"))

# Relevante kolommen selecteren
Uitval_kolommen <- Uitval %>% 
  select(persnr, status:uitval3jr)

Switch_kolommen <- Switch %>% 
  select(persnr, studiewissel1jr:HBOsector_na_switch3jr) 
  

message("\n\nSTAAT VAN AVANS --- Voeg alle data samen")


# Kolommen Uitval en Switch toevoegen aan bestand Rendement
Samengevoegd_bestand <- Rendement  %>% 
  left_join(Uitval_kolommen, by="persnr") %>% 
  left_join(Switch_kolommen, by="persnr") %>% 
  
  
# Volgorde kolommen en namen wijzigen  
  select(persnr,
         inschrijvingsjaar, 
         geslacht,
         ## TODO locatie bestaat niet bij mij? ik ben uitgegaan van vestigingsnummer
         #locatie,
         locatie = vestigingsnummer,
         opleidingscode = oplactequivalent,
         opleidingsnaam = isat,
         opleidingsvorm,
         opleidingsniveau = typeHObinnensoortHO,
         ## TODO vooropleiding bestaat niet bij mij? ik ben uitgegaan van hoogstevooropl
         vooropleiding = hoogstevooropl,
         int_student = indicatieinternationalestudent,
         indicatieEER,
         #MBOdomein,
         #VOprofielCM:VOprofielNT,
         HBOsector = crohoonderdeelactopl,
         #avans_sector,
         leeftijd_bij_instroom = leeftijdpeildatum1oktober,
         leeftijdgroep_instroom = leeftijdgroep_instroom1okt,
         postcode4_student_1okt = postcodecijfersstudentop1oktober,
         postcode4_vooropleiding_voorHO = postcodecijfershoogstevooroplvoorHO,
         status, 
         jaar_eerste_diploma_avans = firstdiplomajaar1, 
         soortAvansdiploma = soortdiplomainstelling.y, 
         rendement3jr:rendement8jr, 
         uitvalxjr:HBOsector_na_switch3jr) %>% 
  
  
  # Kolommen omzetten naar factor
  mutate(opleidingscode = factor(opleidingscode),
         ## TODO uitgecomment
         #avans_sector = factor(avans_sector),
         locatie = factor(locatie),
         opleidingsvorm = factor(opleidingsvorm),
         postcode4_student_1okt = factor(postcode4_student_1okt), 
         postcode4_vooropleiding_voorHO = factor(postcode4_vooropleiding_voorHO)) %>% 
  
  # Variabelen opschonen -> naar inleesscript
  ## TODO Dit geeft warnings
  mutate(
         # vooropleiding = fct_recode(vooropleiding, 
         #            "mbo" = "mbo 4",
         #            NULL = "mbo overig",
         #            "onbekend / overig" = "vooropleiding onbekend",
         #            "onbekend / overig" = "overig"),
         
         opleidingsvorm = fct_recode(opleidingsvorm, 
                                     "duaal" = "duaal/coöp"),
         
         HBOsector = fct_recode(HBOsector, 
                                "gedrag & maatschappij" = "gedrag en maatschappij",
                                "taal & cultuur" = "taal en cultuur"),
         ## TODO uitgecomment
         # MBOdomein = fct_recode(MBOdomein, 
         #                        NULL = "vooropleiding anders dan mbo",
         #                        NULL = "mbo algemeen"),
         
         postcode4_student_1okt = fct_recode(postcode4_student_1okt, 
                                NULL = "0010",
                                NULL = "0020",
                                NULL = "0030",
                                NULL = "0040"
                                ),
         
         postcode4_vooropleiding_voorHO = fct_recode(postcode4_vooropleiding_voorHO, 
                                             NULL = "0010",
                                             NULL = "0020",
                                             NULL = "0030",
                                             NULL = "0040"
         ),
         
         opleidingsniveau = fct_recode(opleidingsniveau, 
                                       NULL = "postinitiele master",
                                       bachelor = "bachelor-opleiding",
                                       master = "master-opleiding"),
         
         leeftijdgroep_instroom = fct_collapse(leeftijdgroep_instroom,
                                               "30+ jaar" = c("30-34 jaar", "35-39 jaar", "40-44 jaar", "45 jaar of ouder"))
         
                                     ) %>% 
  
  # Gecombineerde kolommen
  mutate(uitval = case_when(uitvalxjr == 1 ~ "Uitgevallen binnen 1 jaar",
                            uitvalxjr %in% 2:3 ~ "Uitgevallen in 2e of 3e jaar",
                            uitvalxjr > 3 ~ "Uitgevallen na 3 jaar",
                            TRUE ~ "Niet uitgevallen"),
         
         studiewissel = case_when(studiewissel1jr == "Gewisseld binnen 1 jaar" ~ "Gewisseld binnen 1 jaar",
                                  studiewissel3jr == "Gewisseld binnen 3 jaar" ~ "Gewisseld in het 2e of 3e jaar",
                                  TRUE ~ "Niet gewisseld"),
         
         rendement = case_when(rendement5jr == "Diploma binnen 5 jaar" ~ "Diploma binnen 5 jaar",
                               rendement8jr == "Diploma binnen 8 jaar" ~ "Diploma binnen 5-8 jaar",
                               rendement8jr == "Diploma na 8 jaar" ~ "Diploma na 8 jaar",
                               rendement8jr == "Geen diploma" ~ "Geen diploma")
                                  
  ) 

# Samengevoegd bestand opslaan
message("STAAT VAN AVANS --- Sla gecombineerde data op")
saveRDS(Samengevoegd_bestand, file = paste0(here::here(), "/", jaar, "/Indicatoren_SvA_", jaar, ".RDS"))
