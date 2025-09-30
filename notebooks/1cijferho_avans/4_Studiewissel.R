# Lees 1CHO in en maak switchbestand (standaard: switch binnen 1 jaar of binnen 3 jaar); 
# switch bepalen voor zittende studenten en niet voor studenten die een diploma hebben behaald of zijn uitgevallen (elkaar uitsluitende categorieën)
# en daarnaast alleen switch voor aaneensluitende studiejaren bepalen, waarmee switch bij studieonderbrekers dus niet wordt meegenomen. 

library(tidyverse)
jaar = 2025

##### Data inlezen #####

# Basisbestand inlezen
if(!"Basisbestand1CHO" %in% ls()){
  Basisbestand1CHO <- readRDS(paste0(here::here(), "/", jaar, "/Basisbestand_1CHO_", jaar, ".RDS"))
}

# Lees diplomabestand in (studenten met een diploma niet als switchers aanmerken)
if(!"Diploma_Avans" %in% ls()){
  Diploma_Avans <- readRDS(paste0(here::here(), "/", jaar, "/Diploma_Avans_", jaar, ".RDS"))
}


# Lees uitvalbestand in (studenten die binnen 1 of 3 jaar zijn uitgevallen niet als switcher aanmerken)
uitval_1_3_jaar = readRDS(file = paste0(here::here(), "/", jaar, "/Uitval_", jaar, ".RDS")) %>%
  
  select(persnr, uitvalxjr) %>% 
  
  mutate(uitval_een_drie = case_when(uitvalxjr == 1 ~ "binnen 1 jaar", # uitvalxjr kan niet kleiner dan 1 zijn , want is gebaseerd op: case_when(status == "Uitgevallen" ~ laatste_jaar_inschrijving + 1 - eerstejaar_Avans)
                                   uitvalxjr > 1 & uitvalxjr <= 3 ~ "binnen 2- en 3de jaar",
                            TRUE ~ "niet uitgevallen eerste drie jaar"),
         uitval_een_drie = factor(uitval_een_drie))


# Lees instroombestand in
if(!"Cohorten_InstroomAvans" %in% ls()){
  Cohorten_InstroomAvans <- readRDS(file = paste0(here::here(), "/", jaar, "/Instroom_cohorten_", jaar, ".RDS"))
}


# Bestanden aanmaken voor zittende studenten: Cohortbestand en diplomabestand koppelen en daarna uitvalbestand koppelen.
# Studenten die zitten na 1 of na 3 jaar zijn studenten die binnen 1 / 3 jaar niet hun diploma hebben behaald of niet zijn uitgevallen binnen 1/3 jaar
koppelbestand = Cohorten_InstroomAvans %>% 
  
  select(-soortdiplomainstelling) %>% 
  
  left_join(Diploma_Avans) %>% 
  
  select(persnr, inschrijvingsjaar, eerstejaar_Avans, firstdiplomajaar1, opleidingsvorm, opleidingsfase) %>% 
  
  mutate(diploma_na_x_jaar = firstdiplomajaar1 - eerstejaar_Avans + 1,
         diploma_een_drie = case_when(diploma_na_x_jaar <= 1 ~ "binnen 1 jaar of daarvoor", # diploma_na_x_jaar kan ook negatief of nul zijn als diploma bv behaald is bij een Ad inschrijving
                                      # die niet actief is op 1 okt en daar diploma in dat jaar behaald en daarna actief op 1 okt instroomt bij ba.
                                      # Hier wel <=1 aanhouden, want studenten die in een inschrijvingsjaar dat niet actief op 1 okt is , maar wel een diploma, bv Ad-diploma
                                      # halen in dat jaar en daarna instromen bij de ba, kunnen niet meer aangemerkt worden als switcher omdat zij een diploma behaald hebben.
                                   diploma_na_x_jaar > 1 & diploma_na_x_jaar <= 3 ~ "binnen 2- en 3de jaar",
                                   TRUE ~ "geen diploma in eerste drie jaar"),
         diploma_een_drie = factor(diploma_een_drie)) %>% 
  
  left_join(uitval_1_3_jaar) 

#Opmerkingen:
# eerstejaar_Avans is gebaseerd op verblijfsjaaractinst == 1, dus na-inschrijvers tellen mee vanaf het eerste volledige studiejaar,
# firstdiplomajaar1 is gebaseerd op het studiejaar waarin diploma is behaald,
# opleidingsvorm en opleidingsfase zijn gebaseerd op jaar van instroom.
  
# Bestanden aanmaken voor zittende studenten na 1 en na 3 jaar
zittend_1jr = koppelbestand %>% 
  
  filter(diploma_een_drie != "binnen 1 jaar of daarvoor") %>% 
  
  filter(uitval_een_drie != "binnen 1 jaar")


zittend_3jr = koppelbestand %>% 
  
  filter(diploma_een_drie != "binnen 1 jaar of daarvoor") %>% 
  
  filter(diploma_een_drie != "binnen 2- en 3de jaar") %>% 
  
  filter(uitval_een_drie != "binnen 1 jaar") %>% 
  
  filter(uitval_een_drie != "binnen 2- en 3de jaar")


##### Bestand aanmaken voor studiewissel binnen 1 jaar##########

# Selecteer eerste en tweede verblijfsjaar uit basisbestand
# Alleen studenten (hoofdinschrijvingen) uit het instroombestand bekijken die niet binnen 1 jaar hun diploma hebben behaald of zijn uitgevallen binnen 1 jaar
Basisbestand1CHO_bewerkt = Basisbestand1CHO %>% 
  
  filter(persnr %in% zittend_1jr$persnr) %>% 
  
  filter(verblijfsjaaractueleinstelling %in% c(1,2) & 
           soortinschrijvingactinstelling == "hoofdinschrijving binnen het domein actuele instelling")


# Van instroom weten we dat voorgaande selecties leiden tot 1 regel per student voor verblijfsjaar 1. Verblijfsjaar 2 zou ook 1 regel per student moeten zijn.
# Check: Aantal regels per student mag dus niet meer dan 2 zijn, anders foutmelding.
Freq_tabel_persnr = Basisbestand1CHO_bewerkt %>% 
  
  count(persnr) 

if(any(Freq_tabel_persnr$n>2)){
  stop("Meer dan twee regels per student in het switchcohortbestand gevonden!")
}

# Van studenten met maar 1 regel in het switchbestand (bv studenten die in het huidige jaar zijn ingestroomd) is geen switch te bepalen, dus alleen studenten met 2 regels selecteren.
Persnr_met_2verblijfsjaren = Freq_tabel_persnr %>% 
  
  filter(n==2)

# In het bewerkte basisbestand alleen de studenten selecteren met 2 regels.
Studiewissel_binnen1jr_Avans_dubbeleregels = Basisbestand1CHO_bewerkt %>% 
  
  filter(persnr %in% Persnr_met_2verblijfsjaren$persnr) 

# Check om te bepalen of aantal verblijfsjaar = 1 gelijk is aan aantal verblijfsjaar = 2
if(sum(Studiewissel_binnen1jr_Avans_dubbeleregels$verblijfsjaaractueleinstelling == 1) != sum(Studiewissel_binnen1jr_Avans_dubbeleregels$verblijfsjaaractueleinstelling == 2)){
  stop("Aantal verblijfsjaren 1 en 2 is niet gelijk")
}

# Bestand aanmaken voor Studiewissel binnen 1 jaar
Studiewissel_binnen1jr_Avans = Studiewissel_binnen1jr_Avans_dubbeleregels %>%    
   
  arrange(persnr, verblijfsjaaractueleinstelling) %>% 
  
  mutate(persnrdubb = ifelse(persnr==lag(persnr),"zelfde student", "andere student"),
         isatdubb = ifelse(isat==lag(isat),"zelfde opleiding", "andere opleiding"),
         studiewissel1jr = ifelse(verblijfsjaaractueleinstelling == 2 & persnrdubb == "zelfde student" & isatdubb == "andere opleiding", "Gewisseld binnen 1 jaar", "Niet gewisseld binnen 1 jaar"),
         verschil_kalenderjaren = inschrijvingsjaar - lag(inschrijvingsjaar),
         opleidingscode_na_switch1jr = ifelse(studiewissel1jr == "Gewisseld binnen 1 jaar", as.character(oplactequivalent), NA),
         opleidingsnaam_na_switch1jr = ifelse(studiewissel1jr == "Gewisseld binnen 1 jaar", as.character(isat), NA),
         opleidingsvorm_na_switch1jr = ifelse(studiewissel1jr == "Gewisseld binnen 1 jaar", as.character(opleidingsvorm), NA),
         opleidingsniveau_na_switch1jr = ifelse(studiewissel1jr == "Gewisseld binnen 1 jaar", as.character(typeHObinnensoortHO), NA),
         HBOsector_na_switch1jr = ifelse(studiewissel1jr == "Gewisseld binnen 1 jaar", as.character(crohoonderdeelactopl), NA)
         ) %>% 
  
  mutate(studiewissel1jr = factor(studiewissel1jr),
         opleidingscode_na_switch1jr = factor(opleidingscode_na_switch1jr),
         opleidingsnaam_na_switch1jr = factor(opleidingsnaam_na_switch1jr),
         opleidingsvorm_na_switch1jr = factor(opleidingsvorm_na_switch1jr),
         opleidingsniveau_na_switch1jr = factor(opleidingsniveau_na_switch1jr),
         HBOsector_na_switch1jr = factor(HBOsector_na_switch1jr)) %>% 
  
  select(persnr,
         verschil_kalenderjaren,
         studiewissel1jr, 
         opleidingscode_na_switch1jr, 
         opleidingsnaam_na_switch1jr, 
         opleidingsvorm_na_switch1jr, 
         opleidingsniveau_na_switch1jr,
         HBOsector_na_switch1jr) %>% 
 
  # alleen switch bepalen voor aaneensluitende kalenderjaren, dus switch na studieonderbreking niet meenemen 
  filter(studiewissel1jr == "Gewisseld binnen 1 jaar", verschil_kalenderjaren == 1)
  


##### Bestand aanmaken voor studiewissel binnen 3 jaar##########

# Voor zittende studenten verblijfsjaren 1 en 4 selecteren (Er wordt geen rekening gehouden met studieonderbrekers en/of wisselaars
# die vaker switchen dan 1 keer) en hoofdinschrijving-> om een bestand te krijgen met 1 regel per student per verblijfsjaar.
Basisbestand1CHO_bewerkt = Basisbestand1CHO %>% 
  
  filter(persnr %in% zittend_3jr$persnr) %>% 
  
  filter(verblijfsjaaractueleinstelling %in% c(1,4) & soortinschrijvingactinstelling == "hoofdinschrijving binnen het domein actuele instelling")

# Van instroom weten we dat voorgaande selecties leiden tot 1 regel per student voor verblijfsjaar 1. Verblijfsjaar 4 zou ook 1 regel per student moeten zijn.
# Check: Aantal regels per student mag dus niet meer dan 2 zijn, anders foutmelding.
Freq_tabel_persnr = Basisbestand1CHO_bewerkt %>% 
  count(persnr) 

if(any(Freq_tabel_persnr$n>2)){
  stop("Meer dan twee regels per student in het switchcohortbestand gevonden!")
}

# Van studenten met maar 1 regel in het switchbestand is geen switch te bepalen, dus alleen studenten met 2 regels selecteren.
Persnr_met_2verblijfsjaren = Freq_tabel_persnr %>% 
  filter(n==2)


# In het bewerkte basisbestand alleen de studenten selecteren met 2 regels.
Studiewissel_binnen3jr_Avans_dubbeleregels = Basisbestand1CHO_bewerkt %>% 
  
  filter(persnr %in% Persnr_met_2verblijfsjaren$persnr) 

# Check om te bepalen of aantal verblijfsjaar = 1 gelijk is aan aantal verblijfsjaar = 4
if(sum(Studiewissel_binnen3jr_Avans_dubbeleregels$verblijfsjaaractueleinstelling == 1) != sum(Studiewissel_binnen3jr_Avans_dubbeleregels$verblijfsjaaractueleinstelling == 4)){
  stop("Aantal verblijfsjaren 1 en 4 is niet gelijk")
}


Studiewissel_binnen3jr_Avans = Studiewissel_binnen3jr_Avans_dubbeleregels %>%    
  
  arrange(persnr, verblijfsjaaractueleinstelling) %>% 
  
  mutate(persnrdubb = ifelse(persnr==lag(persnr),"zelfde student", "andere student"),
         isatdubb = ifelse(isat==lag(isat),"zelfde opleiding", "andere opleiding"),
         studiewissel3jr = ifelse(verblijfsjaaractueleinstelling == 4 & persnrdubb == "zelfde student" & isatdubb == "andere opleiding","Gewisseld binnen 3 jaar", "Niet gewisseld binnen 3 jaar"),
         verschil_kalenderjaren = inschrijvingsjaar - lag(inschrijvingsjaar),
         opleidingscode_na_switch3jr = ifelse(studiewissel3jr == "Gewisseld binnen 3 jaar", as.character(oplactequivalent), NA),
         opleidingsnaam_na_switch3jr = ifelse(studiewissel3jr == "Gewisseld binnen 3 jaar", as.character(isat), NA),
         opleidingsvorm_na_switch3jr = ifelse(studiewissel3jr == "Gewisseld binnen 3 jaar", as.character(opleidingsvorm), NA),
         opleidingsniveau_na_switch3jr = ifelse(studiewissel3jr == "Gewisseld binnen 3 jaar", as.character(typeHObinnensoortHO), NA),
         HBOsector_na_switch3jr = ifelse(studiewissel3jr == "Gewisseld binnen 3 jaar", as.character(crohoonderdeelactopl), NA)) %>% 
  
  mutate(studiewissel3jr = factor(studiewissel3jr),
         opleidingscode_na_switch3jr = factor(opleidingscode_na_switch3jr),
         opleidingsnaam_na_switch3jr = factor(opleidingsnaam_na_switch3jr),
         opleidingsvorm_na_switch3jr = factor(opleidingsvorm_na_switch3jr),
         opleidingsniveau_na_switch3jr = factor(opleidingsniveau_na_switch3jr),
         HBOsector_na_switch3jr = factor(HBOsector_na_switch3jr)) %>% 
  
  select(persnr,
         verschil_kalenderjaren,
         studiewissel3jr, 
         opleidingscode_na_switch3jr, 
         opleidingsnaam_na_switch3jr, 
         opleidingsvorm_na_switch3jr, 
         opleidingsniveau_na_switch3jr,
         HBOsector_na_switch3jr) %>% 
  
  # alleen switch bepalen voor aaneensluitende kalenderjaren, dus switch na studieonderbreking niet meenemen 
  filter(studiewissel3jr == "Gewisseld binnen 3 jaar", verschil_kalenderjaren == 3)

#De twee switchbestanden aan het instroombestand koppelen
if(!"Cohorten_InstroomAvans" %in% ls()){
  Cohorten_InstroomAvans <- readRDS(file = paste0(here::here(), "/", jaar, "/Instroom_cohorten_", jaar, ".RDS"))
}

Switch_ahv_instroomcohort_Avans = Cohorten_InstroomAvans %>% 
  left_join(Studiewissel_binnen1jr_Avans, by="persnr") %>% 
  left_join(Studiewissel_binnen3jr_Avans, by="persnr") %>%
  
  mutate(studiewissel1jr = case_when(!(persnr %in% zittend_1jr$persnr) ~ "Geen switch bepaald",
                                     TRUE ~ studiewissel1jr),
         studiewissel3jr = case_when(!(persnr %in% zittend_3jr$persnr) ~ "Geen switch bepaald",
                                     TRUE ~ studiewissel3jr)) %>%
  
  # Labels voor zittende studenten die niet gewisseld zijn
  mutate(studiewissel1jr = fct_na_value_to_level(studiewissel1jr, "Niet gewisseld binnen 1 jaar"),
         studiewissel3jr = fct_na_value_to_level(studiewissel3jr, "Niet gewisseld binnen 3 jaar"))
           
# Opmerking:
# Switch binnen 3 jaar is incl switch binnen 1 jaar, maar het verschil tussen switch 1 jr en switch 3 jaar is niet de switch  in het 2de of 3de jaar tov het eerste jaar,
# want het wordt alleen op de zittende studenten bepaald. De groepen zijn niet precies hetzelfde, doordat er meer studenten zijn 
# die zijn uitgevallen of diploma behaald in het 2de en 3de jaar. 


# Switchbestand opslaan
saveRDS(Switch_ahv_instroomcohort_Avans, file = paste0(here::here(), "/", jaar, "/Switch_", jaar, ".RDS"))




