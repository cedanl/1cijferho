# Inleesscript voor 1CHO 
# Veerle van Son & Damiëtte Bakx-van den Brink
# mei 2021

library(tidyverse)

####### Bestand inlezen ####### 

jaar = 2025
## TODO , "/" toegevoegd voor jaar
bestand_locatie = paste0(str_remove(here::here(), "1CHO_inlezen_R"), "/", jaar, "/DUO-bestanden/")
## TODO , file.path gebruikt voor pad, dat voegt automatisch scheidingsteken toe, dus kan bijv. bij posities ook
#bestand_locatie = file.path(str_remove(here::here(), "1CHO_inlezen_R"), jaar, "DUO-bestanden")
bron = list.files(bestand_locatie, "^EV299XX\\d{2}\\.\\w{3}", full.names = T)

# Posities uit bestandsbeschrijving
posities = read_csv2(paste0(bestand_locatie, "Posities_1CHO.csv"))

# Run script met labels 
source("Labels inlezen.R")

# Lees ruw 1CHO-bestand in
# TODO: Warning bij nationaliteit 3 kolom, dit wordt bij mijn dummy-bestand een logical omdat er teveel lege waarden in zitten
Basisbestand_1CHO_raw = read_fwf(bron, fwf_widths(posities$aantal_posities, posities$kolomnaam))

overzicht_basisbestand = data.frame(kolomnaam = colnames(Basisbestand_1CHO_raw),
                                    positie = 1:ncol(Basisbestand_1CHO_raw),
                                    type = sapply(Basisbestand_1CHO_raw,class))

####### Data opschonen ####### 

# Lijst van numerieke variabelen
vars_numeriek = c("leeftijdpeildatum1oktober",
                  "diplomajaar", 
                  "verblijfsjaarHO", "verblijfsjaarsoortHO", "verblijfsjrtypeHObinnensoortHO",
                  "verblijfsjaartypeHObinnenHO", "verblijfsjaaractueleopleiding",
                  "verblijfsjaaractueleinstelling", "verblijfsjaaractueleopleidinginstelling",
                  "verblijfsjaaractinsttypehobinnensoortho")

vars_datum = c("datuminschrijving", "datumuitschrijving", "datumtekeningdiploma")


# Zet juiste datatypes
Basisbestand1CHO = Basisbestand_1CHO_raw %>% 
  
  # Zet numerieke variabelen om naar numeriek
  mutate_at(vars_numeriek, as.numeric) %>% 
  
  # Zet datumvariabelen om naar datumtype
  mutate_at(vars_datum, lubridate::ymd) %>%
  
  mutate(
    
    # Voeg labels uit de .asc-bestanden en bestandsbeschrijving toe
    ## Bestanden
    isat = factor(oplactequivalent, levels=isat_labels$code, labels=isat_labels$naam),
    brin = factor(actinstelling, levels = actinstelling_labels$code, labels = actinstelling_labels$naam),
    hoogstevooroplvoorHO = factor(hoogstevooroplvoorHO, levels=hoogstevooropl_labels$code, labels=hoogstevooropl_labels$naam),
    hoogstevooropleidingbinnenHO = factor(hoogstevooropleidingbinnenHO, levels=hoogstevooropl_labels$code, labels=hoogstevooropl_labels$naam),
    
    ## Bestandsbeschrijving
    ### Opleiding
    opleidingsvorm = factor(opleidingsvorm, levels = opleidingsvorm_labels$code, opleidingsvorm_labels$naam), 
    oplfaseact = factor(oplfaseact, levels = oplfaseact_labels$code, labels = oplfaseact_labels$naam),
    crohoonderdeelactopl = factor(crohoonderdeelactopl, levels = crohoonderdeelactopl_labels$code, labels = crohoonderdeelactopl_labels$naam),
    typeHObinnensoortHO = factor(typeHObinnensoortHO, levels = typeHObinnensoortHO_labels$code, labels = typeHObinnensoortHO_labels$naam),
    
    ### Inschrijving
    inschrijvingsvorm = factor(inschrijvingsvorm, levels = inschrijvingsvorm_labels$code, labels = inschrijvingsvorm_labels$naam),
    indicatieactiefoppeildatum = factor(indicatieactiefoppeildatum, levels = indicatieactiefoppeildatum_labels$code, labels = indicatieactiefoppeildatum_labels$naam),
    soortinschrijvingactinstelling = factor(soortinschrijvingactinstelling, levels = soortinschrijvingactinstelling_labels$code, labels = soortinschrijvingactinstelling_labels$naam),
    
    ### Diploma's
    soortdiplomainstelling = factor(soortdiplomainstelling, levels = soortdiplomainstelling_labels$code, labels = soortdiplomainstelling_labels$naam),
    
    ### Persoonsgegevens
    geslacht = factor(geslacht, levels = geslacht_labels$code, labels = geslacht_labels$naam),
    indicatieinternationalestudent = factor(indicatieinternationalestudent, levels = indicatieinternationalestudent_labels$code, labels = indicatieinternationalestudent_labels$naam),
    indicatieEER = factor(indicatieEERoppeildatum1okt, levels = indicatieEER_labels$code, labels = indicatieEER_labels$naam)     
  )


###### Nieuwe kolommen aanmaken ###### 

Basisbestand_1CHO_bewerkt = Basisbestand1CHO %>% 
  mutate(afkomst_int_student = case_when(
    indicatieinternationalestudent=="internationale student" & indicatieEERoppeildatum1okt=="EER-student" ~ "internationale student uit EER", 
    indicatieinternationalestudent=="internationale student" & indicatieEERoppeildatum1okt=="niet-EER-student" ~ "internationale student NIET uit EER",
    indicatieinternationalestudent=="geen internationale student" ~ "geen internationale student"),
    leeftijdgroep_instroom1okt = case_when(leeftijdpeildatum1oktober < 20 ~ "jonger dan 20 jaar",
                                           leeftijdpeildatum1oktober >= 20 & leeftijdpeildatum1oktober < 25  ~ "20-24 jaar",
                                           leeftijdpeildatum1oktober >= 25 & leeftijdpeildatum1oktober < 30  ~ "25-29 jaar",
                                           leeftijdpeildatum1oktober >= 30 & leeftijdpeildatum1oktober < 35  ~ "30-34 jaar",
                                           leeftijdpeildatum1oktober >= 35 & leeftijdpeildatum1oktober < 40  ~ "35-39 jaar",
                                           leeftijdpeildatum1oktober >= 40 & leeftijdpeildatum1oktober < 45  ~ "40-44 jaar",
                                           leeftijdpeildatum1oktober >= 45 ~ "45 jaar of ouder")) %>% 
  mutate(leeftijdgroep_instroom1okt = factor(leeftijdgroep_instroom1okt),
         leeftijdgroep_instroom1okt = forcats::fct_relevel(leeftijdgroep_instroom1okt, "jonger dan 20 jaar", after=0),
         afkomst_int_student = factor(afkomst_int_student),
         oplactequivalent = factor(oplactequivalent))


overzicht_basisbestand_bewerkt = data.frame(kolomnaam = colnames(Basisbestand_1CHO_bewerkt),
                                            positie = 1:ncol(Basisbestand_1CHO_bewerkt),
                                            type = sapply(Basisbestand_1CHO_bewerkt,class))

####### Bestand opslaan ####### 

# Basisbestand opslaan
## TODO: Dit ging fout voor mij omdat er "../" in stond die ik niet kon plaatsen
saveRDS(Basisbestand_1CHO_bewerkt, file = paste0(here::here(), "/", jaar, "/Basisbestand_1CHO_", jaar, ".RDS"))
saveRDS(Basisbestand_1CHO_bewerkt, file = paste0(here::here(), "/", jaar, "/Basisbestand_1CHO_", jaar, ".RDS"))
