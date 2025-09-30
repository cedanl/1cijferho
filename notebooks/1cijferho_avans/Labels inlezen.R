# Namen en labels inlezen

# Handmatig inlezen uit Bestandsbeschrijving_1cyferho_2022_v1.1.txt:
## oplfaseact: (Opleidingsfase actueel)
## opleidingsvorm


# ISAT-codes en bijbehorende labels inlezen (opleidingen)
isat_labels = read_fwf(paste0(bestand_locatie, "Dec_isat.asc"), 
               fwf_widths(c(5, NA), c("code", "naam")))


# BRIN-codes en bijbehorende labels inlezen (onderwijsinstellingen)
actinstelling_labels = read_fwf(paste0(bestand_locatie, "Dec_actuele_instelling.asc"), 
                       fwf_widths(c(4, NA), c("code", "naam")))

# Hoogste vooropleiding voor ho en bijbehorende labels inlezen (onderwijsinstellingen)
hoogstevooropl_labels = read_fwf(paste0(bestand_locatie, "Dec_vopl.asc"), 
                                fwf_widths(c(5, NA), c("code", "naam")))

############# Uit Bestandsbeschrijving_1cyferho ############# 

# Opleidingsvorm
## 3 = coöp-student of duaal onderwijs (vanaf het studiejaar 1998-1999)
opleidingsvorm_labels = data.frame(code = c(1,2,3), naam = c("voltijd", "deeltijd", "duaal/coöp")) 

## Opleidingsfase actueel
# datapasta::vector_paste()
# dput(substring(lijst, 1,1))
# dput(substring(lijst, 5))


oplfaseact_labels = data.frame(code = c("A", "B", "D", "I", "K", "M", "O", "P", "Q", "S", "Z"), 
                        naam = c("associate degree", "bachelor", "propedeuse bachelor of propedeuse associate degree", 
                                   "initiële opleiding (dit betreft voornamelijk ongedeelde opleidingen)", 
                                   "kandidaatsfase", "master", "oude stijl (toegestaan t/m inschrijvingsjaar 1992/1993)", 
                                   "propedeuse (voornamelijk betreft dit ongedeelde opleidingen)", 
                                   "post-initiële master", "schakelprogramma",
                                   "beroepsfase artsen-, tandartsen-, dierenartsen- en apothekersopleidingen"))


# Inschrijvingsvorm

inschrijvingsvorm_labels = data.frame(code = c("S","A","E","T"), 
                               naam = c("student (initieel en vervolgonderwijs)",
           "auditor (initieel onderwijs)(toegestaan t/m inschrijvingsjaar 1995-1996)" ,
           "extraneus (initieel onderwijs)",
           "toegelaten student (coöp- of duaal onderwijs)"))



crohoonderdeelactopl_labels = data.frame(code = c(1:9, 0, "X"),
                                         naam = c("onderwijs",
                                                  "landbouw en natuurlijke omgeving",
                                                  "natuur",
                                                  "techniek",
                                                  "gezondheidszorg",
                                                  "economie",
                                                  "recht",
                                                  "gedrag en maatschappij",
                                                  "taal en cultuur",
                                                  "sectoroverstijgend",
                                                  "opleiding telt niet mee"))
  

typeHObinnensoortHO_labels = data.frame(code = c("ad",
                                          "ba",
                                          "ma",
                                          "pi",
                                          "xx"),
                                 naam = c("associate degree",
                                          "bachelor-opleiding",
                                          "master-opleiding",
                                          "postinitiele master",
                                          "opleiding telt niet mee"))


indicatieactiefoppeildatum_labels = data.frame(code = c(1,
                                                 2,
                                                 3,
                                                 4),
                                        naam = c("actief op peildatum 1 oktober (inschrijving)",
                                                 "inschrijving beëindigd vóór peildatum (uitschrijving)",
                                                 "inschrijving nog niet begonnen op peildatum en combinatie opleiding-instelling, komt NIET voor bij een andere inschrijving van de betreffende student (echte na-inschrijving)",
                                                 "inschrijving nog niet begonnen op peildatum en combinatie opleiding-instelling, komt WEL voor bij een andere inschrijving van de betreffende student (onechte na-inschrijving)"))


soortinschrijvingactinstelling_labels = data.frame(code = c(1,
                                                                                   2,
                                                                                   3,
                                                                                   4,
                                                                                   5,
                                                                                   6,
                                                                                   7,
                                                                                   8,
                                                                                   9,
                                                                                   "A",
                                                                                   "B",
                                                                                   "C",
                                                                                   "D",
                                                                                   "E"),
                                        naam = c("hoofdinschrijving binnen het domein actuele instelling",
                                                   "neveninschrijving binnen het domein actuele instelling (combinatie opleiding-instelling komt NIET voor bij een andere inschrijving van de betreffende student) (echte neveninschrijving)",
                                                   "inschrijving is niet actief op peildatum 1 oktober",
                                                   "neveninschrijving binnen het domein actuele instelling (combinatie opleiding-instelling komt WEL voor bij een andere inschrijving van de betreffende student) (onechte neveninschrijving)", 
                                                   "opleiding telt niet mee",
                                                   "uitwisselingsstudent: hoofdinschrijving binnen het domein actuele instelling",
                                                   "uitwisselingsstudent: neveninschrijving binnen het domein actuele instelling (combinatie opleiding-instelling komt NIET voor bij een andere inschrijving van de betreffende student)",
                                                   "uitwisselingsstudent: inschrijving is niet actief op peildatum 1 oktober",
                                                   "uitwisselingsstudent: neveninschrijving binnen het domein actuele instelling (combinatie opleiding-instelling komt WEL voor bij een andere inschrijving van de betreffende student)", 
                                                   "aangewezen instelling: hoofdinschrijving binnen het domein actuele instelling",
                                                   "aangewezen instelling: neveninschrijving binnen het domein actuele instelling (combinatie opleiding-instelling komt NIET voor bij een andere inschrijving van de betreffende student)",
                                                   "aangewezen instelling: inschrijving is niet actief op peildatum 1 oktober",
                                                   "aangewezen instelling: neveninschrijving binnen het domein actuele instelling (combinatie opleiding-instelling komt WEL voor bij een andere inschrijving van de betreffende student)",
                                                   "aangewezen instelling: opleiding telt niet mee"))
  

soortdiplomainstelling_labels = data.frame(code = c("01",
                                                                   "02",
                                                                   "03",
                                                                   "04",
                                                                   "05", 
                                                                   "06",
                                                                   "07",
                                                                   "08", 
                                                                   "09", 
                                                                   "10",
                                                                   "13",
                                                                   "14",
                                                                   "15",
                                                                   "16",
                                                                   "99"),
                                naam = c("Hoofd-propedeuse-diploma binnen de actuele instelling",
                                           "Neven-propedeuse-diploma binnen de actuele instelling",
                                           "Hoofd-bachelor-diploma binnen de actuele instelling",
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
                                           "Nevendiploma postinitiele master binnen de actuele instelling",
                                           "Ongeldig diplomarecord of er bestaat in hetzelfde inschrijvingsjaar in dezelfde opleiding aan dezelfde instelling in dezelfde opleidingsfase actueel al eenzelfde diploma in een ander record"))




geslacht_labels = data.frame(code = c("V","M", "O"), naam = c("vrouw", "man", "onbekend"))

indicatieEER_labels = data.frame(code = c("J", "N"), naam = c("EER-student", "niet-EER-student"))


indicatieinternationalestudent_labels = data.frame(code = c("J", "N"), naam = c("internationale student", "geen internationale student"))


