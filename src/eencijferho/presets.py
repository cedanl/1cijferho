PRESET_CONFIGS: dict[str, dict] = {
    "custom": {
        "label": "Eigen instellingen",
        "description": None,
        "available": True,
        "settings": None,
    },
    "nfwa": {
        "label": "No Fairness Without Awareness",
        "description": (
            "Configuratie voor het NFWA-project (CEDA/Npuls). "
            "Alleen EV-bestanden; geslacht en hoogste vooropleiding vóór het HO gedecodeerd en verrijkt."
        ),
        "available": True,
        "settings": {
            "opt_convert_ev": True,
            "opt_convert_vakhavw": True,
            "opt_decoded": True,
            "opt_enriched": True,
            "opt_parquet": True,
            "opt_snake_case": True,
            # Twee spellingen van dezelfde kolom: de schone Latin-1-naam (correct
            # gecodeerde DUO-data) en de variant met replacement-tekens die in sommige
            # Dec-bestanden zit. De decode-filter matcht op genormaliseerde naam, dus
            # beide opnemen dekt zowel nette als beschadigde bronbestanden af.
            "decode_columns": [
                "Hoogste vooropleiding vóór het HO",
                "Hoogste vooropleiding vï¿½ï¿½r het HO",
                "Hoogste vooropleiding",
                "Opleidingscode",
                "Vakcode",
            ],
            "enrich_variables": ["Geslacht", "Hoogste vooropleiding vóór het HO"],
        },
    },
    "svo": {
        "label": "Staat van het Onderwijs",
        "description": "Binnenkort beschikbaar.",
        "available": False,
        "settings": None,
    },
}

# De Evaluatietool Selectie gebruikt dezelfde doorstroom-/retentielogica als NFWA
# en heeft daarom exact dezelfde enriched 1CHO-uitvoer nodig (decoded Opleidingscode
# voor de opleidingsnaam, geslacht en vooropleiding, plus de inschrijvingsjaren waaruit
# `groep` wordt afgeleid). We verwijzen naar hetzelfde settings-object zodat beide presets
# niet uit elkaar lopen als de NFWA-configuratie wordt aangepast. Presets worden alleen
# uitgelezen, nooit ter plekke gemuteerd, dus het delen van dit object is veilig.
PRESET_CONFIGS["evaluatietool"] = {
    "label": "Evaluatietool Selectie",
    "description": (
        "Configuratie voor de Evaluatietool Selectie (CEDA/Npuls). "
        "Identiek aan de NFWA-preset: EV-bestanden met geslacht en vooropleiding gedecodeerd "
        "en verrijkt, zodat de doorstroomgroep (gestart / niet naar jaar 2 / doorgestroomd) "
        "bepaald kan worden."
    ),
    "available": True,
    "settings": PRESET_CONFIGS["nfwa"]["settings"],
}
