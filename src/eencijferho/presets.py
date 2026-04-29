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
            "Alleen EV-bestanden; geslacht, vooropleiding en aansluiting gedecodeerd en verrijkt."
        ),
        "available": True,
        "settings": {
            "opt_convert_ev": True,
            "opt_convert_vakhavw": False,
            "opt_decoded": True,
            "opt_enriched": True,
            "opt_parquet": True,
            "opt_encrypt": True,
            "opt_snake_case": True,
            "decode_columns": ["geslacht", "vooropleiding", "aansluiting"],
            "enrich_variables": ["geslacht", "vooropleiding", "aansluiting"],
        },
    },
    "svo": {
        "label": "Staat van het Onderwijs",
        "description": "Binnenkort beschikbaar.",
        "available": False,
        "settings": None,
    },
}
