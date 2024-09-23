import configparser
import datetime
import os
from decimal import Decimal
import requests

import pandas as pd
import pymssql

# Config-fil
config = configparser.ConfigParser()
config.read("config.ini")


# Hvilket år er i år?
i_aar = datetime.datetime.now().year

# Path til lagrede data
db_path = "db.pickle"


def VEDB_hent(query, kun_en_linje=False):
    """
    Sender Query til Visma sin database.

    Argumenter:
        query

    Retur:
        list med linjer som dict
    """

    server = os.getenv("VISMA_SERVER")
    bruker = os.getenv("VISMA_BRUKER")
    passord = os.getenv("VISMA_PASSORD")

    with pymssql.connect(server, bruker, passord) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(query)
            if kun_en_linje:
                output = cursor.fetchone()
            else:
                output = cursor.fetchall()

    # Returner dict.
    return output


def beregn_brutto(bilag):
    """
    Beregner MVA-beløp og bruttobeløp per utgiftslinje.

    Argumenter:
        bilag som liste, med linjer som dict.

    Retur:
        bilag som liste, med linjer som dict.
    """
    retur = []
    husk_bilagslinje = 0

    for linje in bilag:
        # Er vi på en ny "hovedlinje"
        if husk_bilagslinje != linje["bilaglin"]:
            if linje["bilaglin"] > 1:
                # Dersom bilaget har mer enn 1 "hovedlinje, legg forrige linje til retur."
                husk_linje["mva"] = husk_linje["brutto"] - husk_linje["netto"]  # noqa: F821
                retur.append(husk_linje)  # noqa: F821
            husk_linje = linje
            husk_linje["brutto"] = husk_linje["netto"]
        else:
            # Hvis ikke er vi på en momslinje, som skal legges til bruttobeløpet.
            husk_linje["brutto"] += linje["netto"]

        husk_bilagslinje = linje["bilaglin"]

    # Legg til siste linje og returner.
    husk_linje["mva"] = husk_linje["brutto"] - husk_linje["netto"]
    retur.append(husk_linje)

    return retur


def flytt_levinfo(bilag):
    """
    Tar leverandørinfo på faktura o.l., og flytter til alle linjene.

    Argumenter:
        list med linjer som dict

    Retur:
        list med linjer som dict
    """
    retur = []

    # Hent ut fakturainfo fra første linje
    faktura_info = {
        key: str(bilag[0][key]).strip()
        for key in [
            "resknr",
            "faktlop",
            "orgnr",
            "navn",
            "faktnr",
            "faktdato",
            "forfdato",
        ]
    }

    # Sett på fakturainfo fra linje nr. 2
    for linje in bilag[1:]:
        linje = {key: faktura_info.get(key, linje[key]) for key in linje}
        retur.append(linje)

    return retur


def utlign_oreavrunding(bilag):
    """
    Sjekker om det finnes øreavrundingkonto med noen få øre på bilaget,
    legger øreavrundingen til første utgiftslinje.

    Argumenter:
        List med linjer som dict

    Retur:
        List med linjer som dict
    """
    avrunding_belop = Decimal(0.00)

    for linje in tuple(bilag):
        if (linje["tekst"].rstrip() == "Øreavrunding") and (linje["netto"] < 0.10):
            avrunding_belop += linje["netto"]
            bilag.remove(linje)

    bilag[0]["brutto"] += avrunding_belop

    return bilag


def aggreger_bilag(bilagslinjer):
    """
    Agreggerer bilag ned til nivået vi skal ha inn i ISY.

    Argumenter:
        List med linjer som dict

    Retur:
        List med linjer som dict
    """
    df = pd.DataFrame(bilagslinjer)

    df = (
        df.groupby(
            [
                "journalnr",
                "selskap",
                "aar",
                "bilag",
                "faktlop",
                "faktnr",
                "faktdato",
                "forfdato",
                "periode",
                "resknr",
                "navn",
                "orgnr",
                "ansvar",
                "prosjekt",
                "dispnr",
                "etr",
            ]
        )
        .agg({"netto": "sum", "mva": "sum", "brutto": "sum"})
        .reset_index()
    )

    # Gjør tomme felt til NA
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # Fjern linjer uten prosjekt
    df.dropna(subset=["prosjekt"], inplace=True)

    bilagslinjer = df.to_dict("records")

    return bilagslinjer


def status(melding, nylinje=True):
    """
    Legger på timestamp og printer en melding i terminalen.

    Argumenter:
        melding, nylinje (default: True)

    Retur:
        ingen
    """

    if nylinje:
        print(f"{datetime.datetime.now().strftime("%H:%M:%S")}: {melding}")
    else:
        print(
            f"{datetime.datetime.now().strftime("%H:%M:%S")}: {melding}",
            end="",
        )
    return


def get_public_ip():
    response = requests.get("https://api.ipify.org").text
    return response
