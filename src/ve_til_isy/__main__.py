import datetime
import os
import sys
import csv

import pandas as pd
from dotenv import load_dotenv
from tomlkit import dumps, parse

from .hjelpere import (
    VEDB_hent,
    aggreger_bilag,
    beregn_brutto,
    flytt_levinfo,
    status,
    utlign_oreavrunding,
    get_public_ip,
)

from .sftp import upload_sftp

# Filsti til innstillingsfil, og hvor output skal lagres
FILSTI = os.getenv("FILSTI")

# Filsti til SQL-scriptene
FILSTI_SQL = os.path.dirname(__file__) + "/sql/"

# Last .env-fil, hvis den finnes.
load_dotenv()


# Hvilket år er i år?
i_aar = datetime.datetime.now().year
aar_liste = [i_aar - 1, i_aar, i_aar + 1]


def main():
    status("🏁 Starter behandling 🏁")
    status(f"ip-adresse: {get_public_ip()}")

    # Hent innstillinger
    with open(FILSTI + "/" + "setup.toml", "r") as file:
        innstillinger_toml = file.read()
    innstillinger = parse(innstillinger_toml)

    # Loop igjennom selskapene i innstillings-fila:
    for selskap in innstillinger["selskap"]:
        status(f"Jobber med selskap {selskap["nr"]} {selskap["navn"]}.")

        # Loop igjennom i fjor, i år, neste år.
        for aar in aar_liste:
            # sjekk om dette året skal behandles i dette selskapet:
            if aar >= selskap["start_aar"]:
                # Hent sist behandlede journal. Dersom ikke tidligere behandlet, start med 0.
                try:
                    siste_journal = selskap["journal_" + str(aar)]
                except:  # noqa: E722
                    siste_journal = 0

                status(f"Sjekker år {aar}, fra journalnr. {siste_journal}... ", False)

                # Hent en liste over bilag som skal overføres for de respektive selskap / år
                liste_over_bilag = hent_liste_over_bilag(selskap, aar, siste_journal)

                # Dersom det er bilagsnr i liste_over_bilag, hent transaksjonslinjene.
                if liste_over_bilag:
                    print("Starter behandling.")

                    # Hent transaksjoner fra Visma.
                    df = hent_transaksjoner(selskap["nr"], aar, liste_over_bilag)

                    # Dersom det er transaksjoner etter bearbeiding, så kan vi lagre:
                    if df.empty:
                        print("Ingen transaksjoner å behandle.")
                    else:
                        # Lagre fakturabilder.
                        lagre_fakturadokumenter(
                            selskap["nr"],
                            aar,
                            df[["bilag", "faktlop"]].drop_duplicates(),
                        )

                        # Lagrer sist behandlede journal
                        selskap["journal_" + str(aar)] = int(df["journalnr"].max())

                        # Bearbeid df til ISY-format
                        df = bearbeid_df(df)

                        # Eksport til CSV
                        # Opprett mappe dersom den ikke finnes.
                        FILSTI_CSV = (
                            FILSTI
                            + "/csv_rapporter_til_opplasting/"
                            + str(selskap["nr"])
                            + "/"
                        )
                        os.makedirs(FILSTI_CSV, exist_ok=True)
                        # Lagre filen.
                        filnavn = f"{FILSTI_CSV}Visma til ISY selskap {selskap["nr"]} {selskap["navn"]} {aar} j {siste_journal} til {selskap["journal_" + str(aar)]} {datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}.csv"
                        df.to_csv(
                            filnavn,
                            index=False,
                            decimal=",",
                            quoting=csv.QUOTE_NONNUMERIC,
                        )

                else:
                    print("Ingen nye bilag!")

    # Lagre endringer i innstillinger. (sist behandlet journal)
    innstillinger_toml = dumps(innstillinger)

    with open(FILSTI + "\\" + "setup.toml", "w") as file:
        file.write(innstillinger_toml)

    # Ferdig
    status("🥳 Ferdig! 😊👍")

    return


def hent_liste_over_bilag(selskap, aar, journal):
    """
    Lager en liste over bilag som skal behandles

    Argumenter:
        Liste over selskap og år

    Retur:
        Liste over bilag
    """

    # Initialiserer
    liste_over_bilag = []
    with open(FILSTI_SQL + "hent_bilagsliste.sql", "r") as file:
        query_template = file.read()

    # Vi tilpasser database-spørringen til selskapet og året
    query = query_template.format(
        u_selskap=selskap["nr"],
        u_aar=aar,
        u_journal=journal,
        avdeling_dim=str(selskap["avdeling_dim"]).zfill(2),
        avdeling_filter=selskap["avdeling_filter"],
        prosjekt_dim=str(selskap["prosjekt_dim"]).zfill(2),
    )

    """
    Kan optimaliseres med bedre query
    """
    liste = VEDB_hent(query)

    liste_over_bilag = ""

    for index, linje in enumerate(liste):
        if index != 0:
            liste_over_bilag += ","
        liste_over_bilag += str(linje["u_bilag"])

    return liste_over_bilag


def hent_transaksjoner(u_selskap, u_aar, bilagsliste):
    """
    Tar en liste over selskap, år og bilagsnummer,
    og returnerer transaksjonslinjene fra Visma.

    Argumenter:
        selskapsnummer, år, bilagsliste på format "bilagnr,bilagnr,bilagnr..."

    Retur:
        dataframe med transaksjonslinjer
    """

    # Initialisere
    bilagslinjer = []

    # Henter query-template:
    with open(FILSTI_SQL + "hent_transaksjoner.sql", "r") as file:
        query_template = file.read()

    # Hent bilagslinjer
    status("Henter transaksjoner fra databasen... ", False)
    query = query_template.format(
        u_selskap=u_selskap, u_aar=u_aar, bilagsliste=bilagsliste
    )
    resultat = VEDB_hent(query)
    print("ferdig!")

    status("Behandler transaksjoner... ", False)
    # omgjør til dataframe
    df = pd.DataFrame(resultat)

    # Gå igjennom ett og ett bilagsnummer
    for bilag in df.groupby("bilag"):
        # Omgjør bilagslinjene til en list med dict, og prosesser
        bilag = bilag[1].to_dict("records")

        bilag = beregn_brutto(bilag)

        # Dersom dette er et leverandørbilag (type 70-bilag)
        if bilag[0]["orgnr"]:
            bilag = flytt_levinfo(bilag)

        bilag = utlign_oreavrunding(bilag)

        bilag = aggreger_bilag(bilag)

        # Legg til i output-variabel
        for linje in bilag:
            bilagslinjer.append(linje)

    df = pd.DataFrame(bilagslinjer)

    print("ferdig!")

    return df


def lagre_fakturadokumenter(u_selskap, aar, bilag_og_faktlop):
    """
    Tar en df med bilag og fakturaløpenummer, og lagrer fakturadokumentene.

    Argumenter:
        df[["bilag","faktlop"]]

    Retur:
        ingen
    """

    status("Lagrer fakturabilder... ", False)

    # lag fakturamappe
    # os.makedirs(FILSTI + "/fakturabilder", exist_ok=True)
    # os.makedirs(FILSTI + "/fakturabilder/" + u_selskap, exist_ok=True)
    # os.makedirs(FILSTI + "/fakturabilder/" + u_selskap + "/" + aar + "/" + linje["bilag"], exist_ok=True)

    with open(FILSTI_SQL + "hent_fakturadokumenter.sql", "r") as file:
        query_template = file.read()

    for linje in bilag_og_faktlop.to_dict("records"):
        query = query_template.format(u_selskap=u_selskap, faktlop=linje["faktlop"])
        resultat = VEDB_hent(query)

        for index, row in enumerate(resultat):
            if str.strip(row["U_TYPE"]) == "PDF":
                # Opprett mappene hvor fakturabilde skal lagres.
                FILSTI_FAKTURA = (
                    FILSTI
                    + "/fakturabilder/"
                    + str(u_selskap)
                    + "/"
                    + str(aar)
                    + "/"
                    + str(linje["bilag"])
                    + "/"
                )
                os.makedirs(FILSTI_FAKTURA, exist_ok=True)

                # Skriv databasefelt til fil:
                with open(
                    FILSTI_FAKTURA
                    + str(u_selskap)
                    + "-"
                    + str(aar)
                    + "-"
                    + str(linje["bilag"])
                    + "-"
                    + str(index + 1)
                    + ".pdf",
                    "wb",
                ) as fil:
                    fil.write(row["U_DOKUMENT"])
    print("ferdig")

    return


def get_contractid(row):
    if row["dispnr"].startswith("ISY-PNS"):
        return (
            row["dispnr"].split("ISY-PNS")[-1]
            + str("-")
            + row["etr"].split("ISY-ETR")[-1]
        )
    else:
        return ""


def bearbeid_df(df):
    """
    Tar en dataframe og bearbeider denne til ISY sitt ønskede format.

    Argumenter:
        df

    Retur:
        df
    """

    # "Rensker" opp
    df["periode"] = df["aar"].astype(str) + df["periode"].astype(str).str.zfill(2)
    df["faktdato"] = pd.to_datetime(df["faktdato"], format="%Y%m%d.0")
    df["forfdato"] = pd.to_datetime(df["forfdato"], format="%Y%m%d.0")
    df["resknr"] = pd.to_numeric(df["resknr"])
    df["netto"] = pd.to_numeric(df["netto"])
    df["mva"] = pd.to_numeric(df["mva"])
    df["brutto"] = pd.to_numeric(df["brutto"])
    df["dispnr"] = df["dispnr"].astype(str)
    df["etr"] = df["etr"].astype(str)
    df["ContractID"] = df.apply(get_contractid, axis=1)

    df = df.rename(
        columns={
            "bilag": "VoucherID",
            "faktnr": "ExternalInvoiceRef",
            "faktdato": "VoucherDate",
            "forfdato": "DueDate",
            "periode": "Period",
            "resknr": "Aux04",
            "navn": "SupplierName",
            "orgnr": "SupplierID",
            "netto": "NetAmount",
            "mva": "TaxAmount",
            "brutto": "BaseAmount",
            "ansvar": "Ansvar",
            "prosjekt": "Eiendel",
            "dispnr": "kb09",
            "etr": "kb10",
        }
    )

    # Fjern "None" i blanke celler.
    df[["kb09", "kb10"]] = df[["kb09", "kb10"]].fillna("")

    df = df.drop(columns=["journalnr", "selskap", "aar"])

    # Lag "tomme" kolonner:
    df["LineNo"] = pd.Series(dtype="int")
    df["InvoiceDate1"] = pd.Series(dtype="int")
    df["TaxCode"] = pd.Series(dtype="int")
    df["Mottaker"] = pd.Series(dtype="int")
    df["Konterer"] = pd.Series(dtype="int")
    df["Attestant"] = pd.Series(dtype="int")
    df["Anviser"] = pd.Series(dtype="int")
    df["Konto"] = pd.Series(dtype="int")
    df["Tjeneste"] = pd.Series(dtype="int")
    df["Aktivitet"] = pd.Series(dtype="int")
    df["Byggtabell"] = pd.Series(dtype="int")
    df["ChangeID"] = pd.Series(dtype="int")
    # df["kb09"] = pd.Series(dtype="int")
    # df["kb10"] = pd.Series(dtype="int")
    df["Description"] = pd.Series(dtype="int")

    # Juster rekkefølge
    df = df[
        [
            "VoucherID",
            "LineNo",
            "ExternalInvoiceRef",
            "VoucherDate",
            "DueDate",
            "InvoiceDate1",
            "Period",
            "Aux04",
            "SupplierName",
            "SupplierID",
            "NetAmount",
            "TaxAmount",
            "BaseAmount",
            "TaxCode",
            "Mottaker",
            "Konterer",
            "Attestant",
            "Anviser",
            "Konto",
            "Ansvar",
            "Tjeneste",
            "Eiendel",
            "Aktivitet",
            "Byggtabell",
            "ContractID",
            "ChangeID",
            "kb09",
            "kb10",
            "Description",
        ]
    ]

    return df


def sjekk_env():
    liste_over_env = [
        "VISMA_SERVER",
        "VISMA_BRUKER",
        "VISMA_PASSORD",
        "VISMA_DB_PREFIX",
        "FILSTI",
        "FTP_SERVER",
        "FTP_BRUKER",
        "FTP_PASSORD",
    ]

    mangler = ""

    for variabel in liste_over_env:
        if not os.getenv(variabel):
            mangler = mangler + " " + variabel

    if mangler:
        sys.exit(f"😬🫨🤯 Mangler følgende ENV-variabler:{mangler} . Please fix 🥳☀️😎")


# Sjekk at vi har inne alle env-variablene som trengs
sjekk_env()

# Dersom .py-fila kjøres direkte, start main()
if __name__ == "__main__":
    main()
