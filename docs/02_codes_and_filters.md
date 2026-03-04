---
icon: material/database-outline
---

# Codes and filters
## Nadag TOML-file
The file `nadag.toml` controls the handling of the original NADAG codes, renaming and filtering. If there are codes that are not interesting to fetch as well as if there are codes that are ignored but should be part of the results can easily be added in the toml-file.
A custom/different toml-file can be used by giving the path to the file in the `NADAG_PYTHON_NADAG_TOML_PATH` environment variable (or either in the `.env`-file or `nadag_python/config.py`-file).

## Code mapping of methods and samples

Methods and samples are filtered and renamed following the convention in the table below. Methods that do not have a code in the "after processing" field are not considered in the results.


| Code | Name | After Processing |
|------|------|------------------|
| 1  | Dreietrykksondering |--> rp | 
| 2  | Trykksondering (CPT, CPTU) |--> cpt | 
| 14 | Poretrykksmåling | -------- |
| 15 | Totalsondering Norge |--> tot | 
| 16 | Totalsondering Sverige |--> tot |
| 17 | Dreiesondering manuell |--> tot |
| 18 | Dreiesondering maskinell |--> tot |
| 19 | Vingeboring | -------- |
| 20 | Enkel sondering | -------- |
| 21 | Ramsondering | -------- |
| 22 | Ramsondering A | -------- |
| 23 | Ramsondering B | -------- |
| 24 | Bergkontrollboring | -------- |
| 25 | Dilatometertest | -------- |
| 26 | Dynamisk sondering uspesifisert | -------- |
| 27 | Gassmåling | -------- |
| 28 | Grunnvannsmåling | -------- |
| 29 | Hejarsondering A | -------- |
| 30 | Hejarsondering B | -------- |
| 31 | HK sondering | -------- |
| 32 | Hydraulisk test | -------- |
| 33 | Jord-berg sondering 1 | -------- |
| 34 | Jord-berg sondering 2 | -------- |
| 35 | Jord-berg sondering 3 | -------- |
| 36 | Kombinasjonssondering uspesifisert |--> tot | 
| 37 | Kombisondering |--> tot |
| 38 | Platebelastning | -------- |
| 39 | Slagsondering | -------- |
| 40 | SPT | -------- |
| 41 | Statisk sondering uspesifisert |--> rp |
| 42 | Stikksondering | -------- |
| 43 | Vektsondering manuell | -------- |
| 44 | Vektsondering maskinell | -------- |
| 45 | Ikke angitt | -------- |
| 46 | Dreiesondering uspesifisert |--> rp |
| 49 | Miljøundersøkelse | -------- |
| 50 | Elektrisk sondering | -------- |
| 51 | Kjerneboring | -------- |


Similarly for samples:

| Code | Name | After Processing |
|------|------|------------------|
| 3 | Blokkprøve |--------|
| 4 | Graveprøve |--------|
| 5 | Kanneprøve |--------|
| 6 | Kjerneprøve | --> sa |
| 7 | Naverprøve |--------|
| 8 | Ramprøve |--------|
| 9 | Sedimentprøve |--------|
| 10 |  Skovlprøve |--------|
| 11 |  Stempelprøve |--------|
| 12 |  Vannprøve |--------|
| 13 |  Gassprøve |--------|
| 47 |  Prøve uspesifisert | --> sa |
| 48 |  Prøveserie uspesifisert | --> sa |


## Boolean flags for total sounding

We use boolean flags (True/False) to mark the segments where a total sounding has used `hammering`, `increased rotation` or `flushing`. The codes in the datasett that triggers this flags area presented below.

| description | codes |
|-------------|-------|
| Hammering starts | 11, 15, 63 |
| Hammering ends | 16, 64       |
| Increased rotation starts | 51 |
| Increased rotation ends | 52 |
| Flushing starts | 14, 63 |
| Flushing ends | 62, 64 |


An overview of the codes is shown below.

| code | description |
|-------------|-------|
| 1 | ForrigeKodeFeil |
| 2 | Startnivå |
| 3 | NyMetode |
| 4 | MerInformasjon |
| 5 | TidligereStopp |
| 6 | Opphold |
| 7 | UtenVridning |
| 8 | SynkVekt0 |
| 9 | SynkVekt0.64 |
| 10 | IngenRegistrering |
| 11 | Slag |
| 12 | Vridning |
| 13 | Skarv |
| 14 | Spyling |
| 15 | StartSlag |
| 16 | SluttSlag |
| 17 | Fylling |
| 18 | Tørrskorpe |
| 19 | Friksjonsjord |
| 20 | UorganiskKohesjonsjord |
| 21 | OrganiskJord |
| 22 | UbestemtJordart |
| 23 | Leire |
| 24 | JordMedSand |
| 25 | JordMedGrus |
| 26 | JordMedStein |
| 27 | GjennomborretBlokkEllerStein |
| 28 | IkkeMerkbareSprekker |
| 29 | OppsprukketFjell |
| 30 | SværtOppsprukketFjell |
| 31 | FjellMedSlag |
| 32 | IngenVurdering |
| 36 | Avsluttet |
| 37 | FunksjonskontrollOk |
| 38 | FunksjonskontrollIkkeOk |
| 39 | Hindring |
| 41 | Frosset |
| 42 | Skadet |
| 43 | BøydSonderingsstang |
| 44 | SonderingstangAv |
| 46 | AntattGrunnvann |
| 47 | Tidsangivelse |
| 48 | SpissDyp |
| 49 | Frostisolering |
| 50 | KullAske |
| 51 | StartVridning |
| 52 | SluttVridning |
| 53 | UrenMakadam |
| 54 | Fjellnivå |
| 55 | Terrengoverflate |
| 56 | Silt |
| 57 | Torv |
| 58 | MoreneJord |
| 59 | LeirMorene |
| 60 | SandmedGrus |
| 61 | BlokkSlutt |
| 62 | SpylingSlutt |
| 63 | SlagSpyl |
| 64 | SlagSpylSlutt |
| 65 | Pumping |
| 66 | PumpingSlutt |
