import pandas as pd
from re import sub
from json import loads
from requests import Session

features = {
    "ts": "time",
    "pro": "demand_programmed",
    "pre": "demand_forecast",
    "dem": "demand_real",
    "car": "generation_coal",
    "die": "generation_diesel_engines",
    "gas": "generation_gas_turbine",
    "cc": "generation_combined_cycle",
    "cb": "balearic_peninsula_link",
    "fot": "generation_solar",
    "eol": "generation_wind",
    "emm": "mallorca-menorca_link",
    "emi": "mallorca-ibiza_link",
    "otrRen": "generation_other_renewables",
    "resid": "generation_waste",
    "genAux": "generation_auxiliary",
    "cogen": "generation_cogeneration",
    "eif": "ibiza-formentera_link",
    "nuc": "generation_nuclear",
    "hid": "generation_hydro",
    "inter": "international_exchanges",
    "icb": "balearic_peninsula_link",
    "solFot": "generation_solar_photovoltaic",
    "solTer": "generation_solar_thermal",
    "sol": "generation_solar",
    "termRenov": "generation_thermal_renewable",
    "cogenResto": "generation_cogeneration_waste",
    "gf": "generation_fuel_gas",
    "vap": "generation_vapor_turbine",
}

areacode = {
    "Baleares": "BALEARES5M",
    "Peninsula": "DEMANDAQH",
    "Canarias": "CANARIAS",
}

timezone = {
    "Baleares": "Europe/Madrid",
    "Peninsula": "Europe/Madrid",
    "Canarias": "Atlantic/Canary",
}

valores = {
    "demandaGeneracion": "valoresHorariosGeneracion",
    "prevProg": "valoresPrevistaProgramada",
}


class SpanishScraper:
    def __init__(self, session=None, verify=True):
        self.verify = verify
        if session and isinstance(session, Session):
            self.session = session
        else:
            self.session = Session()

    def _request(self, url):
        response = self.session.request("GET", url, verify=self.verify)
        return self.__getjson(response.text)

    def _makeurl(self, areacode, date, system, datatype):
        base = "https://demanda.ree.es/WSvisionaMoviles{2}Rest/resources/{3}{2}?curva={0}&fecha={1}"
        return base.format(areacode, date, system, datatype)

    def __getjson(self, text):
        removed_null = sub("null\(", "", text)
        response_cleaned = sub("\);", "", removed_null)
        json = loads(response_cleaned)
        return json

    def get(self, system, startdate, enddate):
        df = pd.DataFrame()
        # iterate over all days (one request for every endpoint per day)
        for date in pd.date_range(startdate, enddate, freq="D"):
            datestring = "{}-{:02d}-{:02d}".format(date.year, date.month, date.day)
            day = pd.DataFrame()
            # get data from both endpoints
            for datatype in ["prevProg", "demandaGeneracion"]:
                url = self._makeurl(areacode[system], datestring, system, datatype)
                jsondata = self._request(url)
                data = pd.DataFrame(jsondata[valores[datatype]])
                data.rename(columns=features, inplace=True)
                data.index = data["time"]
                data.drop("time", axis=1, inplace=True)
                day = pd.concat([day, data], axis=1)
            # filter out data from other days
            day = day.loc[day.index.str.startswith(str(datestring))]
            df = pd.concat([df, day], axis=0)

        # make datetime index (local timezone)
        df.index = df.index.str.replace("1A", "01")
        df.index = df.index.str.replace("1B", "01")
        df.index = df.index.str.replace("2A", "02")
        df.index = df.index.str.replace("2B", "02")

        df.index = pd.to_datetime(df.index)
        df.index = df.index.tz_localize(timezone[system], ambiguous="infer")

        return df


# select system and date range
system = 'Baleares'
startdate = '2022-11-11'
enddate = '2022-12-24'

# request data
data = SpanishScraper().get(system, startdate, enddate)
