from dataclasses import dataclass
from s3fs import S3FileSystem
import json
from typing import Any, Dict
import httpx
import polars as pl


@dataclass
class Mappings:
    regions: Dict[str, str]
    civilstand: Dict[str, str]


def extract_data() -> Dict[str, Any]:
    with open("api.json") as f:
        query = json.load(f)

    query["response"]["format"] = "json"
    data = httpx.post(
        "https://api.scb.se/OV0104/v1/doris/sv/ssd/BE/BE0101/BE0101A/BefolkningNy",
        json=query,
    )
    return data.json()


def get_column_mappings() -> Mappings:
    data = httpx.get(
        "https://api.scb.se/OV0104/v1/doris/sv/ssd/BE/BE0101/BE0101A/BefolkningNy"
    )
    objects = data.json()
    regions = None
    civilstand = None
    for i in objects["variables"]:
        if i["code"] == "Region":
            regions = {str(a): str(b) for a, b in zip(i["values"], i["valueTexts"])}
        elif i["code"] == "Civilstand":
            civilstand = {str(a): str(b) for a, b in zip(i["values"], i["valueTexts"])}
    return Mappings(regions, civilstand)


def create_dataframe(data: Dict[str, Any], mappings: Mappings) -> pl.DataFrame:
    df = pl.DataFrame(data["data"])
    fields = [c["code"] for c in data["columns"]]
    df = (
        df.with_columns(pl.col("key").list.to_struct(fields=fields))
        .unnest("key")
        .with_columns(pl.col("values").list.to_struct(fields=["Population"]))
        .unnest("values")
        .with_columns(
            [pl.col("Tid").str.to_date("%Y"), pl.col("Population").str.to_integer()]
        )
        .select(
            [
                pl.col("Kon").replace({"1": "Man", "2": "Woman"}),
                pl.col("Region").replace(mappings.regions),
                pl.col("Civilstand").replace(mappings.civilstand),
                pl.col("Tid"),
                pl.col("Population"),
            ]
        )
    )
    return df


def upload_df_to_s3(df: pl.DataFrame):
    fs = S3FileSystem(client_kwargs={"endpoint_url": "https://minio.lab.foffe.dev"})
    with fs.open("s3://user-foffe/befolkning.parquet", "wb") as f:
        df.write_parquet(f)


if __name__ == "__main__":
    data = extract_data()
    mappings = get_column_mappings()
    df = create_dataframe(data, mappings)
    upload_df_to_s3(df)
