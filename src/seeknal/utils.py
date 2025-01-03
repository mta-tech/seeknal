import hashlib
import json
import re
from datetime import datetime
from functools import reduce
from typing import List, Optional, Union

import pandas as pd
import pendulum
import typer
from IPython import get_ipython
from pydantic import BaseModel


class Later(BaseModel):
    """
    Declare when a job submitted

    Attributes:
        when (Union(str, datetime)): specify the time or date. This accepts string or datetime class.
            If string given, it will expect these options:

              1). today - set today date and combine the time variable

              2). ([0-9]+)d - set number of days after current time. For example: "1d"

              3). ([0-9]+)h - set number of hours after current time. For example: "1h"

              4). ([0-9]+)m - set number of minutes after current time. For example: "1m"

        time (datetime): set specific time. Default to current time
        timezone (str, optional): override timezone
    """

    when: Union[str, datetime] = "today"
    time: datetime = datetime.now().time()  # type: ignore
    timezone: Optional[str] = pendulum.now().timezone_name

    def get_date_hour(self):
        if isinstance(self.when, str):
            _date = pendulum.now(tz=self.timezone)
            if self.when == "today":
                return pendulum.instance(
                    datetime.combine(_date, self.time), tz=self.timezone
                )
            else:
                match = re.match(r"([0-9]+)([a-z]+)", self.when, re.I)
                if match:
                    selector = match.group(2)
                    inc = int(match.group(1))
                    if selector == "d":
                        _added_date = _date.add(days=inc)
                        return pendulum.instance(
                            datetime.combine(_added_date, self.time), tz=self.timezone
                        )
                    elif selector == "h":
                        _now = datetime.combine(_date, self.time)
                        return pendulum.instance(_now, tz=self.timezone).add(hours=inc)
                    elif selector == "m":
                        _now = datetime.combine(_date, self.time)
                        return pendulum.instance(_now, tz=self.timezone).add(
                            minutes=inc
                        )
                    else:
                        raise AttributeError("Unknown time identifier")
                else:
                    raise AttributeError("Accepted format is %d[d, h, m]")
        elif isinstance(self.when, datetime):
            return pendulum.instance(self.when, tz=self.timezone)

    def to_utc(self, as_string: bool = True):
        try:
            date = self.get_date_hour().in_tz("UTC")
            if as_string:
                return date.to_datetime_string()
            else:
                return date
        except:
            raise


def is_notebook() -> bool:
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def to_snake(string):
    """
    Converts camel case string to snake case. E.g. projectId -> project_id
    Usually used to convert the api response into python dictionaries
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


def pretty_returns(
        res_json,
        date_to_pull: Optional[List[str]] = None,
        configs: Optional[str] = None,
        truncate: bool = True,
        return_as_df: bool = False,
):
    """
    Print result from SJS.

    Parameters
    ----------
    res_json : dict
        Result to be printed.
    date_to_pull : (List of str, optional)
        if this is set, it will select specified date from the result for printing.
    configs : (str, optional)
        if this is set, it will select specified config name from the result for printing
    truncate : (bool, optional)
        Truncate the printed result
    return_as_df : (bool, optional)
        if set true, return a Pandas Dataframe.

    Returns
    -------
    Either printed result or Pandas Dataframe

    """
    if date_to_pull is None:
        date_to_pull = [""]
    for i in date_to_pull:
        try:
            if i != "":
                if configs is not None:
                    typer.echo(
                        typer.style(
                            "\nResult from date: {}".format(i),
                            fg=typer.colors.GREEN,
                            bold=False,
                        )
                    )
            res = res_json[i]
            for idx, k in enumerate(res):
                if configs is not None and not return_as_df:
                    typer.echo(
                        typer.style(
                            "\nResult from config: {}".format(configs[idx].name),
                            fg=typer.colors.GREEN,
                            bold=False,
                        )
                    )
                elif not return_as_df:
                    typer.echo(
                        typer.style(
                            "\nResult from config: {}".format(idx),
                            fg=typer.colors.GREEN,
                            bold=False,
                        )
                    )
                df_schema = get_df_schema(k, return_as_df=return_as_df)
                if not return_as_df:
                    if not truncate:
                        pd.set_option("display.max_rows", None)
                    else:
                        pd.set_option("display.max_rows", 20)
                    if "content" in df_schema:
                        typer.echo(
                            typer.style(
                                "DataFrame: ", fg=typer.colors.MAGENTA, bold=False
                            )
                        )
                        typer.echo(pd.DataFrame(df_schema["content"]))
                    if "schema" in df_schema:
                        typer.echo(
                            typer.style("Schema: ", fg=typer.colors.MAGENTA, bold=False)
                        )
                        typer.echo(df_schema["schema"])
                else:
                    if "content" in df_schema:
                        return {
                            "content": pd.DataFrame(df_schema["content"]),
                            "schema": df_schema["schema"],
                        }
                    else:
                        return {"content": None, "schema": None}
        except:
            typer.echo(
                typer.style(
                    "Cannot find result for date: {}".format(i),
                    fg=typer.colors.RED,
                    bold=False,
                )
            )


def get_df_schema(job_result, return_as_df=False):
    """
    Parse result to get content and schema

    Parameters
    ----------
    job_result : dict
        Job result to be parsed for content and schema.
    return_as_df: (bool, optional)
        Job result to be returned as a dataframe

    Returns
    -------
    Dict with keys: "content" and "schema"

    """
    df_schema = {}
    if "content" in job_result:
        content = map(lambda x: json.loads(x), job_result["content"])
        df_schema["content"] = list(content)
    if "schema" in job_result:
        if not return_as_df:
            schema = list(
                map(lambda x: json.loads(x.replace("'", '"')), job_result["schema"])
            )
            schema = list(
                map(
                    lambda x: "|- {0}: {1}".format(
                        list(x.items())[0][0], list(x.items())[0][1]
                    ),
                    schema,
                )
            )
            schema = reduce(lambda x, y: "{0}\n{1}".format(x, y), schema)
            df_schema["schema"] = schema
        else:
            df_schema["schema"] = job_result["schema"]
    return df_schema


def check_is_dict_same(content_one: dict, content_two: dict):
    idempotency_key = hashlib.sha256(
        json.dumps(content_one, sort_keys=True).encode()
    ).hexdigest()

    new_idempotency_key = hashlib.sha256(
        json.dumps(content_two, sort_keys=True).encode()
    ).hexdigest()

    if idempotency_key == new_idempotency_key:
        return True
    else:
        return False


def _check_is_dict_same(content_one: dict, content_two: dict):
    return check_is_dict_same(content_one, content_two)
