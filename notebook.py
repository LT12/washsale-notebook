import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _():
    import pytest
    from datetime import datetime, timedelta
    import polars as pl

    return pl, timedelta


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    White-space separated table of transactions.
    """)
    return


@app.cell
def _():
    wsv_transactions = """
    Symbol	Type	Quantity	Date_Acquired	Date_Sold	Cost_Basis	Proceeds
    EXAMPLE	RSU     42          01/01/2025	    01/02/2025  $100        $200
    """.strip()
    return (wsv_transactions,)


@app.cell
def _(pl, wsv_transactions):
    def import_transactions(wsv_transactions: str):
        lines = wsv_transactions.splitlines()
        header = lines[0].strip().split()
        rows = [row.strip().split() for row in lines[1:]]

        transactions = pl.DataFrame(rows, schema=header, orient="row")
        return (
            transactions.with_columns(
                pl.col("Quantity").cast(pl.Float64),
                pl.col("Date_Sold").str.to_date("%m/%d/%Y"),
                pl.col("Date_Acquired").str.to_date("%m/%d/%Y"),
                pl.col("Cost_Basis").str.replace_all(r"[$,]", "").cast(pl.Float64),
                pl.col("Proceeds").str.replace_all(r"[$,]", "").cast(pl.Float64),
            )
            .with_columns((pl.col("Proceeds") - pl.col("Cost_Basis")).alias("Net"))
            .with_row_index(name="Index")
            .sort("Date_Sold")
        )


    transactions = import_transactions(wsv_transactions)
    transactions
    return import_transactions, transactions


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Total Gain/Loss (If no wash sale carrys**)
    """)
    return


@app.cell
def _(transactions):
    transactions["Net"].sum()
    return


@app.cell
def _(timedelta):
    def calculate_single_wash_sale(record, records, used_as_replacement):
        loss_to_defer = abs(record["Proceeds"] - record["Adjusted_Cost_Basis"])
        sale_date = record["Date_Sold"]

        holding_period_days = (
            sale_date - record["Adjusted_Date_Acquired"]
        ).days

        wash_sale_window_days = 30
    
        for replacement in records:
            if (
                record["Index"] == replacement["Index"]
                or replacement["Index"] in used_as_replacement
                or record["Symbol"] != replacement["Symbol"]
            ):
                continue

            acquisition_delta = abs((record["Date_Sold"] - replacement["Date_Acquired"]).days)
            if acquisition_delta <= wash_sale_window_days:
                print(record["Date_Sold"], replacement["Date_Acquired"], acquisition_delta, holding_period_days)
            
                record["Wash_Sale_Loss_Deferred"] = loss_to_defer

                replacement["Adjusted_Cost_Basis"] += loss_to_defer

                replacement["Adjusted_Date_Acquired"] -= timedelta(
                    days=holding_period_days
                )

                used_as_replacement.add(replacement["Index"])
                return

    return (calculate_single_wash_sale,)


@app.cell
def _(calculate_single_wash_sale, pl, transactions):
    def calculate_wash_sales(transactions):
        records = transactions.to_dicts()
        used_as_replacement: set[int] = set()

        for r in records:
            r.update({
                "Adjusted_Cost_Basis": r["Cost_Basis"],
                "Adjusted_Date_Acquired": r["Date_Acquired"],
                "Wash_Sale_Loss_Deferred": 0.0,
            })

        for record in records:
            is_loss = (record["Proceeds"] - record["Adjusted_Cost_Basis"]) < -1E-2

            if is_loss: 
                calculate_single_wash_sale(record, records, used_as_replacement)


        return (
            pl.from_dicts(records)
            .with_columns(
                [
                    (
                        pl.col("Proceeds")
                        - pl.col("Adjusted_Cost_Basis")
                        + pl.col("Wash_Sale_Loss_Deferred")
                    ).alias("Realized_Gain_Loss"),
                    pl.when(
                        (
                            pl.col("Date_Sold") - pl.col("Adjusted_Date_Acquired")
                        ).dt.total_days()
                        > 365
                    )
                    .then(pl.lit("Long"))
                    .otherwise(pl.lit("Short"))
                    .alias("Term"),
                ]
            )
            .sort(by="Index")
        )


    adjusted_transactions = calculate_wash_sales(transactions)
    adjusted_transactions.select(
        pl.col("Index"),
        pl.col("Type"),
        pl.col("Quantity"),
        pl.col("Date_Acquired"),
        pl.col("Adjusted_Date_Acquired"),
        pl.col("Date_Sold"),
        pl.col("Proceeds"),
        pl.col("Adjusted_Cost_Basis"),
        pl.col("Wash_Sale_Loss_Deferred"),
            pl.col("Realized_Gain_Loss"),
        pl.col("Term"),
    )
    return (calculate_wash_sales,)


@app.cell
def _(calculate_wash_sales, import_transactions, wsv_transactions):
    def test_net_washsale_is_zero():
        transactions = import_transactions(wsv_transactions)
        adjusted_transactions = calculate_wash_sales(transactions)
        delta_net_wash_sale = (
            transactions["Net"].sum()
            - adjusted_transactions["Realized_Gain_Loss"].sum()
        )
        assert abs(delta_net_wash_sale) < 1e-2

    return


if __name__ == "__main__":
    app.run()
