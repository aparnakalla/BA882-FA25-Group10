def transform_routes(df):
    """
    Basic transformation: rename columns, keep relevant fields, add timestamp.
    """
    keep_cols = ["id", "attributes.long_name", "attributes.short_name",
                 "attributes.type", "attributes.color", "attributes.text_color"]
    df = df[keep_cols]
    df.rename(columns={
        "id": "route_id",
        "attributes.long_name": "route_name",
        "attributes.short_name": "short_name",
        "attributes.type": "type",
        "attributes.color": "color",
        "attributes.text_color": "text_color"
    }, inplace=True)
    return df
