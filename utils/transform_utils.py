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


def transform_alerts(df):
    """
    Basic transformation: keep relevant MBTA alerts fields, rename columns, and standardize structure.
    """
    keep_cols = [
        "id",
        "attributes.cause",
        "attributes.effect",
        "attributes.header",
        "attributes.informed_entity",
        "attributes.severity",
        # "attributes.service_effect_text",
        "attributes.timeframe",
        "attributes.updated_at",
        "attributes.created_at"
    ]

    # Keep only relevant columns
    df = df[keep_cols]

    # Rename columns for clarity
    df.rename(columns={
        "id": "alert_id",
        "attributes.cause": "cause",
        "attributes.effect": "effect",
        "attributes.header": "header",
        "attributes.informed_entity": "informed_entity",
        "attributes.severity": "severity",
        "attributes.service_effect_text": "service_effect",
        "attributes.timeframe": "timeframe",
        "attributes.updated_at": "updated_at",
        "attributes.created_at": "created_at"
    }, inplace=True)

    return df

def transform_route_patterns(df):
    """
    Basic transformation: keep relevant MBTA route pattern fields, rename columns, and standardize structure.
    """
    keep_cols = [
        "id",
        # "attributes.route_id",
        "attributes.name",
        "attributes.direction_id",
        "attributes.sort_order",
        # "attributes.representative_trip_id",
        "attributes.typicality"
    ]

    df = df[keep_cols]

    df.rename(columns={
        "id": "route_pattern_id",
        # "attributes.route_id": "route_id",
        "attributes.name": "pattern_name",
        "attributes.direction_id": "direction_id",
        "attributes.sort_order": "sort_order",
        # "attributes.representative_trip_id": "trip_id",
        "attributes.typicality": "typicality"
    }, inplace=True)

    return df


def transform_stops(df):
    """
    Basic transformation: keep relevant MBTA stop fields, rename columns, and standardize structure.
    """
    keep_cols = [
        "id",
        "attributes.name",
        "attributes.platform_code",
        "attributes.platform_name",
        "attributes.latitude",
        "attributes.longitude",
        "attributes.wheelchair_boarding",
        "attributes.vehicle_type"
    ]

    df = df[keep_cols]

    df.rename(columns={
        "id": "stop_id",
        "attributes.name": "stop_name",
        "attributes.platform_code": "platform_code",
        "attributes.platform_name": "platform_name",
        "attributes.latitude": "latitude",
        "attributes.longitude": "longitude",
        "attributes.wheelchair_boarding": "wheelchair_accessible",
        "attributes.vehicle_type": "vehicle_type"
    }, inplace=True)

    return df


def transform_vehicles(df):
    """
    Basic transformation: keep relevant MBTA vehicle fields, rename columns, and standardize structure.
    """
    keep_cols = [
        "id",
        "attributes.label",
        "attributes.direction_id",
        "attributes.current_status",
        "attributes.current_stop_sequence",
        "attributes.occupancy_status",
        "attributes.latitude",
        "attributes.longitude",
        "attributes.updated_at"
    ]

    df = df[keep_cols]

    df.rename(columns={
        "id": "vehicle_id",
        "attributes.label": "vehicle_label",
        "attributes.direction_id": "direction_id",
        "attributes.current_status": "status",
        "attributes.current_stop_sequence": "stop_sequence",
        "attributes.occupancy_status": "occupancy_status",
        "attributes.latitude": "latitude",
        "attributes.longitude": "longitude",
        "attributes.updated_at": "updated_at"
    }, inplace=True)

    return df

