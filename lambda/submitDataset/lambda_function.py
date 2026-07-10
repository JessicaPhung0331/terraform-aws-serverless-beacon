from route_create_dataset import route_any, route_single 
from route_update_dataset import route as route_update_dataset


def lambda_handler(event, context):
    if event["resource"] == "/submit_dataset":
        return route_any(event)

    elif event["resource"] == "/submit_dataset/{type}":
        return route_single(event, event["pathParameters"].get("type", None))


if __name__ == "__main__":
    pass
