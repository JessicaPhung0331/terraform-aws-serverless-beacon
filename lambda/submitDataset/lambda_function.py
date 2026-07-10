from route_create_dataset import route
from route_update_dataset import route as route_update_dataset


def lambda_handler(event, context):
    if event["resource"] == "/submit_dataset":
        return route(event)

    elif event["resource"] == "/submit_dataset/{type}":
        return route(event, event["pathParameters"].get("type", None))


if __name__ == "__main__":
    pass
