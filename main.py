import asyncio
from enum import StrEnum, auto
from functools import wraps
from typing import Type

import aiohttp
from aiohttp.client_exceptions import ClientError
from pydantic import AnyUrl, BaseModel, PositiveInt, TypeAdapter, ValidationError


class Item(BaseModel):
    id: PositiveInt
    body: str


class Resources(StrEnum):
    posts = auto()
    comments = auto()
    albums = auto()
    photos = auto()
    todos = auto()
    users = auto()


class URLs(StrEnum):
    JsonPlaceHolder = "https://jsonplaceholder.typicode.com"


class Response(BaseModel):
    item: list[Item | None] | None = None
    excpt: Type[Exception] | None = None
    attempts: PositiveInt = 1


def retry(max_attempts: PositiveInt):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempts = 1
            while attempts < max_attempts:
                try:
                    response: Response = await func(*args, **kwargs)
                    response.attempts = attempts
                    return response
                except (ClientError, ValidationError) as error:
                    print(error)  # TODO: Change this to logging.error
                    attempts += 1
                    if attempts >= max_attempts:
                        return Response(excpt=type(error), item=None, attempts=attempts)

        return wrapper

    return decorator


def create_url(pagination: int, resource: Resources, base_url: URLs) -> AnyUrl:
    return AnyUrl(f"{base_url.value}/{resource.value}?_page={pagination}")


@retry(max_attempts=2)
async def send_request(
    session: aiohttp.ClientSession,
    url: AnyUrl,
) -> Response:
    print(str(url))  # TODO: Change this to logging.debug
    async with session.get(str(url), verify_ssl=False) as response:
        response.raise_for_status()
        items = TypeAdapter(list[Item | None]).validate_python(await response.json())
        return Response(item=items, excpt=None)


async def start_fetching_cycle(
    session: aiohttp.ClientSession, resource: Resources, base_url: URLs
):
    result_had_items = True
    pagination = 0
    results: list[Response] = []
    while result_had_items:
        pagination += 1
        url = create_url(
            resource=resource,
            pagination=pagination,
            base_url=base_url,
        )
        task = asyncio.create_task(send_request(session=session, url=url))
        result = await task
        result_had_items = result.item and len(result.item) > 0
        results.append(result)
    return results


async def main():
    resources: list[Resources] = [Resources.posts, Resources.comments]

    async with aiohttp.ClientSession() as session:
        tasks = list()
        for resource in resources:
            task = asyncio.create_task(
                start_fetching_cycle(
                    session=session,
                    resource=resource,
                    base_url=URLs.JsonPlaceHolder,
                )
            )
            tasks.append(task)
        result = await asyncio.gather(*tasks)
        print(f"Finished fetching -> {result}")


asyncio.run(main())
