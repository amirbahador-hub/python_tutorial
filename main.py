import asyncio
from dataclasses import dataclass
from enum import StrEnum, auto
from functools import wraps
from typing import Any, Type

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


@dataclass
class ApiImpl:
    _type: Type[Any]
    sub_dirs: list[Any]


class Response(BaseModel):
    item: list[Any] | None = None
    excpt: Type[Exception] | None = None
    attempts: PositiveInt = 1


STRATEGY: dict[URLs, ApiImpl] = {
    URLs.JsonPlaceHolder: ApiImpl(
        _type=list[Item | None],
        sub_dirs=[Resources.posts, Resources.comments],
    )
}


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
    _type=list[Any | None],
) -> Response:
    print(str(url))  # TODO: Change this to logging.debug
    async with session.get(str(url), verify_ssl=False) as response:
        response.raise_for_status()
        items = TypeAdapter(_type).validate_python(await response.json())
        return Response(item=items, excpt=None)


async def start_fetching_cycle(
    session: aiohttp.ClientSession,
    resource: Resources,
    base_url: URLs,
    _type=list[Any | None],
):
    result_had_items = True
    pagination = 0
    results: list[Response] = []
    while result_had_items:
        pagination += 1
        url = create_url(resource=resource, pagination=pagination, base_url=base_url)
        task = asyncio.create_task(send_request(session=session, url=url, _type=_type))
        result = await task
        result_had_items = result.item and len(result.item) > 0
        results.append(result)
    return results


async def main(to_fetch: URLs):
    strategy = STRATEGY.get(to_fetch)
    assert strategy is not None

    async with aiohttp.ClientSession() as session:
        tasks = list()
        for resource in strategy.sub_dirs:
            task = asyncio.create_task(
                start_fetching_cycle(
                    session=session,
                    resource=resource,
                    base_url=URLs.JsonPlaceHolder,
                    _type=strategy._type,
                )
            )
            tasks.append(task)
        await asyncio.gather(*tasks)


asyncio.run(main(URLs.JsonPlaceHolder))
