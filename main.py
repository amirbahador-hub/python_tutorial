from typing import AsyncGenerator, Any
import json
import asyncio
import logging
from dataclasses import dataclass

import aiohttp
from aiohttp.client_exceptions import ClientError, ContentTypeError


class PaginationEndedException(Exception):
    pass


class FetchingDataFailedException(Exception):
    pass


class JsonParsingException(Exception):
    pass


@dataclass
class Item:
    id: int
    body: str


class Request:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_with_retry(self, url: str, retries: int = 1) -> aiohttp.ClientResponse:
        for _ in range(retries):
            try:
                response = await self.session.get(url)
                return response
            except ClientError as error:
                logging.info(error)

        raise FetchingDataFailedException()


class JsonPlaceholderApi:
    base_url = 'https://jsonplaceholder.typicode.com/'

    def __init__(self, request: Request):
        self.request = request

    async def retrieve_data(self, resource: str, pagination: int = 1) -> AsyncGenerator[Item, None]:
        url = self.base_url + f"{resource}?_page={pagination}"
        place_holder_response = await self.request.get_with_retry(url, retries=5)
        items = await self.extract_data(client_response=place_holder_response)
        for item in items:
            yield self.create_item(item)

    async def extract_data(self, client_response: aiohttp.ClientResponse) -> list[dict]:
        items = await self.get_json(client_response)
        if len(items) > 0:
            return items
        else:
            raise PaginationEndedException()

    async def get_json(self, client_response: aiohttp.ClientResponse) -> Any:
        try:
            return await client_response.json()
        except (json.JSONDecodeError, ContentTypeError):
            raise JsonParsingException()

    def create_item(self, item: dict) -> Item | None:
        try:
            return Item(id=item.get('id'), body=item.get('body'))
        except AttributeError:
            return None


async def process_items_with_pagination(resource: str, api: JsonPlaceholderApi) -> None:
    pagination = 0
    errors = 0
    while errors <= 10:
        pagination += 1
        try:
            async for item in api.retrieve_data(resource, pagination):
                print(item)
        except PaginationEndedException:
            break
        except (FetchingDataFailedException, JsonParsingException):
            errors += 1
            continue


async def main():
    resources = ["posts", "comments"]

    tasks = list()
    async with aiohttp.ClientSession() as session:
        request = Request(session=session)
        api = JsonPlaceholderApi(request=request)
        for resource in resources:
            tasks.append(
                asyncio.create_task(process_items_with_pagination(resource=resource, api=api))
            )
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())