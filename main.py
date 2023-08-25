from typing import AsyncGenerator, Callable, Type, Optional, Dict, Any
from dataclasses import dataclass
import logging
import aiohttp
import asyncio
import time


@dataclass
class Item:
    id: int
    body: str


def retry_decorator(
    retries: Optional[int] = None,
    exceptions: Type[Exception] | tuple[Type[Exception], ...] = Exception,
):
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            _retries = retries
            while _retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    _retries -= 1
                    print(f"Retrying: {_retries}")
                    time.sleep(1)
                    continue
            raise ValueError

        return wrapper

    return decorator


class JsonplaceholderAPI:
    base_url = "https://jsonplaceholder.typicode.com/"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    @retry_decorator(retries=10, exceptions=(Exception, aiohttp.ClientError))
    async def fetch(self, url: str) -> Optional[Dict[str, Any]]:
        async def get(session, url):
            async with session.get(url) as response:
                return await response.json()

        return await get(self.session, url)

    async def retrieve_data(
        self,
        resource: str,
        pagination: int = 1,
    ) -> AsyncGenerator[Item, None]:
        url = self.base_url + f"{resource}?_page={pagination}"
        try:
            items = await self.fetch(url)
            for item in items:
                yield self.create_item(item)
            if items is not None:
                async for item in self.retrieve_data(
                    resource=resource, pagination=pagination + 1
                ):
                    yield item
        except Exception as ex:
            logging.error(f"Exception: {ex}")

    @staticmethod
    def create_item(item):
        item_id = item.get("id")
        item_body = item.get("body")
        return Item(
            id=item_id,
            body=item_body,
        )


async def process_items(
    api: JsonplaceholderAPI,
    resource: str,
) -> None:
    async for item in api.retrieve_data(resource=resource):
        print(item)


async def main():
    reses = ["posts", "comments"]
    async with aiohttp.ClientSession() as session:
        api = JsonplaceholderAPI(session=session)
        tasks = [asyncio.create_task(process_items(api, resource=res)) for res in reses]

        await asyncio.gather(*tasks)


asyncio.run(main())
