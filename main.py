from typing import AsyncGenerator, Callable
from dataclasses import dataclass
import logging
import aiohttp
import asyncio


@dataclass
class Item:
    id: int
    body: str


def retry(func: Callable, retries: int, *args, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception:
            continue
    return None


class JsonplaceholderAPI:
    base_url = "https://jsonplaceholder.typicode.com/"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def fetch_with_retry(
        self, url: str, retries: int
    ) -> aiohttp.ClientResponse | None:
        async def get(session, url):
            async with session.get(url) as response:
                res = await response.json()
            return res

        return await retry(get, retries, self.session, url)

    async def retrieve_data(
        self,
        resource: str,
        pagination: int = 1,
    ) -> AsyncGenerator[Item, None]:
        url = self.base_url + f"{resource}?_page={pagination}"
        try:
            items = await self.fetch_with_retry(url, retries=10)
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
