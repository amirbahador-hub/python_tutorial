from typing import AsyncGenerator
from dataclasses import dataclass
import logging
import aiohttp
import asyncio


@dataclass
class Item:
    id: int
    body: str

class JsonplaceholderAPI:

    base_url = 'https://jsonplaceholder.typicode.com/'

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def fetch_with_retry(
        self, url: str, retries: int
    ) -> aiohttp.ClientResponse | None:
            try:
                for i in range(retries):
                    async with self.session.get(url) as response:
                        return await response.json()
            except Exception:
                return None
        

    async def retrieve_data(
        self,
        resource: str,
        pagination: int = 1,
    ) -> AsyncGenerator[Item, None]:
        url = self.base_url + f"{resource}?_page={pagination}"
        try:
            items = await self.fetch_with_retry(url, retries=10)
            for item in items:
                item_id = item.get("id")
                item_body = item.get("body")
                yield Item(
                    id=item_id,
                    body=item_body,
                )
            if items is not None:
                    async for item in self.retrieve_data(
                        resource=resource, pagination=pagination + 1
                    ):
                        yield item
        except Exception as ex:
            logging.error(
                f"Exception: {ex}"
            )


async def process_items(
    api: JsonplaceholderAPI,
    resource: str,
) -> None:
    async for item in api.retrieve_data(
        resource=resource
    ):
        print(item)


async def main():
    reses = ["posts", "comments"]
    async with aiohttp.ClientSession() as session:
        tasks = list()

        api = JsonplaceholderAPI(session=session)
        for res in reses:
            task = asyncio.create_task(process_items(
                api,
                resource=res
            ))
            tasks.append(task)
        await asyncio.gather(*tasks)

asyncio.run(main())