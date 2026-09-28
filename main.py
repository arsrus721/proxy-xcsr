from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

TARGET = "https://xcsr.finland-dc0.qpda.ru"

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}

client: httpx.AsyncClient


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(60.0, connect=10.0),
        follow_redirects=False,  # редиректы отдаём клиенту как есть
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )
    yield
    await client.aclose()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

ALL_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"]


@app.api_route("/{full_path:path}", methods=ALL_METHODS, include_in_schema=False)
async def proxy(request: Request, full_path: str):
    # сырой путь и сырая query — без перекодирования
    raw_path = request.scope.get("raw_path") or request.url.path.encode()
    query = request.scope.get("query_string", b"")
    url = TARGET + raw_path.decode("latin-1")
    if query:
        url += "?" + query.decode("latin-1")

    # тело читаем всегда, в т.ч. у GET / HEAD / OPTIONS
    body = await request.body()

    headers = [
        (k, v)
        for k, v in request.headers.raw
        if k.decode().lower() not in HOP_BY_HOP | {"host", "content-length"}
    ]

    upstream_req = client.build_request(
        request.method, url, headers=headers, content=body
    )
    upstream = await client.send(upstream_req, stream=True)

    resp = StreamingResponse(
        upstream.aiter_raw(),  # raw: не распаковываем, content-encoding остаётся валидным
        status_code=upstream.status_code,
        background=BackgroundTask(upstream.aclose),
    )
    # raw_headers сохраняет дубли (например, несколько Set-Cookie)
    resp.raw_headers = [
        (k, v)
        for k, v in upstream.headers.raw
        if k.decode().lower() not in HOP_BY_HOP
    ]
    return resp
