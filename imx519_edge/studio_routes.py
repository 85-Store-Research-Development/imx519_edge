"""サーバー（スタジオ）への中継の API。完成画像・処理の進み具合・iPhone カメラモードの写真。

85pi では処理しない。ID を確かめて、そのままスタジオに渡すだけ。
"""

import asyncio
import contextlib
import re
from collections.abc import Callable, Iterator

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response

from .session import SESSION_ID
from .studio import Studio

UPLOAD_ID = re.compile(r"^u[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
MAX_UPLOAD_BYTES = 150 * 1024 * 1024  # iPhone の ProRAW（48MP）は約 70 MB
# 受け取った分から順にスタジオへ送る。85pi のメモリに載るのは、この数のかたまりまで
RELAY_CHUNKS = 16


class _Aborted(Exception):
    """受け取りを途中でやめた（大きすぎる、接続が切れた）。送っている側を止める。"""


_END = object()


async def relay_body(request: Request, send: Callable[[Iterator[bytes], int | None], dict]):
    """本文を受け取りながら send（別のスレッドで動く）に渡し、send の結果を返す。

    本文を全部読んでから送ると、ProRAW では 1 枚で 70 MB ほど 85pi のメモリに載る。
    """
    length = request.headers.get("content-length")
    length = int(length) if length and length.isdigit() else None
    if length is not None and length > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "大きすぎます")

    loop = asyncio.get_running_loop()
    chunks: asyncio.Queue = asyncio.Queue(maxsize=RELAY_CHUNKS)

    def body() -> Iterator[bytes]:
        while True:
            chunk = asyncio.run_coroutine_threadsafe(chunks.get(), loop).result()
            if chunk is _END:
                return
            if isinstance(chunk, _Aborted):
                raise chunk
            yield chunk

    sender = asyncio.ensure_future(run_in_threadpool(send, body(), length))

    async def put(item) -> None:
        """入れる前に送る側が終わっていたら（届かない など）、その結果を出す。"""
        putting = asyncio.ensure_future(chunks.put(item))
        await asyncio.wait({putting, sender}, return_when=asyncio.FIRST_COMPLETED)
        if not putting.done():
            putting.cancel()
            await sender  # 例外ならここで出る
            raise HTTPException(502, "サーバーが受け取りを途中で終えました")

    def abort() -> None:
        while not chunks.empty():
            chunks.get_nowait()
        chunks.put_nowait(_Aborted())

    received = 0
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            received += len(chunk)
            if received > MAX_UPLOAD_BYTES:
                raise HTTPException(413, "大きすぎます")
            await put(chunk)
        await put(_END)
    except BaseException:
        if not sender.done():
            abort()
            with contextlib.suppress(BaseException):
                await sender
        raise
    return await sender, received


def studio_router(studio: Studio | None, guard: Callable, log: Callable[[str, str], None]):
    """guard は api.py の例外の変換（StudioError → 502 など）。"""
    router = APIRouter(prefix="/api/studio")

    def need_studio() -> Studio:
        if not studio:
            raise HTTPException(503, "サーバーの設定がありません")
        return studio

    def session_id_or_404(session_id: str) -> str:
        if not SESSION_ID.match(session_id):
            raise HTTPException(404, "ありません")
        return session_id

    def upload_id_or_404(upload_id: str) -> str:
        if not UPLOAD_ID.match(upload_id):
            raise HTTPException(404, "ありません")
        return upload_id

    # --- 完成画像と処理の進み具合 ---

    @router.get("/sessions")
    def sessions(cms_id: int | None = None):
        params = {"cms_id": cms_id} if cms_id is not None else None
        return guard(need_studio().json, "/api/sessions", params=params)

    @router.get("/sessions/{session_id}/{name}")
    def session_image(session_id: str, name: str):
        session_id_or_404(session_id)
        if name not in ("preview.jpg", "final.jpg"):
            raise HTTPException(404, "ありません")
        data = guard(need_studio().image, f"/api/sessions/{session_id}/{name}")
        return Response(data, media_type="image/jpeg", headers={"Cache-Control": "max-age=60"})

    @router.post("/sessions/{session_id}/reprocess")
    def session_reprocess(session_id: str):
        session_id_or_404(session_id)
        result = guard(need_studio().json, f"/api/sessions/{session_id}/reprocess", method="POST")
        log("info", f"処理をやり直します: {session_id}")
        return result

    # --- iPhone カメラモード（写真をサーバーに中継し、補正した画像を返す） ---

    @router.post("/uploads", status_code=201)
    async def upload(request: Request, filename: str):
        """本文は写真そのもの。85pi には置かず、受け取った分から順にサーバーに渡す。"""
        target = need_studio()
        result, size = await relay_body(
            request, lambda body, length: guard(target.upload, body, filename, length)
        )
        log("info", f"iPhone の写真をサーバーに送りました: {filename}（{size / 1e6:.1f} MB）")
        return result

    @router.get("/uploads")
    def uploads():
        return guard(need_studio().json, "/api/uploads")

    @router.get("/uploads/{upload_id}/{name}")
    def upload_file(upload_id: str, name: str, download: bool = False):
        upload_id_or_404(upload_id)
        if name not in ("preview.jpg", "final.jpg"):
            raise HTTPException(404, "ありません")
        params = {"download": "true"} if download and name == "final.jpg" else None
        data, disposition = guard(need_studio().file, f"/api/uploads/{upload_id}/{name}", params)
        headers = {"Cache-Control": "no-store"}
        if disposition:
            headers["Content-Disposition"] = disposition
        return Response(data, media_type="image/jpeg", headers=headers)

    @router.post("/uploads/{upload_id}/reprocess")
    def upload_reprocess(upload_id: str):
        upload_id_or_404(upload_id)
        return guard(need_studio().json, f"/api/uploads/{upload_id}/reprocess", method="POST")

    @router.delete("/uploads/{upload_id}")
    def upload_delete(upload_id: str):
        upload_id_or_404(upload_id)
        return guard(need_studio().json, f"/api/uploads/{upload_id}", method="DELETE")

    return router
