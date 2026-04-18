"""
Патч TLS для совместимости с корпоративным MITM / самоподписанными цепочками GigaChat.
Если GIGACHAT_VERIFY_SSL_CERTS=true — патчи не применяются (прод с нормальным CA).
"""
import os
import ssl


def _verify_ssl_enabled() -> bool:
    return os.environ.get("GIGACHAT_VERIFY_SSL_CERTS", "False").strip().lower() in (
        "1",
        "true",
        "yes",
    )


if not _verify_ssl_enabled():
    os.environ.setdefault("PYTHONHTTPSVERIFY", "0")
    os.environ.setdefault("CURL_CA_BUNDLE", "")
    os.environ.setdefault("REQUESTS_CA_BUNDLE", "")
    os.environ.setdefault("SSL_CERT_FILE", "")
    os.environ.setdefault("GIGACHAT_VERIFY_SSL_CERTS", "False")

    try:
        ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[attr-defined]
    except AttributeError:
        pass

    try:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass

    # httpx: только если verify не передан явно
    try:
        import httpx

        _orig_client_init = httpx.Client.__init__

        def _patched_client_init(self: httpx.Client, *args: object, **kwargs: object) -> None:
            if "verify" not in kwargs:
                kwargs["verify"] = False
            _orig_client_init(self, *args, **kwargs)

        httpx.Client.__init__ = _patched_client_init  # type: ignore[method-assign]

        _orig_async_init = httpx.AsyncClient.__init__

        def _patched_async_init(self: httpx.AsyncClient, *args: object, **kwargs: object) -> None:
            if "verify" not in kwargs:
                kwargs["verify"] = False
            _orig_async_init(self, *args, **kwargs)

        httpx.AsyncClient.__init__ = _patched_async_init  # type: ignore[method-assign]
    except ImportError:
        pass
