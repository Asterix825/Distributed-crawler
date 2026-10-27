from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "session_id"}

def normalize_url(raw_url: str) -> str | None:
    try:
        parts = urlsplit(raw_url.strip())
        if parts.scheme not in ("http", "https") or not parts.hostname:
            return None
        
        scheme = parts.scheme.lower()
        host = parts.hostname.lower()
        
        # Strip default ports
        port = parts.port
        if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            netloc = host
        elif port:
            netloc = f"{host}:{port}"
        else:
            netloc = host

        # Clean path
        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        # Strip tracking params & sort remaining query keys alphabetically
        filtered_query = [
            (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k.lower() not in TRACKING_PARAMS
        ]
        filtered_query.sort(key=lambda x: x[0])
        query_str = urlencode(filtered_query)

        # Drop #fragment completely
        return urlunsplit((scheme, netloc, path, query_str, ""))
    except Exception:
        return None