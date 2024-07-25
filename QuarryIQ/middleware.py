from django.utils.deprecation import MiddlewareMixin
import logging

logger = logging.getLogger(__name__)


class TestContentSecurityPolicyMiddleware(MiddlewareMixin):
    def process_response(self, request, response):
        # Define the CSP policy
        policy = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' "
            "https://cdn.jsdelivr.net "
            "https://code.jquery.com "
            "https://cdnjs.cloudflare.com "
            "https://netdna.bootstrapcdn.com "
            "https://cdn.jsdelivr.net/gh/python-visualization/folium "
            "https://cdn.jsdelivr.net/npm/leaflet@1.9.3/dist "
            "https://cdn.jsdelivr.net/npm/bootstrap@5.2.2/dist "
            "https://cdnjs.cloudflare.com/ajax/libs/Leaflet.awesome-markers/2.0.2 "
            "https://cdn.plot.ly; "
            "style-src 'self' 'unsafe-inline' "
            "https://cdn.jsdelivr.net "
            "https://netdna.bootstrapcdn.com "
            "https://cdnjs.cloudflare.com "
            "https://cdn.jsdelivr.net/npm/leaflet@1.9.3/dist "
            "https://cdn.jsdelivr.net/npm/bootstrap@5.2.2/dist "
            "https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@6.2.0 "
            "https://cdn.jsdelivr.net/gh/python-visualization/folium/folium/templates; "
            "font-src 'self' "
            "https://cdnjs.cloudflare.com; "
            "img-src 'self' data: "
            "https://tile.openstreetmap.org "
            "https://cdn.jsdelivr.net; "  # Added this to allow images from cdn.jsdelivr.net
            "connect-src 'self'; "
            "frame-src 'self'; "
            "object-src 'none'; "
            "media-src 'none'; "
            "frame-ancestors 'none'; "
            "report-uri /csp-report-endpoint/"
        )

        # Apply the CSP header
        response['Content-Security-Policy-Report-Only'] = policy

        return response





class XContentTypeOptionsMiddleware(MiddlewareMixin):
    def process_response(self, request, response):
        response['X-Content-Type-Options'] = 'nosniff'
        logger.debug("Added X-Content-Type-Options header")
        return response

class XXSSProtectionMiddleware(MiddlewareMixin):
    def process_response(self, request, response):
        response['X-XSS-Protection'] = '1; mode=block'
        logger.debug("Added X-XSS-Protection header")
        return response