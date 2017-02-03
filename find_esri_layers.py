import argparse
import logging
import posixpath
import re
import requests

def check_esri_response(resp):
    resp.raise_for_status()
    error = resp.json().get('error')
    if error:
        raise ValueError("{}: {}".format(resp.request.url, error.get('message')))

class EsriServer(object):
    def __init__(self, url, **kwargs):
        self._root_url = url

        root_logger = kwargs.get('logger') or logging.getLogger()
        self._logger = root_logger.getChild('esriserver')


    def get_server_metadata(self):
        resp = requests.get(self._root_url, params=dict(f='json'))
        check_esri_response(resp)
        data = resp.json()

        if data.get('folder') or data.get('services'):
            return data
        else:
            raise ValueError("URL doesn't appear to be an Esri server")

    def _iter_services(self, metadata):
        for service in metadata.get('services', []):
            service_url = posixpath.join(self._root_url, '{name}/{type}'.format(**service))
            if service.get('type') not in ('MapServer', 'FeatureServer'):
                self._logger.debug("Skipping %s because service type %s", service_url, service.get('type'))
                continue

            resp = requests.get(service_url, params=dict(f='json'))
            check_esri_response(resp)
            data = resp.json()
            data['@url'] = service_url
            yield data

    def _iter_folder(self, metadata):
        for service in self._iter_services(metadata):
            yield service

        for folder in metadata.get('folders', []):
            service_url = posixpath.join(self._root_url, folder)
            resp = requests.get(service_url, params=dict(f='json'))
            check_esri_response(resp)
            for service in self._iter_folder(resp.json()):
                yield service

    def iter_services(self):
        metadata = self.get_server_metadata()
        for service in self._iter_folder(metadata):
            yield service

def main():
    parser = argparse.ArgumentParser(description='Helps find Esri layers useful for OpenAddresses')
    parser.add_argument('url', help='the Esri URL to search')
    parser.add_argument('-v', '--verbose', help='increase verbosity',
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING)

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    logger = logging.getLogger('esri')

    s = EsriServer(args.url, logger=logger)
    for service in s.iter_services():
        layers = service.get('layers', [])
        for layer in layers:
            layer_url = posixpath.join(service.get('@url'), str(layer.get('id')))

            if re.match(r'.*(addr|parcel).*', layer.get('name'), re.IGNORECASE):
                print "{}: {}".format(
                    layer_url,
                    layer.get('name'),
                )

            resp = requests.get(layer_url, params=dict(f='json'))
            check_esri_response(resp)
            layer = resp.json()

            fields = layer.get('fields')

            if not fields:
                logger.debug("No fields in %s", layer_url)
                continue

            for field in fields:
                if re.match(r'.*(situs|location|st.?name).*', field.get('alias'), re.IGNORECASE):
                    print "{}: {} has field {}".format(
                        layer_url,
                        layer.get('name'),
                        field.get('alias'),
                    )

if __name__ == '__main__':
    main()
