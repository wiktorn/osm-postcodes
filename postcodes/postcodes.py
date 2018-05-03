import concurrent.futures
import functools
import multiprocessing
import pathlib
import typing

import collections
import osmium
import shapely.geometry
import shapely.wkb
import tqdm

wkb_factory = osmium.geom.WKBFactory()


def osmium2shapely(o):

    wkb = wkb_factory.create_multipolygon(o)
    return shapely.wkb.loads(wkb, hex=True)


class PostcodeExtractor(osmium.SimpleHandler):
    def __init__(self, reporthook):
        super(PostcodeExtractor, self).__init__()
        self.postcodes = collections.defaultdict(list)
        self.reporthook = reporthook

    def node(self, n):
        self.reporthook(1)
        postcode = n.tags.get('addr:postcode')
        if postcode:
            self.accumulate_postcodes(postcode, shapely.geometry.Point(n.location.lon, n.location.lat))

    def area(self, a):
        self.reporthook(1)
        postcode = a.tags.get('addr:postcode')
        if postcode:
            try:
                shape = osmium2shapely(a).representative_point()
                self.accumulate_postcodes(postcode, shape)
            except RuntimeError:
                # ignore invalid area errors
                pass

    def accumulate_postcodes(self, postcode: str, point: shapely.geometry.base.BaseGeometry):
        self.postcodes[postcode].append(point)


class UrlRetriveProgressBar(tqdm.tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def union_shapes(lst):
    first = lst[0]
    return functools.reduce(shapely.geometry.Point.union, lst[1:], first)


def get_postcodes(data: pathlib.Path) -> typing.Dict[str, shapely.geometry.Point]:
    with tqdm.tqdm(desc="Extracting postcodes") as pb:
        pe = PostcodeExtractor(pb.update)
        pe.apply_file(str(data))

    ret = {}

    def job(_postcode, _positions):
        ret[_postcode] = union_shapes(_positions).centroid

    with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() - 1) as executor:
        futures = [executor.submit(job, postcode, positions) for postcode, positions in pe.postcodes.items()]

        # give meaningful progressbar
        for _ in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Generating position"
        ):
            pass

    return ret
