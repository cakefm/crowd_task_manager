import xml.dom.minidom as xml
import os

from collections import namedtuple
from PIL import Image

class NotOnSamePageException(Exception):
    pass

class Slice(namedtuple("ImmutableSlice", ["score", "start", "end", "same_line", "same_page"])):
    """
    Class for specifying slices and performing operations on them. It is initialized with a reference to
    a score instance, a starting measure index, and an ending measure index (exclusive, as in Python).
    Note that a slice is immutable, this will allow for precomputing certain properties of the measures within
    without worrying about changes to indices etc.
    """
    def __new__(clazz, score, start, end):
        """Doing this allows making additional computed fields immutable as well."""
        measures = score.measures[start:end]
        self = super(Slice, clazz).__new__(clazz,
            score,
            start,
            end,
            all([measures[0].line == x.line for x in measures]),
            all([measures[0].page == x.page for x in measures]))
        return self

    def get_image(self):
        """
        Obtains an image from the slice, retrieving the needed images and measures from the Score
        instance. If the measures are not on the same page, a NotOnSamePageException exception is
        thrown. If the measures are not on the same line, a larger crop will occur that respects
        their original locations on the score. This can be made more convenient in the future as
        needed.
        """

        measures = self.get_measures()
        im = self.score.get_page_image(measures[0].page)

        # If all slices are on same line/page, a simple crop will suffice
        if self.same_page and self.same_line:
            return im.crop(measures[0].ulc + measures[-1].lrc)

        # Otherwise, if they are just on the same page, put the chosen slices on a blank image, respecting the original positions
        elif self.same_page:
            p_im = Image.new("RGB", (im.width, im.height), (255, 255, 255))
            for measure in measures:
                # Watch out for PIL fail here: if measure.ulc is a list and not a tuple, it will get modified for no apparent reason.
                # Very likely a bug, as it is undocumented.
                p_im.paste(im.crop(measure.ulc + measure.lrc), measure.ulc)

            x0 = min(measures, key = lambda x : x.ulc[0]).ulc[0]
            y0 = min(measures, key = lambda x : x.ulc[1]).ulc[1]
            x1 = max(measures, key = lambda x : x.ulc[0]).lrc[0]
            y1 = max(measures, key = lambda x : x.ulc[1]).lrc[1]

            return p_im.crop((x0, y0, x1, y1))

        # Won't handle slices that are not on the same page (for now...)
        else:
            raise NotOnSamePageException(f"Measures {self.start}-{self.end} are not on the same page!")

    def get_name(self, slice_type="slice"):
        """
        Creates a name based on the properties of the slice. slice_type can optionally be given to customize the
        name further based on the specifics on the slice being made.
        """
        return f"{slice_type}_{self.start}-{self.end}.jpg"

    def get_measures(self):
        """
        Gets the slice's measures from the score.
        """
        return self.score.measures[self.start:self.end]

    def to_db_dict(self):
        """
        Get a dictionary representation of the object for storage in the database.
        """
        return {
        "name" : self.get_name(), 
        "score" : self.score.name,
        "start" : self.start,
        "end" : self.end
        }


# Convenience classes
class Measure(namedtuple("Measure", ["ulc", "lrc", "width", "height", "index", "line", "page", "xml", "has_clef"])):
    """
    Get a dictionary representation of the object for storage in the database.
    """
    def to_db_dict(self):
        return {
        "index" : self.index,
        "line_index" : self.line,
        "page_index" : self.page,
        "xml" : self.xml,
        "has_clef" : self.has_clef
        }

Line = namedtuple("Line", ["measures", "start", "index"])
Page = namedtuple("Page", ["lines", "start", "index", "image_name"])
ScoreDef = namedtuple("ScoreDef", ["location", "xml"]) # "location" is the index of the first measure after it (in other words, first measure where it takes effect)

class NoScoreDefException(Exception):
    pass

class Score:
    """
    The score objects contains the score data:
    - The measures with their corresponding XML data
    - The lines with the measures
    - The pages with the lines and page image paths
    - The score definitions
    """
    def __init__(self, path):
        # Create all relevant paths and names
        mei_path = f"{path}{os.path.sep}whole{os.path.sep}aligned.mei"
        self.pages_path = f"{path}{os.path.sep}pages{os.path.sep}"
        self.name = os.path.basename(os.path.normpath(path))

        # Data structures
        self.measures = []
        self.lines = []
        self.pages = []
        self.images = {}
        self.score_defs = []

        # MEI parsing
        self.mei = xml.parse(mei_path)

        # Storing the zones in a dict and collect page images
        image_names = []
        zones = {}
        for surface in self.mei.getElementsByTagName("surface"):
            graphic = surface.getElementsByTagName("graphic")[0]
            image_name = graphic.attributes["target"].value
            image_names.append(image_name)
            for zone in surface.getElementsByTagName("zone"):
                zones[zone.attributes["xml:id"].value] = zone

        # Get the main score def
        score_node = self.mei.getElementsByTagName("score")[0]
        try:
            score_def_xml_node = [x for x in score_node.childNodes if x.nodeType == xml.Node.ELEMENT_NODE and x.tagName == 'scoreDef'][0]
        except IndexError as e:
            raise NoScoreDefException("ERROR: The MEI does not contain a global score def!") from e
        self.score_def = ScoreDef(0, score_def_xml_node.toxml())

        line = []
        page = []
        entries = [x for x in self.mei.getElementsByTagName("section")[0].childNodes if x.nodeType==xml.Node.ELEMENT_NODE]
        for entry in entries[1:]: # Skip the first page separator
            if entry.tagName == "pb" and page:
                self.pages.append(Page(tuple(page), page[0].measures[0].index, len(self.pages), image_names[len(self.pages)]))
                del page[:]
            if entry.tagName == "sb" or entry.tagName == "pb" and line:
                line_obj = Line(tuple(line), line[0].index, len(self.lines))
                self.lines.append(line_obj)
                page.append(line_obj)
                del line[:]
            if entry.tagName == "measure":
                zone = zones[entry.attributes["facs"].value[1:]]
                ulc = tuple([int(v) for v in (zone.attributes["ulx"].value, zone.attributes["uly"].value)])  # Upper left corner
                lrc = tuple([int(v) for v in (zone.attributes["lrx"].value, zone.attributes["lry"].value)])  # Lower right corner

                has_clef = False
                # If the line list is empty, this measure is the first measure, and thus contains a clef
                if not line: 
                    has_clef = True

                inner_xml = entry.toxml()
                score_measure = Measure(ulc, lrc, lrc[0]-ulc[0], lrc[1]-ulc[1], len(self.measures), len(self.lines), len(self.pages), inner_xml, has_clef)
                self.measures.append(score_measure)
                line.append(score_measure)
            if entry.tagName == "scoreDef":
                self.score_defs.append(ScoreDef(len(self.measures), entry.toxml()))


    def get_page_image(self, page_index):
        """
        Lazily loads the images associated with pages.
        """
        image_name = self.pages[page_index].image_name
        if image_name not in self.images:
            self.images[image_name] = Image.open(f"{self.pages_path}{os.path.sep}{image_name}")
        return self.images[image_name]

    def _get_n_iterator(self, L, n):
        """
        Builds a list of `n`-tuples that contain elements from `L` in sequence. 

        Example:
        L = [a, b, c, d]
        n = 2

        Result:
        [(a, b), (b, c), (c, d)]
        """
        Z = []
        for i in range(n):
            Z += [L[i:]]
        return zip(*Z)

    def get_page_slices(self, n=1, start=0, end=None):
        """
        Gets slices that start at the beginning of a page and end at the end of a page.
        By default tuples of single elements are created, adjust `n` to change this.
        If `end` is not given, it takes everything until the end.
        """
        iterator = self._get_n_iterator(self.pages[start:end], n)
        slices = []
        for pages in iterator:
            start = pages[0].lines[0].measures[0].index
            end = pages[-1].lines[-1].measures[-1].index
            slices.append(Slice(self, start, end + 1))
        return slices

    def get_line_slices(self, n=1, start=0, end=None):
        """
        Gets slices that start at the beginning of a line and end at the end of a line.
        By default tuples of single elements are created, adjust `n` to change this.
        If `end` is not given, it takes everything until the end.
        """
        iterator = self._get_n_iterator(self.lines[start:end], n)
        slices = []
        for lines in iterator:
            start = lines[0].measures[0].index
            end = lines[-1].measures[-1].index
            slices.append(Slice(self, start, end + 1))
        return slices

    def get_measure_slices(self, n=1, start=0, end=None):
        """
        Gets slices that start at given measure index.
        By default tuples of single elements are created, adjust `n` to change this.
        If `end` is not given, it takes everything until the end.
        """
        iterator = self._get_n_iterator(self.measures[start:end], n)
        slices = []
        for measures in iterator:
            start = measures[0].index
            end = measures[-1].index
            slices.append(Slice(self, start, end + 1))
        return slices

    def to_db_dict(self):
        """
        Get a dictionary representation of the object for storage in the database.
        """
        score_dict = {
        "name" : self.name,
        "measures": [dict(measure.to_db_dict()) for measure in self.measures],
        "score_defs" : [dict(self.score_def._asdict()) for score_def in self.score_defs],
        "main_score_def": dict(self.score_def._asdict())
        }

        return score_dict
