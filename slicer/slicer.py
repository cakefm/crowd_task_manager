import xml.dom.minidom as xml
import os

from collections import namedtuple
from PIL import Image

import sys
sys.path.append("..")
from common.settings import cfg
import common.file_system_manager as fsm
import common.tree_tools as tt

# TODO: ditch namedtuples and use dataclasses along with json libs for them so we don't need this ugly boilerplate stuff

class NotOnSamePageException(Exception):
    pass

# TODO: we need "staff_start" and "staff_end" support
class Slice(namedtuple("ImmutableSlice", ["score", "start", "end", "staff_start", "staff_end", "type", "tuple_size", "page", "same_line", "same_page"])):
    """
    Class for specifying slices and performing operations on them. It is initialized with a reference to
    a score instance, a starting measure index, an ending measure index (exclusive, as in Python), and a
    slice index. Note that a slice is immutable, this will allow for precomputing certain properties of 
    the measures within without worrying about changes to indices etc.
    """
    def __new__(clazz, score, start, end, staff_start, staff_end, slice_type, tuple_size, page):
        """Doing this allows making additional computed fields immutable as well."""
        if not staff_end:
            staff_end = len(score.measures[0].staffs)
        staff_measures = [measure.staffs[staff_start] for measure in score.measures[start:end]]
        self = super(Slice, clazz).__new__(
            clazz,
            score,
            start,
            end,
            staff_start,
            staff_end,
            slice_type,
            tuple_size,
            page,
            all([staff_measures[0].line_index == x.line_index for x in staff_measures]),
            all([staff_measures[0].page_index == x.page_index for x in staff_measures])
        )
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
        im = self.score.get_page_image(measures[0].staffs[0].page_index)

        # If all slices are on same line/page, a simple crop will suffice
        if self.same_page and self.same_line:
            return im.crop(measures[0].staffs[self.staff_start].ulc + measures[-1].staffs[self.staff_end-1].lrc)

        # Otherwise, if they are just on the same page, put the chosen slices on a blank image, respecting the original positions
        # TODO: im.width should instead be the combined width of all measures
        elif self.same_page:
            p_im = Image.new("RGB", (im.width, im.height), (255, 255, 255))
            for measure in measures:
                # Watch out for PIL fail here: if measure.ulc is a list and not a tuple, it will get modified for no apparent reason.
                # Very likely a bug, as it is undocumented.
                p_im.paste(im.crop(measure.staffs[self.staff_start].ulc + measure.staffs[self.staff_end-1].lrc), measure.staffs[self.staff_start].ulc)

            x0 = min(measure, key = lambda x : x.staffs[self.staff_start].ulc[0]).staffs[self.staff_start].ulc[0]
            y0 = min(measure, key = lambda x : x.staffs[self.staff_start].ulc[1]).staffs[self.staff_start].ulc[1]
            x1 = max(measure, key = lambda x : x.staffs[self.staff_start].ulc[0]).staffs[self.staff_end-1].lrc[0]
            y1 = max(measure, key = lambda x : x.staffs[self.staff_start].ulc[1]).staffs[self.staff_end-1].lrc[1]

            return p_im.crop((x0, y0, x1, y1))

        # Won't handle slices that are not on the same page (for now...)
        else:
            raise NotOnSamePageException(f"Measures {self.start}-{self.end} are not on the same page!")

    def get_name(self):
        """
        Creates a name based on the properties of the slice.
        """
        return f"{self.tuple_size}-{self.type}_m{self.start+1}-{self.end+1}_s{self.staff_start+1}-{self.staff_end+1}.jpg"

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
            "name": self.get_name(),
            "score": self.score.name,
            "start": self.start,
            "end": self.end,
            "staff_start": self.staff_start,
            "staff_end": self.staff_end,
            "type": self.type,
            "tuple_size": self.tuple_size,
            "page": self.page
        }


# Convenience classes
Staff = namedtuple("Staff", ["ulc", "lrc", "width", "height", "index", "measure_index", "line_index", "page_index", "xml", "has_clef"])
Measure = namedtuple("Measure", ["staffs", "index", "xml", "context", "score_def_before_measure"])
Line = namedtuple("Line", ["measures", "start", "index"])
Page = namedtuple("Page", ["lines", "start", "index", "image_name"])


# TODO: A score needs to have a context
class Score:
    """
    The score objects contains the score data:
    - The measures with their corresponding XML data
    - The lines with the measures
    - The pages with the lines and page image paths
    """
    def __init__(self, name):
        # Create all relevant paths and names
        mei_path = fsm.get_sheet_whole_directory(name) / "aligned.mei"
        self.pages_path = fsm.get_sheet_pages_directory(name)
        self.name = name

        # Data structures
        self.measures = []
        self.lines = []
        self.pages = []
        self.images = {}

        # MEI parsing
        self.mei = xml.parse(str(mei_path))

        # Storing the zones in a dict and collect page images
        image_names = []
        zones = {}
        for surface in self.mei.getElementsByTagName("surface"):
            graphic = surface.getElementsByTagName("graphic")[0]
            image_name = graphic.attributes["target"].value
            image_names.append(image_name)
            for zone in surface.getElementsByTagName("zone"):
                zones[zone.attributes["xml:id"].value] = zone

        line = []
        page = []
        entries = [x for x in self.mei.getElementsByTagName("section")[0].childNodes if x.nodeType==xml.Node.ELEMENT_NODE]
        skipped_first_page_tag = False
        accumulated_score_def = self.mei.getElementsByTagName("scoreDef")[0]  # The initial one
        last_score_def_index = -1
        score_def_before_measure = None
        self.context = self.build_initial_context()
        for entry_index, entry in enumerate(entries):
            if entry.tagName == "pb":
                if not skipped_first_page_tag:
                    skipped_first_page_tag = True
                else:
                    self.pages.append(Page(tuple(page), page[0].measures[0].index, len(self.pages), image_names[len(self.pages)]))
                    del page[:]
            if entry.tagName == "scoreDef":
                self.update_score_def_with_score_def(accumulated_score_def, entry)
                last_score_def_index = entry_index
                score_def_before_measure = entry
                pass
            if entry.tagName == "sb" or entry.tagName == "pb" and line:
                line_obj = Line(tuple(line), line[0].index, len(self.lines))
                self.lines.append(line_obj)
                page.append(line_obj)
                del line[:]
            if entry.tagName == "measure":
                staffs = []
                for staff in entry.getElementsByTagName("staff"):
                    zone = zones[staff.attributes["facs"].value[1:]]
                    ulc = tuple([int(v) for v in (zone.attributes["ulx"].value, zone.attributes["uly"].value)])  # Upper left corner
                    lrc = tuple([int(v) for v in (zone.attributes["lrx"].value, zone.attributes["lry"].value)])  # Lower right corner

                    # TODO: this seems redundant, should be solved by making specific slices instead
                    has_clef = False
                    # If the line list is empty, this measure is the first measure, and thus the staff contains a clef
                    if not line:
                        has_clef = True

                    inner_xml = staff.toxml()

                    score_staff = Staff(ulc, lrc, lrc[0]-ulc[0], lrc[1]-ulc[1], len(staffs), len(self.measures), len(self.lines), len(self.pages), inner_xml, has_clef)
                    staffs.append(score_staff)

                # Adapt context for measure
                measure_context = self.context.cloneNode(deep=True)
                measure_context_score_def = measure_context.getElementsByTagName("scoreDef")[0]
                self.update_score_def_with_score_def(measure_context_score_def, accumulated_score_def)
                for staffDef in measure_context_score_def.getElementsByTagName("staffDef"):
                    staff_n = staffDef.getAttribute("n")
                    clef, clef_entry_index = self.backtrack_first_staff_with_clef(entry, entries, staff_n)

                    # If the clef came later than the last scoredef, we should override the clef
                    if clef and clef_entry_index > last_score_def_index:
                        self.update_score_def_with_clef(measure_context_score_def, clef, staff_n)

                score_def_before_measure_xml = None
                if score_def_before_measure:
                    score_def_before_measure_xml = score_def_before_measure.toxml()
                    score_def_before_measure = None

                score_measure = Measure(
                    staffs,
                    len(self.measures),
                    entry.toxml(),
                    measure_context.toxml(),
                    score_def_before_measure_xml)

                self.measures.append(score_measure)
                line.append(score_measure)

    def backtrack_first_measure_with_clef(self, measure, entries):
        measure_index = entries.index(measure)
        clef_measure = None
        clef_measure_index = -1
        for entry in reversed(entries[:measure_index]):
            if len(entry.getElementsByTagName("clef")) > 0:
                clef_measure = entry
                clef_measure_index = entries.index(clef_measure)
                break
        return clef_measure, clef_measure_index

    def backtrack_first_staff_with_clef(self, measure, entries, n):
        measure_index = entries.index(measure)
        clef = None
        clef_entry_index = -1

        for entry in reversed(entries[:measure_index]):
            staffs = entry.getElementsByTagName("staff")
            for staff in staffs:
                if staff.getAttribute("n") == n:
                    clefs = staff.getElementsByTagName("clef")
                    if len(clefs) > 0:
                        clef = clefs[-1]  # take the last one
                        clef_entry_index = entries.index(entry)
                        break
            if clef:
                break
        return clef, clef_entry_index

    def build_initial_context(self):
        context = self.mei.documentElement.cloneNode(deep=True)
        tt.delete_node(context.getElementsByTagName("facsimile")[0])
        section = context.getElementsByTagName("section")[0]
        tt.replace_child_nodes(section, [])
        section.appendChild(tt.create_element_node("PUT_TASK_XML_HERE"))
        return context

    # TODO: Breaks when clef is in measure instead of staff, will not be the case for now
    def update_score_def_with_clef(self, score_def, clef, n):
        initial_staff_defs = score_def.getElementsByTagName("staffDef")
        clef_shape = clef.getAttribute("shape")
        clef_line = clef.getAttribute("line")
        for staff_def in initial_staff_defs:
            if staff_def.getAttribute("n") == n:
                if clef_shape:
                    staff_def.setAttribute("clef.shape", clef_shape)
                if clef_line:
                    staff_def.setAttribute("clef.line", clef_line)

    def update_score_def_with_score_def(self, score_def, new_score_def):
        attributes = ["key.mode", "key.sig", "meter.unit", "meter.count", "clef.size", "clef.shape"]
        staff_defs = score_def.getElementsByTagName("staffDef")
        new_staff_defs = new_score_def.getElementsByTagName("staffDef")

        for attr in attributes:
            # Sometimes, the score def itself contains some properties, we should try to set those as well
            value = new_score_def.getAttribute(attr)
            if value:
                score_def.setAttribute(attr, value)

            for old_staff_def, new_staff_def in tt.matching_pairs(staff_defs, new_staff_defs, ["n"]):
                value = new_staff_def.getAttribute(attr)
                if value:
                    old_staff_def.setAttribute(attr, value)

    def get_page_image(self, page_index):
        """
        Lazily loads the images associated with pages.
        """
        image_name = self.pages[page_index].image_name
        if image_name not in self.images:
            self.images[image_name] = Image.open(str(self.pages_path / image_name))
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

    def get_page_slices(self, n=1, start=0, end=None, staff_start=0, staff_end=None):
        """
        Gets slices that start at the beginning of a page and end at the end of a page.
        By default tuples of single elements are created, adjust `n` to change this.
        If `end` is not given, it takes everything until the end.
        """
        iterator = self._get_n_iterator(self.pages[start:end], n)
        slices = []
        for pages in iterator:
            s = pages[0].lines[0].measures[0].index
            e = pages[-1].lines[-1].measures[-1].index
            slices.append(Slice(self, s, e + 1, staff_start, staff_end, "pages", n, -1))
        return slices

    def get_line_slices(self, n=1, start=0, end=None, staff_start=0, staff_end=None):
        """
        Gets slices that start at the beginning of a line and end at the end of a line.
        By default tuples of single elements are created, adjust `n` to change this.
        If `end` is not given, it takes everything until the end.
        """
        iterator = self._get_n_iterator(self.lines[start:end], n)
        slices = []
        for lines in iterator:
            s = lines[0].measures[0].index
            e = lines[-1].measures[-1].index
            p = lines[0].measures[0].staffs[0].page_index
            slices.append(Slice(self, s, e + 1, staff_start, staff_end, "lines", n, p))
        return slices

    def get_measure_slices(self, n=1, start=0, end=None, staff_start=0, staff_end=None):
        """
        Gets slices that start at given measure index.
        By default tuples of single elements are created, adjust `n` to change this.
        If `end` is not given, it takes everything until the end.
        """
        iterator = self._get_n_iterator(self.measures[start:end], n)
        slices = []
        for measures in iterator:
            s = measures[0].index
            e = measures[-1].index
            p = measures[0].staffs[0].page_index
            slices.append(Slice(self, s, e + 1, staff_start, staff_end, "measures", n, p))
        return slices

    def to_db_dict(self):
        """
        Get a dictionary representation of the object for storage in the database.
        """

        measures = []
        for measure in self.measures:
            staffs = [staff._asdict() for staff in measure.staffs]
            measures.append(Measure(
                staffs,
                measure.index,
                measure.xml,
                measure.context,
                measure.score_def_before_measure)._asdict())

        return {
            "name" : self.name,
            "measures": measures,
            "context": tt.purge_non_element_nodes(self.context).toprettyxml()
        }
