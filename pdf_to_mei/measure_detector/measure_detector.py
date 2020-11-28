import matplotlib.pyplot as plt
import numpy as np
import scipy.ndimage as im
import scipy.signal as sig
import sys
from collections import namedtuple
from PIL import Image, ImageOps

np.set_printoptions(threshold=sys.maxsize)
np.set_printoptions(linewidth=sys.maxsize)

Page = namedtuple("Page", ["height", "width", "systems"])
System = namedtuple("System", ["ulx", "uly", "lrx", "lry", "v_profile", "h_profile", "measures"])
Measure = namedtuple("Measure", ["ulx", "uly", "lrx", "lry", "staffs"])
Staff = namedtuple("Staff", ["ulx", "uly", "lrx", "lry"])


# This method does some pre-processing on the pages
def open_and_preprocess(path):
    original = Image.fromarray(plt.imread(path)[:, :, 0]).convert('L')  # Convert to "luminance" (single-channel greyscale)
    img = ImageOps.autocontrast(original)                             # Does some automatic histogram stretching to enhance contrast
    img = ImageOps.invert(img)                                   # Inverts the image so we get white on black
    img = img.point(lambda x: x > 50)                           # Threshold on value 50, this will convert the range into [0, 1] (floats though!)
    return np.asarray(img)


def contiguous_regions(condition):
    """
    Finds contiguous True regions of the boolean array "condition". Returns
    a 2D array where the first column is the start index of the region and the
    second column is the end index.
    See: https://stackoverflow.com/questions/4494404/find-large-number-of-consecutive-values-fulfilling-condition-in-a-numpy-array
    """

    # Find the indicies of changes in "condition"
    d = np.diff(condition)
    idx, = d.nonzero() 

    # We need to start things after the change in "condition". Therefore, 
    # we'll shift the index by 1 to the right.
    idx += 1

    if condition[0]:
        # If the start of condition is True prepend a 0
        idx = np.r_[0, idx]

    if condition[-1]:
        # If the end of condition is True, append the length of the array
        idx = np.r_[idx, condition.size] # Edit

    # Reshape the result into two columns
    idx.shape = (-1, 2)
    return idx


def modified_zscore(data):
    """
    Calculate the modified z-score of a 1 dimensional numpy array
    Reference:
        Boris Iglewicz and David Hoaglin (1993), "Volume 16: How to Detect and
        Handle Outliers", The ASQC Basic References in Quality Control:
        Statistical Techniques, Edward F. Mykytka, Ph.D., Editor.
    """
    median = np.median(data)
    deviation = data - median
    mad = np.median(np.abs(deviation)) + 1e-6  # Small epsilon to avoid division by 0
    return 0.6745 * deviation / mad


def find_systems_in_page(img):
    h, w = np.shape(img)

    # Here we will use binary-propagation to fill the systems, making them fully solid.
    # We can then use the mean across the horizontal axis to find where there is "mass" on the vertical axis.
    img_solid = im.binary_fill_holes(img)
    mean_solid_systems = np.mean(img_solid, axis=1)
    labels, count = im.measurements.label(im.binary_opening(mean_solid_systems > 0.2, iterations=int(w / 137.25)))

    systems = []
    for i in range(1, count + 1):
        # Using our labels we can mask out the area of interest on the vertical slice
        mask = (labels == i)
        current = mask * mean_solid_systems

        # Find top and bottom of system (wherever the value is first non-zero)
        uly = np.min(np.where(current > 0))
        lry = np.max(np.where(current > 0))

        # Find left and right border of system as the largest active region
        snippet = np.mean(img_solid[uly:lry, :], axis=0)
        regions = contiguous_regions(snippet > 0.4)
        ulx, lrx = regions[np.argmax(np.diff(regions).flatten())]

        system = System(
            ulx=ulx,
            uly=uly,
            lrx=lrx,
            lry=lry,
            v_profile=np.mean(img[uly:lry, ulx:lrx], axis=1),
            h_profile=np.mean(img[uly:lry, ulx:lrx], axis=0),
            measures=[]
        )
        systems.append(system)

    return systems


def find_measures_in_system(img, system):
    mean, std = np.mean(system.h_profile), np.std(system.h_profile)
    h, w = np.shape(img)
    # This definition assumes a maximum amount of 40 measures per system, spread out with equal distance.
    min_block_width = int(w / 40)
    min_heigth = mean + 2*std
    peaks = sig.find_peaks(system.h_profile, distance=min_block_width, height=min_heigth, prominence=0.2)[0]
    measure_split_candidates = sorted(peaks)

    # Filter out outliers by means of modified z-scores
    zscores = modified_zscore(system.h_profile[measure_split_candidates])
    # Use only candidate peaks if their modified z-score is below a given threshold or if their height is at least 3 standard deviations over the mean
    measure_splits = np.asarray(measure_split_candidates)[(np.abs(zscores) < 50.0) | (system.h_profile[measure_split_candidates] > 3*std)]

    measures = []
    for i in range(len(measure_splits) - 1):
        measures.append(Measure(
            ulx=system.ulx + measure_splits[i],
            uly=system.uly,
            lrx=system.ulx + measure_splits[i + 1],
            lry=system.lry,
            staffs=[]
        ))
    return measures


def find_staff_split_intersect(peak, midpoint, profile):
    # The split is made at a point of low mass (so as few intersections with mass as possible).
    # A small margin is allowed, to find a balance between cutting in the middle and cutting through less mass.
    region_min = np.min(profile)
    boundary_candidates = np.where(profile <= region_min * 1.25)[0]
    # Use index closest to the original midpoint, to bias towards the center between two bars
    staff_split = boundary_candidates[(np.abs(boundary_candidates - (midpoint - peak))).argmin()]
    return peak + staff_split


def find_staff_split_region(peak, midpoint, profile):
    # Find the longest region with intensities below a certain threshold
    region_splits = contiguous_regions(profile < 0.1)
    # Fallback to 'intersect' method when no region is found
    if region_splits.shape[0] == 0:
        return find_staff_split_intersect(peak, midpoint, profile)
    region_idx = np.argmax(np.diff(region_splits).flatten())
    # Split the measures at the middle of the retrieved region
    staff_split = int(np.mean(region_splits[region_idx]))
    return peak + staff_split


def add_staffs_to_system(img, system, measures, method='region'):
    h, w = np.shape(img)
    min_measure_dist = int(h / 30)

    # First we find peaks over the entire system to find the middle between each two consecutive bars
    peaks = sig.find_peaks(system.v_profile, distance=min_measure_dist, height=0.2, prominence=0.2)[0]  # Find peaks in vertical profile, which indicate the bar lines.
    midpoints = np.asarray(peaks[:-1] + np.round(np.diff(peaks) / 2), dtype='int')  # Get the midpoints between the detected bar lines, which will be used as the starting point for getting the Measures.

    populated_measures = []
    for j, measure in enumerate(measures):
        # Slice out the profile for this measure only
        measure_profile = np.mean(img[measure.uly:measure.lry, measure.ulx:measure.lrx], axis=1)
        # The measure splits are relative to the current measure, start with 0 to include the top
        staff_splits = [0]
        for i in range(len(peaks) - 1):
            # Slice out the profile between two peaks (the part in between bars)
            region_profile = measure_profile[peaks[i]:peaks[i + 1]]
            if method == 'intersect':
                staff_splits.append(find_staff_split_intersect(peaks[i], midpoints[i], region_profile))
            elif method == 'region':
                staff_splits.append(find_staff_split_region(peaks[i], midpoints[i], region_profile))
        staff_splits.append(system.lry - system.uly)

        staffs = []
        for i in range(len(staff_splits) - 1):
            staffs.append(Staff(
                ulx=measure.ulx,
                uly=measure.uly + staff_splits[i],
                lrx=measure.lrx,
                lry=measure.uly + staff_splits[i + 1],
            ))
        populated_measure = measure._replace(staffs=staffs)
        populated_measures.append(populated_measure)
    return populated_measures


def detect_measures(path):
    img = open_and_preprocess(path)
    height, width = img.shape
    systems = find_systems_in_page(img)
    populated_systems = []
    for system in systems:
        measures = find_measures_in_system(img, system)
        measures = add_staffs_to_system(img, system, measures, method='region')
        system = system._replace(measures=measures)
        populated_systems.append(system)
    page = Page(height=height, width=width, systems=populated_systems)
    return page
