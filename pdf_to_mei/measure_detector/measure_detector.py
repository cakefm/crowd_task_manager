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
System = namedtuple("System", ["ulx", "uly", "lrx", "lry", "v_profile", "h_profile", "staff_boundaries", "measures"])
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


def find_system_staff_boundaries(v_profile, plot=False):
    peaks = sig.find_peaks(v_profile, height=0.5, prominence=0.15)[0]  # Find peaks in vertical profile, which indicate the bar lines.
    gaps = np.diff(peaks)

    median_gap = np.median(gaps)
    staff_split_indices = np.where(gaps > 2*median_gap)[0]
    staff_split_indices = np.append(staff_split_indices, gaps.shape[0])
    staff_boundaries = []
    cur_start = 0
    for staff_split_idx in staff_split_indices:
        staff_boundaries.append([peaks[cur_start], peaks[staff_split_idx]])
        cur_start = staff_split_idx + 1

    if plot:
        plt.figure()
        plt.plot(v_profile)
        for peak in peaks:
            plt.plot(peak, v_profile[peak], 'x', color='red')
        for bound in staff_boundaries:
            plt.axvline(bound[0], color='green')
            plt.axvline(bound[1], color='green')
        plt.show()

    return staff_boundaries


def find_systems_in_page(img, plot=False):
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
        # Add 1 percent margin on both sides to improve peak detection at the edges of the system h_profile
        ulx = int(ulx - (lrx - ulx) * 0.01)
        lrx = int(lrx + (lrx - ulx) * 0.01)

        # Find staff boundaries for system
        staff_boundaries = find_system_staff_boundaries(np.mean(img[uly:lry, ulx:lrx], axis=1))
        largest_gap = np.max(np.diff(np.asarray(staff_boundaries).flatten()))
        # Add margin of staff gap to both sides to improve peak detection at the edges of the system v_profile
        uly = uly - largest_gap
        lry = lry + largest_gap
        staff_boundaries = find_system_staff_boundaries(np.mean(img[uly:lry, ulx:lrx], axis=1), plot)

        system = System(
            ulx=ulx,
            uly=uly,
            lrx=lrx,
            lry=lry,
            v_profile=np.mean(img[uly:lry, ulx:lrx], axis=1),
            h_profile=np.mean(img[uly:lry, ulx:lrx], axis=0),
            staff_boundaries=staff_boundaries,
            measures=[]
        )
        systems.append(system)

    return systems


def find_measures_in_system(img, system, plot=False):
    h, w = np.shape(img)

    all_indices = np.arange(img.shape[0])
    slices = [slice(system.uly + staff[0], system.uly + staff[1]) for staff in system.staff_boundaries]
    remove_indices = np.hstack([all_indices[i] for i in slices])
    img_without_staffs = np.copy(img)
    img_without_staffs[remove_indices, :] = 0
    h_profile_without_staffs = np.mean(img_without_staffs[system.uly:system.lry, system.ulx:system.lrx], axis=0)
    mean, std = np.mean(h_profile_without_staffs), np.std(h_profile_without_staffs)

    # Take a relatively small min_width to also find measure lines in measures (for e.g. a pickup or anacrusis)
    min_block_width = int(w / 50)
    min_height = mean + 2*std
    peaks = sig.find_peaks(h_profile_without_staffs, distance=min_block_width, height=min_height, prominence=0.18)[0]
    measure_split_candidates = sorted(peaks)

    # Filter out outliers by means of modified z-scores
    zscores = modified_zscore(h_profile_without_staffs[measure_split_candidates])
    # Use only candidate peaks if their modified z-score is below a given threshold or if their height is at least 3 standard deviations over the mean
    measure_splits = np.asarray(measure_split_candidates)[(np.abs(zscores) < 15.0) | (h_profile_without_staffs[measure_split_candidates] > mean + 3*std)]
    if measure_splits.shape[0] > 0 and measure_splits[-1] < (h_profile_without_staffs.shape[0] - 2*min_block_width):
        measure_splits = np.append(measure_splits, h_profile_without_staffs.shape[0])

    if plot:
        plt.figure()
        plt.plot(h_profile_without_staffs, color='green')
        plt.axhline(mean + 2*std)
        for split in peaks:
            plt.plot(split, h_profile_without_staffs[split], 'x', color='red')
        plt.show()

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


def find_staff_split_intersect(profile, plot=False):
    # The split is made at a point of low mass (so as few intersections with mass as possible).
    # A small margin is allowed, to find a balance between cutting in the middle and cutting through less mass.
    region_min = np.min(profile)
    midpoint = int(profile.shape[0] / 2)
    boundary_candidates = np.where(profile <= region_min * 1.25)[0]
    # Use index closest to the original midpoint, to bias towards the center between two bars
    staff_split = boundary_candidates[(np.abs(boundary_candidates - midpoint)).argmin()]

    if plot:
        plt.figure()
        plt.plot(profile)
        plt.axvline(staff_split, color='red')
        plt.show()

    return staff_split


def find_staff_split_region(profile, plot=False):
    # Find the longest region with intensities below a certain threshold
    region_min = np.min(profile)
    region_splits = contiguous_regions(profile <= region_min * 1.25)
    # Fallback to 'intersect' method when no region is found
    if region_splits.shape[0] == 0:
        return find_staff_split_intersect(profile)
    region_idx = np.argmax(np.diff(region_splits).flatten())
    # Split the measures at the middle of the retrieved region
    staff_split = int(np.mean(region_splits[region_idx]))

    if plot:
        plt.figure()
        plt.plot(profile)
        plt.axhline(region_min * 1.25)
        plt.axvline(staff_split, color='red')
        plt.show()

    return staff_split


def add_staffs_to_system(img, system, measures, method='region', plot=False):
    populated_measures = []
    for j, measure in enumerate(measures):
        # Slice out the profile for this measure only
        measure_profile = np.mean(img[measure.uly:measure.lry, measure.ulx:measure.lrx], axis=1)
        # The measure splits are relative to the current measure, start with 0 to include the top
        staff_splits = [0]
        for i in range(len(system.staff_boundaries) - 1):
            # Slice out the profile between two peaks (the part in between bars)
            region_profile = measure_profile[system.staff_boundaries[i][1]:system.staff_boundaries[i + 1][0]]
            if method == 'intersect':
                staff_split = find_staff_split_intersect(region_profile, plot)
            elif method == 'region':
                staff_split = find_staff_split_region(region_profile, plot)
            else:
                staff_split = int(region_profile.shape[0] / 2)
            staff_splits.append(staff_split + system.staff_boundaries[i][1])
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


def detect_measures(path, plot=False):
    img = open_and_preprocess(path)
    height, width = img.shape
    systems = find_systems_in_page(img, plot)
    populated_systems = []
    for system in systems:
        measures = find_measures_in_system(img, system, plot)
        measures = add_staffs_to_system(img, system, measures, method='region', plot=plot)
        system = system._replace(measures=measures)
        populated_systems.append(system)
    page = Page(height=height, width=width, systems=populated_systems)
    return page
