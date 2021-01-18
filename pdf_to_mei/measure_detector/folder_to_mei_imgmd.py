import datetime
import sys
sys.path.append("..")
import common.file_system_manager as fsm
import measure_detector.measure_detector as detector
import os
from uuid import uuid4
from lxml import etree
from tqdm import tqdm
import numpy as np

def run(sheet_name, connection):

    version = '1.0.0'

    template = f'''<?xml version="1.0" encoding="UTF-8"?>
    <mei xmlns="http://www.music-encoding.org/ns/mei">
        <meiHead>
            <fileDesc>
                <titleStmt>
                    <title/>
                </titleStmt>
                <pubStmt/>
            </fileDesc>
             <encodingDesc>
                <appInfo>
                    <application isodate="{datetime.datetime.now().replace(microsecond=0).isoformat()}" version="{version}">
                        <name>MeasureDetector</name>
                        <p>Measures detected with MeasureDetector</p>
                    </application>
                </appInfo>
            </encodingDesc>
        </meiHead>
        <music>
            <facsimile>
            </facsimile>
            <body>
            </body>
        </music>
    </mei>'''.encode()


    # Detect measures
    page_path = fsm.get_sheet_pages_directory(sheet_name)
    image_paths = sorted([str(p.resolve()) for p in page_path.iterdir() if p.is_file()], key = lambda x : int(os.path.basename(x).split('_')[1].split('.')[0]))

    results = []

    tqdm.write(f'Detecting measures in {len(image_paths)} images...')
    for image_path in tqdm(image_paths, unit='img'):
        page = detector.detect_measures(image_path)
        results.append({'path': image_path, 'page': page})
        connection.process_data_events()

    # Generate MEI file
    xml_parser = etree.XMLParser(remove_blank_text=True)
    mei = etree.fromstring(template, parser=xml_parser)

    mei_facsimile = mei.xpath('//*[local-name()="facsimile"]')[0]
    mei_body = mei.xpath('//*[local-name()="body"]')[0]

    mei_mdiv = etree.Element('mdiv')
    mei_mdiv.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'mdiv_' + str(uuid4())
    mei_mdiv.attrib['n'] = str(1)
    mei_mdiv.attrib['label'] = ''
    mei_body.append(mei_mdiv)

    mei_score = etree.Element('score')
    mei_score_def = etree.Element('scoreDef')
    mei_score.append(mei_score_def)
    mei_mdiv.append(mei_score)

    mei_section = etree.Element('section')
    mei_score.append(mei_section)

    mei_section.append(etree.Element('pb'))

    cur_measure, cur_staff = 1, 1

    staff_counts = []
    section_lengths = []
    measures_per_page = []
    for p, result in enumerate(results):
        print("Processing page", page)
        page, path = result['page'], result['path']

        mei_surface = etree.Element('surface')
        mei_surface.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'surface_' + str(uuid4())
        mei_surface.attrib['n'] = str(p + 1)
        mei_surface.attrib['ulx'] = str(0)
        mei_surface.attrib['uly'] = str(0)
        mei_surface.attrib['lrx'] = str(page.width - 1)
        mei_surface.attrib['lry'] = str(page.height - 1)
        mei_facsimile.append(mei_surface)

        mei_graphic = etree.Element('graphic')
        mei_graphic.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'graphic_' + str(uuid4())
        mei_graphic.attrib['target'] = os.path.basename(path)
        mei_graphic.attrib['width'] = str(page.width)
        mei_graphic.attrib['height'] = str(page.height)
        mei_surface.append(mei_graphic)

        for s, system in enumerate(page.systems):
            for m, measure in enumerate(system.measures):

                mei_measure = etree.Element('measure')
                mei_measure.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'measure_' + str(uuid4())
                mei_measure.attrib['n'] = str(cur_measure)
                mei_measure.attrib['label'] = str(cur_measure)
                mei_section.append(mei_measure)

                cur_staff = 1
                for st, staff in enumerate(measure.staffs):
                    mei_zone = etree.Element('zone')
                    mei_zone_id = 'zone_' + str(uuid4())
                    mei_zone.attrib['{http://www.w3.org/XML/1998/namespace}id'] = mei_zone_id
                    mei_zone.attrib['type'] = 'staff'
                    mei_zone.attrib['ulx'] = str(int(staff.ulx))
                    mei_zone.attrib['uly'] = str(int(staff.uly))
                    mei_zone.attrib['lrx'] = str(int(staff.lrx))
                    mei_zone.attrib['lry'] = str(int(staff.lry))
                    mei_surface.append(mei_zone)

                    mei_staff = etree.Element('staff')
                    mei_staff.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'staff_' + str(uuid4())
                    mei_staff.attrib['n'] = str(cur_staff)
                    mei_staff.attrib['label'] = str(cur_staff)
                    mei_staff.attrib['facs'] = f'#{mei_zone_id}'

                    # Staffs should have at least one layer, can optionally be enumerated with "n" if we need more
                    mei_layer = etree.Element('layer')
                    mei_staff.append(mei_layer)

                    mei_measure.append(mei_staff)

                    cur_staff += 1
                staff_counts.append(cur_staff - 1)
                cur_measure += 1
            mei_section.append(etree.Element('sb'))
            section_lengths.append(cur_measure - 1 - sum(section_lengths))
        mei_section.append(etree.Element('pb'))
        measures_per_page.append(cur_measure - 1 - sum(measures_per_page))

    # Add the most likely staff configuration to the scoredef
    # NOTE: does not generalize to scores with more than one staff configuration
    mei_staff_group = etree.Element('staffGrp')
    mei_score_def.append(mei_staff_group)
    for i in range(round(np.mean(staff_counts))):
        n = i + 1
        mei_staff_def = etree.Element('staffDef')
        mei_staff_def.attrib['n'] = str(n)
        mei_staff_def.attrib['lines'] = '5'  # Render looks weird without lines
        mei_staff_group.append(mei_staff_def)

    # Print some detection statistics
    print("Detection Statistics:")
    print(f"{'  mean staff count:':<20}{np.mean(np.mean(staff_counts))}")
    print(f"{'  mean line length:':<20}{np.mean(np.mean(section_lengths))}")
    print(f"{'  mean measures per page:':<20}{np.mean(np.mean(measures_per_page))}")
    print(f"{'  measures per page:'}")
    for i, count in enumerate(measures_per_page):
        page = i + 1
        print(f"{'  - ' + str(page):<20}{count}")


    mei_path = fsm.get_sheet_whole_directory(sheet_name)
    mei_file_dir = mei_path / "aligned.mei"
    with open(str(mei_file_dir), 'wb') as file:
        xml = etree.ElementTree(mei)
        xml.write(file, encoding='utf-8', pretty_print=True)

    tqdm.write('Done.')
