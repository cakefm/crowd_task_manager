def run(sheet_name):
    import datetime
    import sys
    sys.path.append("..")
    import common.file_system_manager as fsm
    from common.settings import cfg

    from glob import glob
    import json
    import os
    from uuid import uuid4

    from lxml import etree
    from PIL import Image, ImageFont
    from PIL.ImageDraw import ImageDraw
    import requests
    from tqdm import tqdm

    version = '1.0.0'

    template = f'''<?xml version="1.0" encoding="UTF-8"?>
    <mei xmlns="http://www.music-encoding.org/ns/mei">
        <meiHead>
            <fileDesc>
                <titleStmt/>
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


    def draw_boxes(image_path, measures):
        image = Image.open(image_path).convert('RGBA')
        overlay = Image.new('RGBA', image.size)
        image_draw = ImageDraw(overlay)

        for measure in measures:
            image_draw.rectangle([int(measure['left']), int(measure['top']), int(measure['right']), int(measure['bottom'])],
                                 fill='#00FFFF1B')
        for m, measure in enumerate(measures):
            image_draw.rectangle([int(measure['left']), int(measure['top']), int(measure['right']), int(measure['bottom'])],
                                 outline='#008888', width=2)

        result_image = Image.alpha_composite(image, overlay).convert('RGB')

        target_dir = os.path.join(os.path.dirname(image_path), 'bboxes')
        os.makedirs(target_dir, exist_ok=True)

        basename = os.path.basename(image_path)
        result_path = os.path.join(target_dir, basename)
        result_image.save(result_path)

    # Detect measures
    page_path = fsm.get_sheet_pages_directory(sheet_name)
    image_paths = sorted([str(p.resolve()) for p in page_path.iterdir() if p.is_file()], key = lambda x : int(os.path.basename(x).split('_')[1].split('.')[0]))

    pages = []

    tqdm.write(f'Detecting measures in {len(image_paths)} images...')
    for image_path in tqdm(image_paths, unit='img'):
        with open(image_path, 'rb') as image:
            address = ":".join(map(str, cfg.measure_detector_address))
            response = requests.post(f'http://{address}/upload', files={'image': image})
        measures = json.loads(response.content.decode('utf-8'))['measures']
        pages.append({'path': image_path, 'measures': measures})

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
    mei_score.append(etree.Element('scoreDef'))
    mei_mdiv.append(mei_score)

    mei_section = etree.Element('section')
    mei_score.append(mei_section)

    mei_section.append(etree.Element('pb'))

    cur_ulx = 0
    cur_measure = 1

    for p, page in enumerate(pages):
        image = Image.open(page['path'])
        image_width, image_height = image.size
        image.close()

        measures = page['measures']
        print(measures)

        # TODO: restore this functionality in some other way?
        # if args.make_images:
        #     draw_boxes(page['path'], measures)

        mei_surface = etree.Element('surface')
        mei_surface.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'surface_' + str(uuid4())
        mei_surface.attrib['n'] = str(p + 1)
        mei_surface.attrib['ulx'] = str(0)
        mei_surface.attrib['uly'] = str(0)
        mei_surface.attrib['lrx'] = str(image_width - 1)
        mei_surface.attrib['lry'] = str(image_height - 1)
        mei_facsimile.append(mei_surface)

        mei_graphic = etree.Element('graphic')
        mei_graphic.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'graphic_' + str(uuid4())
        mei_graphic.attrib['target'] = os.path.basename(page['path'])
        mei_graphic.attrib['width'] = str(image_width)
        mei_graphic.attrib['height'] = str(image_height)
        mei_surface.append(mei_graphic)

        for m, measure in enumerate(measures):
            print(measure)
            mei_zone = etree.Element('zone')
            mei_zone_id = 'zone_' + str(uuid4())
            mei_zone.attrib['{http://www.w3.org/XML/1998/namespace}id'] = mei_zone_id
            mei_zone.attrib['type'] = 'measure'
            mei_zone.attrib['ulx'] = str(int(measure['ulx']))
            mei_zone.attrib['uly'] = str(int(measure['uly']))
            mei_zone.attrib['lrx'] = str(int(measure['lrx']))
            mei_zone.attrib['lry'] = str(int(measure['lry']))
            mei_surface.append(mei_zone)

            mei_measure = etree.Element('measure')
            mei_measure.attrib['{http://www.w3.org/XML/1998/namespace}id'] = 'measure_' + str(uuid4())
            mei_measure.attrib['n'] = str(cur_measure)
            mei_measure.attrib['label'] = str(cur_measure)
            mei_measure.attrib['facs'] = f'#{mei_zone_id}'
            mei_section.append(mei_measure)
            cur_measure += 1

            if len(measures) > m + 1 and measures[m + 1]['ulx'] < measure['ulx']:
                mei_section.append(etree.Element('sb'))
            elif len(measures) <= m + 1:
                mei_section.append(etree.Element('sb'))

        mei_section.append(etree.Element('pb'))

    mei_path = fsm.get_sheet_whole_directory(sheet_name)
    mei_file_dir = mei_path / "aligned.mei"
    with open(str(mei_file_dir), 'wb') as file:
        xml = etree.ElementTree(mei)
        xml.write(file, encoding='utf-8', pretty_print=True, xml_declaration=True)

    tqdm.write('Done.')