# -*- coding: utf-8 -*-
""" Docker composer file to generate several 
    Geoprice containers working. Number of consumer 
    and web service workers is defined in a JSON like 
    argument pased to the composer. 
    It will output a `compose/geoprice.yaml` file to be launched
    with docker-compose up.

    Execution:
    python compose/composer.py '{"consumer": 2, "service": 2}' 
"""
import sys
import json


def validate():
    if len(sys.argv) < 2:
        raise Exception("Missing Deployment parameters")


def get_deploy_args():
    # Validate params
    validate()
    try: 
        _dep = json.loads(sys.argv[1])
    except:
        _dep = {"consumer": 0, "service": 0}
    if "consumer" not in _dep:
        _dep['consumer'] = 0
    if "service" not in _dep:
        _dep['consumer'] = 0
    return _dep


def create_consumer_yaml(c_num):
    with open('compose/consumer.yaml', 'r') as cyf:
        cym_template = cyf.read()
    # Add tabs
    cym_template = "\n".join(['  '+l if j > 0 else l \
            for j, l in enumerate(cym_template.split("\n"))])
    c_yml = ""
    for i in range(c_num):
        c_yml += cym_template.format(node_num=i)
        c_yml +='\n  '
    return c_yml


def create_service_yaml(c_num):
    with open('compose/service.yaml', 'r') as cyf:
        cym_template = cyf.read()
    # Add tabs
    cym_template = "\n".join(['  '+l if j > 0 else l \
            for j, l in enumerate(cym_template.split("\n"))])
    c_yml = ""
    for i in range(c_num):
        c_yml += cym_template.format(node_num=i)
        c_yml +='\n  '
    return c_yml


def write_geoprice_yaml(consy, servy):
    yaml = 'version: "3"' + '\n\n'+ 'services:\n  '
    yaml += consy
    yaml += servy
    with open('compose/geoprice.yaml', 'w') as gyaml:
        gyaml.write(yaml)


if __name__ == '__main__':
    # Fetch arguments json
    _args = get_deploy_args()
    print("Creating deployment files for {}".format(_args))
    # Create Files
    consumer = create_consumer_yaml(_args['consumer'])
    service = create_service_yaml(_args['service'])
    write_geoprice_yaml(consumer, service)

