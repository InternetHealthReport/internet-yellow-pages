import importlib
import json

with open('config.json', 'r') as fp:
    conf = json.load(fp)


# Start a new neo4j container

# run each crawler
status = {}
no_error = True
for module_name in conf['iyp']['crawlers']:
    module = importlib.import_module(module_name)

    try:
        print(module_name)
        print(module.ORG, module.URL, '\n')
        #crawler = module.Crawler(module.ORG, module.URL)
        #crawler.run()
        #crawler.close()
        status[module_name] = "OK"

    except Exception as e:
        no_error = False
        status[module_name] = str(e)



# Stop the database

# dump the database

if not no_error:
    # TODO send an email
    pass
