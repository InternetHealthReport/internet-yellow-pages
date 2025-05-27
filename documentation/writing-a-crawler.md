# How to write your first crawler

To import a new dataset into IYP, you should write a crawler for that dataset. The main
tasks of a crawler are to fetch data, parse it, model it with IYP ontology, and push it
to the IYP database. Most of these tasks are assisted by the [IYP python
library](../iyp/__init__.py). See the [example
crawler](../iyp/crawlers/example/crawler.py) or [existing crawlers](../iyp/crawlers/)
for getting started.
See also the [IHR contributing guidelines](../CONTRIBUTING.md) and [best practices for
writing crawlers](crawler-best-practices.md).

## README

Each crawler should be accompanied by a README.md file. This is the main documentation
for the crawler, it should contain:

- a short description of the dataset,
- any specificities related to the way the data is imported (e.g., time span, data cleaning),
- examples of how the data is modeled,
- dependencies to other crawlers (e.g., if the crawler requires data from another one).

## Adding a crawler to IYP main branch

If you wish your crawler to be part of the IYP weekly dumps, you can submit a [Pull
Request](https://github.com/InternetHealthReport/internet-yellow-pages/pulls) to include
the crawler to IYP's GitHub repository main branch.

Along with the Python code and README, the addition of new datasets should also be
reflected in the following files:

- the list of [imported datasets](./data-sources.md),
- the [IYP acknowledgments](../ACKNOWLEDGMENTS.md) file should list the license of all imported dataset.

Changes to the ontology should be discussed in advance, either on [GitHub
discussion](https://github.com/InternetHealthReport/internet-yellow-pages/discussions)
or by reaching out to [IYP maintainers](mailto:iyp@ihr.live), so that a consensus is reached
before the ontology is updated.
**Any change to the ontology should be reflected in the documentation:** ([Node
types](./node-types.md) and [Relationship types](./relationship-types.md)).

You can also consider adding example queries to the [IYP gallery](./gallery.md), and
organizations providing data to the [IYP frontpage](https://iyp.iijlab.net/).
