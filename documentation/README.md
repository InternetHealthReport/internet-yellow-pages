# IYP documentation

## Ontology

The list of node and relationship types defined for IYP are available at:

- [Node types](./node-types.md)
- [Relationship types](./relationship-types.md)

## Data sources

The list of all datasets imported in IYP is available [here](data-sources.md).
The dataset licenses are available the [acknowledgments](../ACKNOWLEDGMENTS.md).

## Gallery

The [IYP gallery](./gallery.md) provides example queries to help users browse the
database.

## Add new datasets

### Propose a new dataset

Have an idea for a dataset that should be integrated into IYP? Feel free to propose it
by opening a new
[discussion](https://github.com/InternetHealthReport/internet-yellow-pages/discussions).
You should describe the dataset, why it is potentially useful, and, if possible, provide
some initial idea for modeling the data.

The discussion is used to decide if we want to integrate the dataset and how to model
it. So feel free to propose a dataset even if you have no concrete model in mind.

### Import a new dataset

If it was decided that the dataset should be integrated into IYP, we will convert the
discussion into a [GitHub
issue](https://github.com/InternetHealthReport/internet-yellow-pages/issues). At this
stage it is open to anyone who wants to implement a crawler for the dataset.

For a detailed description on how to write your first crawler and contribute to IYP take
a look at the [IHR contributing guidelines](../CONTRIBUTING.md) and the [crawler
instructions](writing-a-crawler.md).
