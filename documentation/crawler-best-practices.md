# Best practices

This is a summary of best practices that should be followed when writing a crawler from
scratch. Only some of these are mandatory but in general you should try to follow all of
them.

## Checklists

**Always required:**

- [ ] Add/update README
- [ ] Update example config
- [ ] Check code style with pre-commit
- [ ] **Test your crawler by actually running it**

If you add a new data source:

- [ ] Update [ACKNOWLEDGMENTS.md](../ACKNOWLEDGMENTS.md)
- [ ] Update [data-sources.md](data-sources.md)

If you update the ontology (i.e., add a new node and/or relationship type):

- [ ] Update [node-types.md](node-types.md) or
  [relationship-types.md](relationship-types.md) as appropriate

If you add a new Python dependency:

- [ ] Update [requirements.txt](../requirements.txt)

## Code considerations

### Correctness

- Avoid creating duplicate nodes/relationships by using sets. Multiple relationships
  between two nodes with the same relationship types are acceptable if the relationship
  properties differ.
- Check that you do not create empty nodes.
- Be aware of [property
  formatters](https://github.com/InternetHealthReport/internet-yellow-pages/blob/main/iyp/__init__.py#L17)
  that might change your ID property and do the
  formatting yourselves beforehand (example IPv6, but the crawler will crash anyways if
  you fail to do this).
- Specify `reference_url_data` as precise as possible, especially if it changes for
  parts of the data within the same crawler. Also try to use URLs that point to the
  correct data even when accessed at a later point in time. Note: `URL` is used as the
  default value for `reference_url_data`. Always specify a `URL`, even if it might not
  be precise and is updated in the code. It makes it easier to know where this crawler
  gets its data from just by looking at the header.
- Try to specify a `reference_url_info` that gives an explanation / reference to the
  data.
- Try to give a precise `reference_time_modification`, but do *not* add this if you are
  unsure. For this field it is better to give no info than wrong info.
- `NAME` should always be `directory.file`.
- In general, do not manipulate the data, e.g., by removing entries or renaming
  properties. IYP is a tool that combines different data sources, and detecting
  differences in the data sources is part of that. If you are unsure, feel free to ask
  since this is not always a clear line. For example, a crawler that adds IP prefixes
  from BGP should not filter out private IP prefixes, even though they do not belong in
  BGP, but should remove prefixes with an invalid format.
- In general, do not add data to nodes apart from the ID properties. Data source
  specific information can (and should) be attached to the created relationships. Nodes
  are accessed by different crawlers and thus should only contain information that all
  crawlers share.
  
### Code style

- Do not use `sys.exit`, but raise an exception to kill a crawler. Also print to the log
  before raising the exception.
- Do not log to stdout/stderr, but use `logging.{info,warning,error}` as appropriate.
  You can log some steps to `info`, but do not be too verbose. `warning` should be for
  unexpected cases which are not critical enough to justify killing the crawler. `error`
  should be followed by an exception. Batch functions automatically log
  node/relationship creations, so you do not have to do this manually.
- Do not change the interface of the default crawler. create_db always uses the default
  `Crawler(ORG, URL, NAME)` call. The `main` function in the crawler file is only for
  testing or individual runs of the crawler and should not be modified.

### Performance

- Think about if you need to specify `all=False` when creating/fetching nodes. Usually
  you will not need to fetch all nodes of a type.
- If possible, iterate over the data only once, gathering nodes and relationships in the
  process. Then iterate over the relationships and replace node values by their IDs.
  This way you do not have to perform the formatting twice (and are probably faster).
- Use batch functions by default except when you are *very* sure you will only create a
  few nodes/relationships.
- Cache data where appropriate, and use the `tmp` directory (advanced usage; not
  required for most crawlers).
