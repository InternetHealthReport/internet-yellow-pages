# AWS IP Address Ranges -- https://docs.aws.amazon.com/vpc/latest/userguide/aws-ip-ranges.html

Amazon Web Services (AWS) publishes its current IP address ranges in JSON format. This information is useful for identifying traffic from AWS resources.

The IYP crawler fetches this data and models it in the graph.

## Graph representation

```cypher
(:Prefix {prefix:'3.5.140.0/22'})-[:MANAGED_BY]->(:Organization {name:'Amazon Web Services'})
(:Prefix {prefix:'3.5.140.0/22'})-[:CATEGORIZED]->(:Tag {label:'AMAZON'})
(:Prefix {prefix:'3.5.140.0/22'})-[:LOCATED_IN]->(:Country {country_code:'JP'})
```

- **Prefix**: The IPv4 or IPv6 CIDR block.
- **Organization**: Linked to "Amazon Web Services".
- **Tag**: The service associated with the prefix (e.g., "AMAZON", "EC2", "S3").
- **Country**: The geographical location derived from the AWS Region (e.g., `ap-northeast-2` -> `JP`).

## Dependence

This crawler is not depending on other crawlers.
