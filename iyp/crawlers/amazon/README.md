# AWS IP Address Ranges -- https://docs.aws.amazon.com/vpc/latest/userguide/aws-ip-ranges.html

Amazon Web Services (AWS) publishes its current IP address ranges in JSON format. This
information is useful for identifying traffic from AWS resources and understanding which
AWS services are associated with different IP prefixes.

The IYP crawler fetches this data and integrates it into the graph. Country information
is derived by scraping the AWS regions documentation to map AWS region codes (e.g.,
`us-east-1`) to ISO country codes (e.g., `US`).

## Graph representation

```cypher
(:GeoPrefix {prefix: '3.5.140.0/22'})-[:COUNTRY]->(:Country {country_code: 'JP'})
(:GeoPrefix {prefix: '3.5.140.0/22'})-[:CATEGORIZED {region: 'ap-northeast-2'}]->(:Tag {label: 'AMAZON'})
```

- **GeoPrefix**: The IPv4 or IPv6 CIDR block with geolocation context (also has Prefix label).
- **Country**: The geographical location derived from the AWS region.
- **Tag**: The AWS service associated with the prefix (e.g., "AMAZON", "EC2", "S3", "CLOUDFRONT").
- **region**: The AWS region code. A few regions cannot be mapped to a country, either
  because they do not belong to a country (e.g., "GLOBAL") or because no country
  information is available yet (e.g., "sa-west-1").

## Dependence

This crawler is not depending on other crawlers.
