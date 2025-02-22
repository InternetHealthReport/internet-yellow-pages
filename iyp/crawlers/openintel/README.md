# **OpenINTEL** – [https://www.openintel.nl/](https://www.openintel.nl/)

## **Introduction**  
OpenINTEL is a measurement platform that **collects daily snapshots** of the Domain Name System (DNS) across the internet. This helps researchers and developers understand changes in the DNS ecosystem over time.  

Currently, OpenINTEL **fetches data only for two major domain ranking lists**:  
- [**Tranco Top 1 Million**](https://tranco-list.eu/)  
- [**CISCO Umbrella Top 1 Million**](https://umbrella.cisco.com/)  

This ensures that only **popular domains** are analyzed to maintain efficiency. Additionally, OpenINTEL also retrieves **authoritative name servers** for these domains.

> ⚠️ **Note:** A separate mail server crawler **exists** but is **disabled** because it generates an **extremely large** number of links, and no one has requested it so far.

---

## **Graph Representation (How Data is Stored)**  
OpenINTEL represents its data in a **graph format**, making it easy to visualize connections between domain names, IP addresses, and services.

### **1️⃣ IP Resolution for Popular Host Names**  
How domain names resolve to IP addresses:

```cypher
(:HostName {name: 'google.com'})-[:RESOLVES_TO]->(:IP {ip: '142.250.179.142'})
```
This means that `google.com` is mapped to the IP address `142.250.179.142`.

---

### **2️⃣ IP Resolution of Authoritative Name Servers**  
Mapping of name servers to IP addresses:

```cypher
(:HostName:AuthoritativeNameServer {name: 'ns1.google.com'})-[:RESOLVES_TO]->(:IP {ip: '216.239.32.10'})
(:IP {ip: '216.239.32.10'})-[:SERVE]->(:Service {name: 'DNS'})
```
Here, `ns1.google.com` resolves to `216.239.32.10`, which provides a DNS service.

---

### **3️⃣ Domain Names Managed by Name Servers**  
Shows which name servers manage which domains:

```cypher
(:DomainName {name: 'google.com'})-[:MANAGED_BY]->(:HostName:AuthoritativeNameServer {name: 'ns1.google.com'})
```
This indicates that `google.com` is managed by `ns1.google.com`.

---

## **Dependencies**  
✅ **This crawler does NOT depend on any other crawlers.**  

This means it can run independently without requiring other systems.
