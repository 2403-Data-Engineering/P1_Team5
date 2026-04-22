# Fraud Detection Queries – PaySim (Neo4j)

---

## 1. Large Transfers Followed by Immediate Cash-Out

### Flag transactions and accounts

```cypher
MATCH (a:Account)-[t1:TRANSACTION {type: 'TRANSFER'}]->(b:Account)
      -[t2:TRANSACTION {type: 'CASH_OUT'}]->(c:Account)
WHERE t2.step >= t1.step
  AND t2.step - t1.step <= 2
  AND t1.amount > 0
  AND t2.amount / t1.amount >= 0.4
  AND t2.amount / t1.amount <= 1.2
SET b.transfer_cashout_flag = 1,
    t1.transfer_cashout_flag = 1,
    t2.transfer_cashout_flag = 1
RETURN count(*) AS flagged_pairs,
       count(DISTINCT b) AS flagged_accounts;
```

### Inspect cases

```cypher
MATCH (a:Account)-[t1:TRANSACTION {type: 'TRANSFER'}]->(b:Account)
      -[t2:TRANSACTION {type: 'CASH_OUT'}]->(c:Account)
WHERE t2.step >= t1.step
  AND t2.step - t1.step <= 2
RETURN b.id AS suspicious_account,
       t1.step AS transfer_step,
       t2.step AS cashout_step,
       t1.amount AS transfer_amount,
       t2.amount AS cashout_amount
LIMIT 25;
```

---

## 2. Dense Community with High Internal Volume

### Drop existing graph 

```cypher
CALL gds.graph.drop('acctGraphRing', false);
```

### Create reduced graph projection

```cypher
CALL gds.graph.project.cypher(
  'acctGraphRing',
  'MATCH (a:Account) WHERE a.id STARTS WITH "C" RETURN id(a) AS id',
  '
  MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
  WHERE a.id STARTS WITH "C"
    AND b.id STARTS WITH "C"
    AND t.type IN ["TRANSFER", "CASH_OUT"]
    AND t.amount >= 1000
  RETURN id(a) AS source, id(b) AS target, t.amount AS amount
  '
);
```

### Run Louvain community detection

```cypher
CALL gds.louvain.write('acctGraphRing', {
  writeProperty: 'community_id'
});
```

### Evaluate communities

```cypher
MATCH (a:Account)
WHERE a.community_id IS NOT NULL
WITH a.community_id AS cid, count(a) AS size
WHERE size >= 3 AND size <= 15

MATCH (x:Account)-[t:TRANSACTION]->(y:Account)
WHERE x.community_id = cid

WITH cid, size,
     sum(CASE WHEN x.community_id = y.community_id THEN t.amount ELSE 0 END) AS internal_volume,
     sum(CASE WHEN x.community_id <> y.community_id THEN t.amount ELSE 0 END) AS outgoing_volume

WITH cid, size, internal_volume, outgoing_volume,
     CASE
       WHEN outgoing_volume = 0 THEN internal_volume
       ELSE internal_volume * 1.0 / outgoing_volume
     END AS ratio

RETURN cid, size, internal_volume, outgoing_volume, ratio
ORDER BY ratio DESC, internal_volume DESC
LIMIT 25;
```

### Flag communities

```cypher
MATCH (a:Account)
WHERE a.community_id IS NOT NULL
WITH a.community_id AS cid, collect(a) AS members, count(a) AS size
WHERE size >= 3 AND size <= 15

MATCH (x:Account)-[t:TRANSACTION]->(y:Account)
WHERE x.community_id = cid

WITH cid, members,
     sum(CASE WHEN x.community_id = y.community_id THEN t.amount ELSE 0 END) AS internal_volume,
     sum(CASE WHEN x.community_id <> y.community_id THEN t.amount ELSE 0 END) AS outgoing_volume

WITH cid, members,
     CASE
       WHEN outgoing_volume = 0 THEN internal_volume
       ELSE internal_volume * 1.0 / outgoing_volume
     END AS ratio

WHERE ratio > 3

UNWIND members AS m
SET m.in_suspicious_ring_flag = 1;
```

---

## 3. Cycles (Circular Money Flow)

### Detect cycles of length 3–5

```cypher
MATCH path = (a:Account)-[:TRANSACTION*3..5]->(a)
RETURN path
LIMIT 25;
```

### Count cycles

```cypher
MATCH path = (a:Account)-[:TRANSACTION*3..5]->(a)
RETURN count(path) AS total_cycles;
```

### Flag accounts in cycles

```cypher
MATCH path = (a:Account)-[:TRANSACTION*3..5]->(a)
UNWIND nodes(path) AS n
SET n.in_cycle_flag = 1;
```

---
