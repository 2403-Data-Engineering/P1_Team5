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


# Node Similarity Among Flagged Accounts

---

## Initialize Flag

Set `similar_to_flagged_flag` to 0 if it does not exist

```cypher
MATCH (a:Account)
WITH a ORDER BY id(a)
CALL (a) {
  WITH a
  SET a.similar_to_flagged_flag = coalesce(a.similar_to_flagged_flag, 0)
} IN TRANSACTIONS OF 5000 ROWS;
```

---

## Flagged Accounts Exist

```cypher
MATCH (a:Account)
RETURN sum(a.flagged) AS flagged_accounts;
```



## Create Reduced Graph Projection

```cypher
CALL gds.graph.project.cypher(
  'similarityTiny',
  '
  MATCH (a:Account)
  WHERE a.id STARTS WITH "C"
  RETURN id(a) AS id
  ',
  '
  MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
  WHERE a.id STARTS WITH "C"
    AND b.id STARTS WITH "C"
    AND t.type = "TRANSFER"
    AND t.amount >= 500000
    AND t.step <= 10
  RETURN id(a) AS source, id(b) AS target
  '
);
```

---



## Estimate Memory for Filtered Node Similarity

```cypher
MATCH (a:Account)
WHERE a.flagged = 1
WITH collect(id(a)) AS flaggedIds

CALL gds.nodeSimilarity.filtered.write.estimate(
  'similarityTiny',
  {
    sourceNodeFilter: flaggedIds,
    targetNodeFilter: 'Account',
    similarityCutoff: 0.8,
    degreeCutoff: 2,
    topK: 5,
    writeRelationshipType: 'SIMILAR_TO_TINY',
    writeProperty: 'similarity'
  }
)
YIELD requiredMemory, treeView
RETURN requiredMemory, treeView;
```

---

## Run Filtered Node Similarity

```cypher
MATCH (a:Account)
WHERE a.flagged = 1
WITH collect(id(a)) AS flaggedIds

CALL gds.nodeSimilarity.filtered.write(
  'similarityTiny',
  {
    sourceNodeFilter: flaggedIds,
    targetNodeFilter: 'Account',
    similarityCutoff: 0.8,
    degreeCutoff: 2,
    topK: 5,
    writeRelationshipType: 'SIMILAR_TO_TINY',
    writeProperty: 'similarity'
  }
)
YIELD relationshipsWritten, nodesCompared
RETURN relationshipsWritten, nodesCompared;
```

---

## Flag Similar Accounts

```cypher
MATCH (a:Account)-[s:SIMILAR_TO_TINY]->(b:Account)
WHERE a.flagged = 1
  AND b.flagged = 0
  AND s.similarity >= 0.8
SET b.similar_to_flagged_flag = 1
RETURN count(DISTINCT b) AS similar_flagged_accounts;
```

---

## Node Similarity (Visualization)

```cypher
MATCH (a:Account)-[s:SIMILAR_TO_TINY]->(b:Account)
WHERE a.flagged = 1
  AND b.flagged = 0
  AND s.similarity >= 0.8
RETURN a, s, b
LIMIT 25;
```

---

## Count Final Results

```cypher
MATCH (a:Account)
RETURN sum(a.similar_to_flagged_flag) AS similar_to_flagged_accounts;
```

---

---
