# Cypher Queries

## Set flag to 0 if doesnt exist
Set one at a time if too intensive
```
MATCH (a:Account)

WITH a ORDER BY id(a)
CALL(a) {
WITH a
SET a.fan_in_flag = coalesce(a.fan_in_flag, 0),
    a.fan_out_flag = coalesce(a.fan_out_flag, 0)
} IN TRANSACTIONS OF 5000 ROWS;
```
## Fan-out

### Set fan-out flags
```
CALL (){
  MATCH (receiver)<-[t:TRANSACTION]-(sender)
  WITH t.step AS step, sender, count(DISTINCT receiver) AS destinations
  RETURN avg(destinations) AS mean, stdev(destinations) AS stdev
}
MATCH (receiver)<-[t:TRANSACTION]-(sender)
WITH t.step as step,sender,count(DISTINCT receiver) AS destinations,mean,stdev
WHERE destinations > mean + 3 * stdev
SET sender.fan_out_flag=1
```

### Fan-out w/ arrows to visualize
```
MATCH (receiver)<-[t:TRANSACTION]-(sender)
WITH t.step as step,sender,count(DISTINCT receiver) AS destinations, collect({rec:receiver,tx:t}) as transactions
WHERE destinations > 1.000007386948633 + 3 * 0.0027178843313898243
RETURN sender,transactions
```
---

## Fan-In

### Set fan-in flags
```
CALL (){
  MATCH (sender)-[t:TRANSACTION]->(receiver)
  WITH t.step AS step, receiver, count(DISTINCT sender) AS sources
  RETURN avg(sources) AS mean, stdev(sources) AS stdev
}

MATCH (sender)-[t:TRANSACTION]->(receiver)
WITH t.step AS step,receiver, count(DISTINCT sender) AS sources, mean, stdev
WHERE sources > mean + 3 * stdev
SET receiver.fan_in_flag=1
```

### Fan-in w/ arrows to visualize
```
CALL (){
  MATCH (sender)-[t:TRANSACTION]->(receiver)
  WITH t.step AS step, receiver, count(DISTINCT sender) AS sources
  RETURN avg(sources) AS mean, stdev(sources) AS stdev
}

MATCH (sender)-[t:TRANSACTION]->(receiver)
WITH t.step AS step,receiver, count(DISTINCT sender) AS sources, collect({src:sender, tx:t}) AS transactions, mean, stdev
WHERE sources > mean + 3 * stdev
RETURN receiver, transactions
```

## Drain-Behavior

### Set drain behavior flags
```
MATCH (a)-[t1:TRANSACTION]->(b), (b)-[t2:TRANSACTION]->(c) WHERE t2.step - t1.step <= 10 AND t2.newbalanceOrig < (t1.amount * 0.1) 
SET b.drain_flag=1
```

### Drain behavior w/ arrows to visualize
```
MATCH (a)-[t1:TRANSACTION]->(b), (b)-[t2:TRANSACTION]->(c) 
WHERE t2.step - t1.step <= 10 AND t2.newbalanceOrig < (t1.amount * 0.1) 
RETURN a,t1,b,t2,c
```

## Guilt By Association

### TODO add all other flags
### Run before guilt by association
```
MATCH (n:Account)
CALL (n) {
    SET n.flagged = CASE 
        WHEN 
            n.fan_out_flag              = 1 OR
            n.fan_in_flag               = 1 OR
            n.drain_flag                = 1 
        THEN 1 ELSE 0 END
} IN TRANSACTIONS OF 10000 ROWS
```

### Set guilt by association flags
```Cypher
MATCH (a:Account {flagged: 0})-[t:TRANSACTION]-(b:Account {flagged: 1}) 
WITH a, count(DISTINCT b) AS bad_neighbors
WHERE bad_neighbors >= 1
SET a.guilt_by_association_flag=1
```

### Guilt by association w/ arrows to visualize
```
MATCH (a:Account {flagged: 0})-[t:TRANSACTION]-(b:Account {flagged: 1}) 
WITH a, count(DISTINCT b) AS bad_neighbors,collect({dst:b,tx:t}) as transaction
WHERE bad_neighbors >= 1
RETURN a,transaction
```

