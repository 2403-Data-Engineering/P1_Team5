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