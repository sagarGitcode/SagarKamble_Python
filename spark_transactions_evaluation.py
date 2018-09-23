#!/usr/bin/env python

import sys
import re
import json
from operator import add

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row
conf=SparkConf().setAppName("TransactionEvaluation")
sc=SparkContext(conf=conf)
sqlContext=SQLContext(sc)
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: need two files as parameters 1. Input_StartOfDay_Positions.txt 2. Input_Transactions.txt", file=sys.stderr)
        sys.exit(-1)

    # read Input_StartOfDay_Positions.txt ignoring column header
    lines = sc.textFile(sys.argv[1])
    dataLines = lines.filter(lambda h: "Instrument" not in h)
    inpStartDayPosition = dataLines.map(lambda l: l.split(","))
    dfInpStartDayPosition = inpStartDayPosition.toDF(["Instrument", "Account", "AccountType", "Quantity"])

    # read Input_Transactions.txt
    inpTransactionjsonRDD = sc.wholeTextFiles(sys.argv[2]).map(lambda x : x[1])
    js = inpTransactionjsonRDD.map(lambda x: re.sub(r"\s+", "", x, re.UNICODE))
    transactions = sqlContext.jsonRDD(js)
    # Join both datasets and get aggregated transaction value (buy and sell) for complete day.
    transPositionJoin = dfInpStartDayPosition.join(transactions, dfInpStartDayPosition.Instrument == transactions.Instrument, 'leftouter').drop(transactions.Instrument).drop(transactions.TransactionId)
    transPositionJoinAgg = transPositionJoin.groupBy("Instrument","Account","AccountType","Quantity","TransactionType").agg({"TransactionQuantity": "sum"})

    def calculateTransactions(line):
        instrument = line[0]
        account = int(line[1])
        accountType = line[2]
        quantity = int(line[3])
        transactionType = line[4]
        sumTransactionQuantity = line[5]
        if accountType == "E" and transactionType == "B":
           sumTransactionQuantity = sumTransactionQuantity * -1
        if accountType == "I" and transactionType == "S":
           sumTransactionQuantity = sumTransactionQuantity * -1
        return (instrument, account, accountType, quantity, sumTransactionQuantity)

    transPositionJoinDelta = transPositionJoinAgg.map(calculateTransactions)
    dfTransPositionJoinDelta = transPositionJoinDelta.toDF(["instrument", "account", "accountType", "quantity", "delta"])
    dfTransPositionJoinDeltaAgg = dfTransPositionJoinDelta.groupBy("instrument", "account", "accountType", "quantity").agg({"delta": "sum"}).withColumnRenamed("sum(delta)","delta").na.fill(0)
    dfEndofDayPosition = dfTransPositionJoinDeltaAgg.select(dfTransPositionJoinDeltaAgg['instrument'],dfTransPositionJoinDeltaAgg['account'],dfTransPositionJoinDeltaAgg['accountType'],dfTransPositionJoinDeltaAgg['quantity'] - dfTransPositionJoinDeltaAgg['delta'], dfTransPositionJoinDeltaAgg['delta'])
    rddEndofDayPositionAbs = dfEndofDayPosition.map(lambda r : Row(instrument=r[0], account=r[1], accountType=r[2], quantity=r[3], delta=r[4], absDelta=abs(r[4])))
    dfEndofDayPositionAbs = sqlContext.createDataFrame(rddEndofDayPositionAbs)
    dfEndofDayPositionSorted = dfEndofDayPositionAbs.sort("absDelta", ascending=False)
    dfEndofDayPositionSortedOutput = dfEndofDayPositionSorted.select(dfEndofDayPositionSorted['instrument'],dfEndofDayPositionSorted['account'],dfEndofDayPositionSorted['accountType'],dfEndofDayPositionSorted['quantity'],dfEndofDayPositionSorted['delta'])
    dfEndofDayPositionSortedOutput.rdd.map(lambda l: (l[0].encode('ascii', 'ignore'),l[1],l[2].encode('ascii', 'ignore'),l[3],l[4])).saveAsTextFile("/user/training/EndofDayPosition_Output.txt")

    #to display largest and lowest net transaction volumes for the day
    print("largest & lowest transactions:", dfEndofDayPositionSortedOutput.head(2))

    #sqlContext.stop()
    sys.exit(0)
