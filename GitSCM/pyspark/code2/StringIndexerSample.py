stringIndexer = StringIndexer(inputCol="label", outputCol="indexed",
    stringOrderType="frequencyDesc")
stringIndexer.setHandleInvalid("error")

model = stringIndexer.fit(stringIndDf)
model.setHandleInvalid("error")

td = model.transform(stringIndDf)
sorted(set([(i[0], i[1]) for i in td.select(td.id, td.indexed).collect()]),
    key=lambda x: x[0])

inverter = IndexToString(inputCol="indexed", outputCol="label2", labels=model.labels)
itd = inverter.transform(td)
sorted(set([(i[0], str(i[1])) for i in itd.select(itd.id, itd.label2).collect()]),
    key=lambda x: x[0])

stringIndexerPath = temp_path + "/string-indexer"
stringIndexer.save(stringIndexerPath)
loadedIndexer = StringIndexer.load(stringIndexerPath)
loadedIndexer.getHandleInvalid() == stringIndexer.getHandleInvalid()

modelPath = temp_path + "/string-indexer-model"
model.save(modelPath)
loadedModel = StringIndexerModel.load(modelPath)
loadedModel.labels == model.labels

indexToStringPath = temp_path + "/index-to-string"
inverter.save(indexToStringPath)
loadedInverter = IndexToString.load(indexToStringPath)
loadedInverter.getLabels() == inverter.getLabels()

loadedModel.transform(stringIndDf).take(1) == model.transform(stringIndDf).take(1)

stringIndexer.getStringOrderType()

stringIndexer = StringIndexer(inputCol="label", outputCol="indexed", handleInvalid="error",
    stringOrderType="alphabetDesc")
model = stringIndexer.fit(stringIndDf)
td = model.transform(stringIndDf)
sorted(set([(i[0], i[1]) for i in td.select(td.id, td.indexed).collect()]),
    key=lambda x: x[0])

fromlabelsModel = StringIndexerModel.from_labels(["a", "b", "c"],
    inputCol="label", outputCol="indexed", handleInvalid="error")
result = fromlabelsModel.transform(stringIndDf)
sorted(set([(i[0], i[1]) for i in result.select(result.id, result.indexed).collect()]),
    key=lambda x: x[0])

testData = sc.parallelize([Row(id=0, label1="a", label2="e"),
                           Row(id=1, label1="b", label2="f"),
                           Row(id=2, label1="c", label2="e"),
                           Row(id=3, label1="a", label2="f"),
                           Row(id=4, label1="a", label2="f"),
                           Row(id=5, label1="c", label2="f")], 3)
multiRowDf = spark.createDataFrame(testData)
inputs = ["label1", "label2"]
outputs = ["index1", "index2"]
stringIndexer = StringIndexer(inputCols=inputs, outputCols=outputs)
model = stringIndexer.fit(multiRowDf)
result = model.transform(multiRowDf)
sorted(set([(i[0], i[1], i[2]) for i in result.select(result.id, result.index1,
    result.index2).collect()]), key=lambda x: x[0])

fromlabelsModel = StringIndexerModel.from_arrays_of_labels([["a", "b", "c"], ["e", "f"]],
    inputCols=inputs, outputCols=outputs)
result = fromlabelsModel.transform(multiRowDf)
sorted(set([(i[0], i[1], i[2]) for i in result.select(result.id, result.index1,
    result.index2).collect()]), key=lambda x: x[0])