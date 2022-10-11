from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (0.0, Vectors.dense([0.0, 0.0])),
    (1.0, Vectors.dense([0.0, 1.0])),
    (1.0, Vectors.dense([1.0, 0.0])),
    (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
mlp = MultilayerPerceptronClassifier(layers=[2, 2, 2], seed=123)
mlp.setMaxIter(100)

mlp.getMaxIter()

mlp.getBlockSize()

mlp.setBlockSize(1)

mlp.getBlockSize()

model = mlp.fit(df)
model.setFeaturesCol("features")

model.getMaxIter()

model.layers

model.weights.size

testDF = spark.createDataFrame([
    (Vectors.dense([1.0, 0.0]),),
    (Vectors.dense([0.0, 0.0]),)], ["features"])
model.predict(testDF.head().features)

model.predictRaw(testDF.head().features)

model.predictProbability(testDF.head().features)

model.transform(testDF).select("features", "prediction").show()







mlp_path = temp_path + "/mlp"
mlp.save(mlp_path)
mlp2 = MultilayerPerceptronClassifier.load(mlp_path)
mlp2.getBlockSize()

model_path = temp_path + "/mlp_model"
model.save(model_path)
model2 = MultilayerPerceptronClassificationModel.load(model_path)
model.layers == model2.layers

model.weights == model2.weights

model.transform(testDF).take(1) == model2.transform(testDF).take(1)

mlp2 = mlp2.setInitialWeights(list(range(0, 12)))
model3 = mlp2.fit(df)
model3.weights != model2.weights

model3.layers == model.layers