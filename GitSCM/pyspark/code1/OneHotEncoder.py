from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([(0.0,), (1.0,), (2.0,)], ["input"])
ohe = OneHotEncoder()
ohe.setInputCols(["input"])

ohe.setOutputCols(["output"])

model = ohe.fit(df)
model.setOutputCols(["output"])

model.getHandleInvalid()

model.transform(df).head().output

single_col_ohe = OneHotEncoder(inputCol="input", outputCol="output")
single_col_model = single_col_ohe.fit(df)
single_col_model.transform(df).head().output

ohePath = temp_path + "/ohe"
ohe.save(ohePath)
loadedOHE = OneHotEncoder.load(ohePath)
loadedOHE.getInputCols() == ohe.getInputCols()

modelPath = temp_path + "/ohe-model"
model.save(modelPath)
loadedModel = OneHotEncoderModel.load(modelPath)
loadedModel.categorySizes == model.categorySizes

loadedModel.transform(df).take(1) == model.transform(df).take(1)