from pyspark import SparkContext
from pyspark.streaming import StreamingContext
def functionToCreateContext():
 ssc = StreamingContext(sc, 10)
 lines = ssc.textFileStream("file:///Users/amrendraupadhyay/Downloads/Amrendra/")
 words = lines.flatMap(lambda line: line.split(" "))
 pairs = words.map(lambda word: (word, 1))
 windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
 windowedWordCounts.saveAsTextFiles("file:///Users/amrendraupadhyay/Downloads/nath")
 ssc.checkpoint("file:///Users/amrendraupadhyay/Downloads/Upadhyay_checkpoint") // setting up the checkpoint
 return ssc
context = StreamingContext.getOrCreate(“file:///Users/amrendraupadhyay/Downloads/Upadhyay_checkpoint”,functionToCreateContext)
context.start()
context.awaitTermination()
