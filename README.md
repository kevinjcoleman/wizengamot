# Wizengamot

## This is a Apache spark command line tool which uses pyspark for execution. You may need to use this tool if bosstweed is unable to process a large state file. It works the same way as bosstweed, however it will need additional pieces of software installed to function.

1. Install Apache Spark and Pyspark, [this blog post has pretty solid instructions for getting started](https://blog.sicara.com/get-started-pyspark-jupyter-guide-tutorial-ae2fe84f594f).

2. Run a job `spark-submit /path/to/wizengamot.py old_file new_file output_directory`
