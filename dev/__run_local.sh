
# copy delta jar to  ~/jupyter_workspace/lib/delta-core_2.12-0.8.0.jar
# copy scripts to ~/jupyter_workspace/src/
# run as __run_local.sh <script_name>

export JUPYTER_WORKSPACE_LOCATION=~/jupyter_workspace/

docker run -it -v ~/.aws:/home/glue_user/.aws -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$AWS_PROFILE -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_spark_submit amazon/aws-glue-libs:glue_libs_3.0.0_image_01 spark-submit --jars /home/glue_user/workspace/lib/delta-core_2.12-0.8.0.jar --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore  /home/glue_user/workspace/src/$1